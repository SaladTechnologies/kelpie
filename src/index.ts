import { mkdirSync } from "fs";

import {
  getWork,
  HeartbeatManager,
  reportFailed,
  reportCompleted,
} from "./api";
import { DirectoryWatcher, recursivelyClearFilesInDirectory } from "./files";
import {
  downloadAllFilesFromPrefix,
  uploadDirectory,
  uploadFile,
  deleteFile,
} from "./s3";
import { CommandExecutor } from "./commands";
import path from "path";
import { version } from "../package.json";

const {
  INPUT_DIR = "/input",
  OUTPUT_DIR = "/output",
  CHECKPOINT_DIR = "/checkpoint",
} = process.env;

mkdirSync(INPUT_DIR, { recursive: true });
mkdirSync(OUTPUT_DIR, { recursive: true });
mkdirSync(CHECKPOINT_DIR, { recursive: true });

const heartbeatManager = new HeartbeatManager();
const commandExecutor = new CommandExecutor();

async function clearAllDirectories(): Promise<void> {
  const dirsToClear = Array.from(
    new Set([INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR])
  );
  await Promise.all(dirsToClear.map(recursivelyClearFilesInDirectory));
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let keepAlive = true;
process.on("SIGINT", () => {
  console.log("Received SIGINT, stopping...");
  keepAlive = false;
});

process.on("SIGTERM", () => {
  keepAlive = false;
  console.log("Received SIGTERM, stopping...");
  process.exit();
});

async function main() {
  console.log(`Kelpie v${version} started`);
  await clearAllDirectories();

  while (keepAlive) {
    let work;
    try {
      work = await getWork();
    } catch (e: any) {
      console.error("Error fetching work: ", e);
      await sleep(10000);
      continue;
    }

    if (!work) {
      console.log("No work available, sleeping for 10 seconds...");
      await sleep(10000);
      continue;
    }
    console.log(`Received work: ${work.id}`);

    console.log("Starting heartbeat manager...");
    const checkpointWatcher = new DirectoryWatcher(CHECKPOINT_DIR);
    const outputWatcher = new DirectoryWatcher(OUTPUT_DIR);

    heartbeatManager.startHeartbeat(
      work.id,
      work.heartbeat_interval,
      async () => {
        await outputWatcher.stopWatching();
        await checkpointWatcher.stopWatching();
        commandExecutor.interrupt();
      }
    );

    // Download required files
    try {
      await downloadAllFilesFromPrefix(
        work.input_bucket,
        work.input_prefix,
        INPUT_DIR,
        20
      );
    } catch (e: any) {
      console.error("Error downloading input files: ", e);
      // await reportFailed(work.id);
      continue;
    }

    try {
      await downloadAllFilesFromPrefix(
        work.checkpoint_bucket,
        work.checkpoint_prefix,
        CHECKPOINT_DIR,
        20
      );
    } catch (e: any) {
      console.error("Error downloading checkpoint files: ", e);
      // await reportFailed(work.id);
      continue;
    }

    console.log(
      "All files downloaded successfully, starting directory watchers..."
    );

    checkpointWatcher.watchDirectory(
      async (localFilePath: string, eventType: string) => {
        const relativeFilename = path.relative(CHECKPOINT_DIR, localFilePath);
        if (eventType === "add") {
          await uploadFile(
            localFilePath,
            work.checkpoint_bucket,
            work.checkpoint_prefix + relativeFilename
          );
        } else if (eventType === "unlink") {
          await deleteFile(
            work.checkpoint_bucket,
            work.checkpoint_prefix + relativeFilename
          );
        }
      }
    );

    if (CHECKPOINT_DIR !== OUTPUT_DIR) {
      outputWatcher.watchDirectory(
        async (localFilePath: string, eventType: string) => {
          const relativeFilename = path.relative(OUTPUT_DIR, localFilePath);
          if (eventType === "add") {
            await uploadFile(
              localFilePath,
              work.output_bucket,
              work.output_prefix + relativeFilename
            );
          }
        }
      );
    }

    try {
      const exitCode = await commandExecutor.execute(
        work.command,
        work.arguments,
        { ...work.environment, INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR }
      );

      if (exitCode === 0) {
        console.log(`Work completed successfully on job ${work.id}`);
        await sleep(1000); // Sleep for a second to ensure the output files are written
        await uploadDirectory(
          OUTPUT_DIR,
          work.output_bucket,
          work.output_prefix,
          5
        );
        await reportCompleted(work.id);
      } else {
        await reportFailed(work.id);
        console.error(
          `Work failed with exit code ${exitCode} on job ${work.id}`
        );
      }
    } catch (e: any) {
      if (/terminated due to signal/i.test(e.message)) {
        console.log("Work was interrupted, likely due to remote cancellation");
      } else {
        console.error("Error processing work: ", e);
        await reportFailed(work.id);
      }
    }
    await checkpointWatcher.stopWatching();
    await outputWatcher.stopWatching();
    heartbeatManager.stopHeartbeat();
    await clearAllDirectories();
  }
}

main().then(() => console.log("Kelpie Exiting"));
