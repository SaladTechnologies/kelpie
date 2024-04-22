import { mkdirSync } from "fs";

import {
  getWork,
  HeartbeatManager,
  reportFailed,
  reportCompleted,
} from "./api";
import { DirectoryWatcher, recursivelyClearFilesInDirectory } from "./files";
import { downloadAllFilesFromPrefix, uploadDirectory, uploadFile } from "./s3";
import { CommandExecutor } from "./commands";
import path from "path";

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
  await Promise.all([
    recursivelyClearFilesInDirectory(INPUT_DIR),
    recursivelyClearFilesInDirectory(OUTPUT_DIR),
    recursivelyClearFilesInDirectory(CHECKPOINT_DIR),
  ]);
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
  process.exit();
});

async function main() {
  await clearAllDirectories();

  while (keepAlive) {
    const work = await getWork();
    if (!work) {
      console.log("No work available, sleeping for 10 seconds...");
      await sleep(10000);
      continue;
    }

    // Download required files
    await Promise.all([
      downloadAllFilesFromPrefix(
        work.input_bucket,
        work.input_prefix,
        INPUT_DIR
      ),
      downloadAllFilesFromPrefix(
        work.checkpoint_bucket,
        work.checkpoint_prefix,
        CHECKPOINT_DIR
      ),
    ]);

    const checkpointWatcher = new DirectoryWatcher(CHECKPOINT_DIR);
    checkpointWatcher.watchDirectory(async (localFilePath) => {
      const relativeFilename = path.relative(CHECKPOINT_DIR, localFilePath);
      await uploadFile(
        localFilePath,
        work.checkpoint_bucket,
        work.checkpoint_prefix + relativeFilename
      );
    });

    const outputWatcher = new DirectoryWatcher(OUTPUT_DIR);
    outputWatcher.watchDirectory(async (localFilePath) => {
      const relativeFilename = path.relative(OUTPUT_DIR, localFilePath);
      await uploadFile(
        localFilePath,
        work.output_bucket,
        work.output_prefix + relativeFilename
      );
    });

    heartbeatManager.startHeartbeat(
      work.id,
      work.heartbeat_interval,
      async () => {
        await outputWatcher.stopWatching();
        await checkpointWatcher.stopWatching();
        commandExecutor.interrupt();
      }
    );

    try {
      const exitCode = await commandExecutor.execute(
        work.command,
        work.arguments,
        { ...work.environment, INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR }
      );

      if (exitCode === 0) {
        await reportCompleted(work.id);
        console.log(`Work completed successfully on job ${work.id}`);
        await sleep(1000); // Sleep for a second to ensure the output files are written
        await uploadDirectory(
          OUTPUT_DIR,
          work.output_bucket,
          work.output_prefix
        );
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

main();
