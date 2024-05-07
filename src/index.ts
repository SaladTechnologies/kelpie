import { mkdirSync } from "fs";

import {
  getWork,
  HeartbeatManager,
  reportFailed,
  reportCompleted,
  Task,
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
import fs from "fs/promises";
import { log as baseLogger } from "./logger";
import { Logger } from "pino";

const {
  INPUT_DIR = "/input",
  OUTPUT_DIR = "/output",
  CHECKPOINT_DIR = "/checkpoint",
} = process.env;

mkdirSync(INPUT_DIR, { recursive: true });
mkdirSync(OUTPUT_DIR, { recursive: true });
mkdirSync(CHECKPOINT_DIR, { recursive: true });

const commandExecutor = new CommandExecutor();

async function clearAllDirectories(): Promise<void> {
  const dirsToClear = Array.from(
    new Set([INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR])
  );
  await Promise.all(
    dirsToClear.map((dir) => recursivelyClearFilesInDirectory(dir, baseLogger))
  );
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function uploadAndCompleteJob(
  work: Task,
  dirToUpload: string,
  heartbeatManager: HeartbeatManager,
  log: Logger
): Promise<void> {
  try {
    await uploadDirectory(
      dirToUpload,
      work.output_bucket,
      work.output_prefix,
      2,
      !!work.compression,
      log
    );
  } catch (e: any) {
    log.error("Error uploading output directory: ", e);
    await reportFailed(work.id, log);
    return;
  }

  try {
    await reportCompleted(work.id, log);
  } catch (e: any) {
    log.error("Error reporting job completion: ", e);
    return;
  }

  log.info(
    `Output directory uploaded and job completed. Removing ${dirToUpload}...`
  );
  await fs.rmdir(dirToUpload, { recursive: true });

  await heartbeatManager.stopHeartbeat();
}

let keepAlive = true;
process.on("SIGINT", () => {
  baseLogger.info("Received SIGINT, stopping...");
  keepAlive = false;
});

process.on("SIGTERM", () => {
  keepAlive = false;
  baseLogger.info("Received SIGTERM, stopping...");
  process.exit();
});

async function main() {
  baseLogger.info(`Kelpie v${version} started`);
  await clearAllDirectories();

  while (keepAlive) {
    let work;
    try {
      work = await getWork();
    } catch (e: any) {
      baseLogger.error("Error fetching work: ", e);
      await sleep(10000);
      continue;
    }

    if (!work) {
      baseLogger.info("No work available, sleeping for 10 seconds...");
      await sleep(10000);
      continue;
    }
    const log = baseLogger.child({ job_id: work.id });
    log.info(`Received work: ${work.id}`);

    log.info("Starting heartbeat manager...");
    const checkpointWatcher = new DirectoryWatcher(CHECKPOINT_DIR, log);
    const outputWatcher = new DirectoryWatcher(OUTPUT_DIR, log);
    const heartbeatManager = new HeartbeatManager(work.id, log);

    heartbeatManager.startHeartbeat(work.heartbeat_interval, async () => {
      await outputWatcher.stopWatching();
      await checkpointWatcher.stopWatching();
      commandExecutor.interrupt();
    });

    // Download required files
    try {
      await downloadAllFilesFromPrefix(
        work.input_bucket,
        work.input_prefix,
        INPUT_DIR,
        20,
        !!work.compression,
        log
      );
    } catch (e: any) {
      log.error("Error downloading input files: ", e);
      // await reportFailed(work.id);
      continue;
    }

    try {
      await downloadAllFilesFromPrefix(
        work.checkpoint_bucket,
        work.checkpoint_prefix,
        CHECKPOINT_DIR,
        20,
        !!work.compression,
        log
      );
    } catch (e: any) {
      log.error("Error downloading checkpoint files: ", e);
      // await reportFailed(work.id);
      continue;
    }

    log.info(
      "All files downloaded successfully, starting directory watchers..."
    );

    checkpointWatcher.watchDirectory(
      async (localFilePath: string, eventType: string) => {
        const relativeFilename = path.relative(CHECKPOINT_DIR, localFilePath);
        if (eventType === "add" || eventType === "change") {
          await uploadFile(
            localFilePath,
            work.checkpoint_bucket,
            work.checkpoint_prefix + relativeFilename,
            !!work.compression,
            log
          );
        } else if (eventType === "unlink") {
          await deleteFile(
            work.checkpoint_bucket,
            work.checkpoint_prefix + relativeFilename,
            log
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
              work.output_prefix + relativeFilename,
              !!work.compression,
              log
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
      await checkpointWatcher.stopWatching();
      await outputWatcher.stopWatching();

      if (exitCode === 0) {
        log.info(`Work completed successfully on job ${work.id}`);
        await sleep(1000); // Sleep for a second to ensure the output files are written
        // Move the output directory to a separate location and upload it asynchronously
        const newDir = `/output-${work.id}`;
        await fs.rename(OUTPUT_DIR, newDir);
        await fs.mkdir(OUTPUT_DIR, { recursive: true });
        uploadAndCompleteJob(work, newDir, heartbeatManager, log);
      } else {
        await reportFailed(work.id, log);
        await heartbeatManager.stopHeartbeat();
        log.error(`Work failed with exit code ${exitCode}`);
      }
    } catch (e: any) {
      if (/terminated due to signal/i.test(e.message)) {
        log.info("Work was interrupted, likely due to remote cancellation");
      } else {
        log.error("Error processing work: ", e);
        await reportFailed(work.id, log);
      }
      await heartbeatManager.stopHeartbeat();
    }
    await checkpointWatcher.stopWatching();
    await outputWatcher.stopWatching();

    await clearAllDirectories();
  }
}

main().then(() => baseLogger.info("Kelpie Exiting"));
