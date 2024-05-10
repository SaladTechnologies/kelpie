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
  downloadSyncConfig,
  uploadSyncConfig,
} from "./s3";
import { CommandExecutor } from "./commands";
import path from "path";
import { version } from "../package.json";
import fs from "fs/promises";
import { log as baseLogger } from "./logger";
import { Logger } from "pino";
import { Task } from "./types";

const {
  INPUT_DIR = "/input",
  OUTPUT_DIR = "/output",
  CHECKPOINT_DIR = "/checkpoint",
} = process.env;

mkdirSync(INPUT_DIR, { recursive: true });
mkdirSync(OUTPUT_DIR, { recursive: true });
mkdirSync(CHECKPOINT_DIR, { recursive: true });

const commandExecutor = new CommandExecutor();

async function clearAllDirectories(dirsToClear: string[]): Promise<void> {
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
    await uploadDirectory({
      directory: dirToUpload,
      bucket: work.output_bucket!,
      prefix: work.output_prefix!,
      batchSize: 2,
      compress: !!work.compression,
      log,
    });
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

const filesBeingSynced = new Set();

async function main() {
  baseLogger.info(`Kelpie v${version} started`);
  await clearAllDirectories(
    Array.from(new Set([INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR]))
  );

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
    const heartbeatManager = new HeartbeatManager(work.id, log);

    const directoryWatchers: DirectoryWatcher[] = [];

    heartbeatManager.startHeartbeat(work.heartbeat_interval, async () => {
      await Promise.all(
        directoryWatchers.map((watcher) => watcher.stopWatching())
      );
      commandExecutor.interrupt();
    });

    if (work.sync) {
      if (work.sync.before && work.sync.before.length) {
        for (const syncConfig of work.sync.before) {
          await downloadSyncConfig(syncConfig, !!work.compression, log);
        }
      }

      if (work.sync.during && work.sync.during.length) {
        for (const syncConfig of work.sync.during) {
          const dirWatcher = new DirectoryWatcher(syncConfig.local_path, log);
          dirWatcher.watchDirectory(
            async (localFilePath: string, eventType: string) => {
              if (filesBeingSynced.has(localFilePath)) {
                return;
              }
              const relativeFilename = path.relative(
                syncConfig.local_path,
                localFilePath
              );
              if (
                (eventType === "add" || eventType === "change") &&
                syncConfig.direction === "upload" &&
                (!syncConfig.pattern ||
                  new RegExp(syncConfig.pattern).test(relativeFilename))
              ) {
                filesBeingSynced.add(localFilePath);
                await uploadFile(
                  localFilePath,
                  syncConfig.bucket,
                  syncConfig.prefix + relativeFilename,
                  !!work.compression,
                  log
                );
                filesBeingSynced.delete(localFilePath);
              } else if (
                eventType == "unlink" &&
                syncConfig.direction === "upload" &&
                (!syncConfig.pattern ||
                  new RegExp(syncConfig.pattern).test(relativeFilename))
              ) {
                filesBeingSynced.add(localFilePath);
                let keyToDelete = syncConfig.prefix + relativeFilename;
                if (!!work.compression) {
                  keyToDelete += ".gz";
                }
                await deleteFile(syncConfig.bucket, keyToDelete, log);
                filesBeingSynced.delete(localFilePath);
              }
            }
          );
          directoryWatchers.push(dirWatcher);
        }
      }
    } else {
      // Download required files
      if (work.input_bucket && work.input_prefix) {
        try {
          await downloadAllFilesFromPrefix({
            bucket: work.input_bucket,
            prefix: work.input_prefix,
            outputDir: INPUT_DIR,
            batchSize: 20,
            decompress: !!work.compression,
            log,
          });
        } catch (e: any) {
          log.error("Error downloading input files: ", e);
          // await reportFailed(work.id);
          continue;
        }
      }

      if (work.checkpoint_bucket && work.checkpoint_prefix) {
        try {
          await downloadAllFilesFromPrefix({
            bucket: work.checkpoint_bucket,
            prefix: work.checkpoint_prefix,
            outputDir: CHECKPOINT_DIR,
            batchSize: 20,
            decompress: !!work.compression,
            log,
          });
        } catch (e: any) {
          log.error("Error downloading checkpoint files: ", e);
          // await reportFailed(work.id);
          continue;
        }
        const checkpointWatcher = new DirectoryWatcher(CHECKPOINT_DIR, log);

        checkpointWatcher.watchDirectory(
          async (localFilePath: string, eventType: string) => {
            const relativeFilename = path.relative(
              CHECKPOINT_DIR,
              localFilePath
            );
            if (eventType === "add" || eventType === "change") {
              await uploadFile(
                localFilePath,
                work.checkpoint_bucket!,
                work.checkpoint_prefix + relativeFilename,
                !!work.compression,
                log
              );
            } else if (eventType === "unlink") {
              await deleteFile(
                work.checkpoint_bucket!,
                work.checkpoint_prefix + relativeFilename,
                log
              );
            }
          }
        );

        directoryWatchers.push(checkpointWatcher);
      }

      log.info(
        "All files downloaded successfully, starting directory watchers..."
      );

      if (
        work.output_bucket &&
        work.output_prefix &&
        CHECKPOINT_DIR !== OUTPUT_DIR
      ) {
        const outputWatcher = new DirectoryWatcher(OUTPUT_DIR, log);
        outputWatcher.watchDirectory(
          async (localFilePath: string, eventType: string) => {
            const relativeFilename = path.relative(OUTPUT_DIR, localFilePath);
            if (eventType === "add") {
              await uploadFile(
                localFilePath,
                work.output_bucket!,
                work.output_prefix + relativeFilename,
                !!work.compression,
                log
              );
            }
          }
        );
        directoryWatchers.push(outputWatcher);
      }
    }

    try {
      const exitCode = await commandExecutor.execute(
        work.command,
        work.arguments,
        { ...work.environment, INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR }
      );
      await Promise.all(
        directoryWatchers.map((watcher) => watcher.stopWatching())
      );

      if (exitCode === 0) {
        log.info(`Work completed successfully on job ${work.id}`);
        await sleep(1000); // Sleep for a second to ensure the output files are written
        // Move the output directory to a separate location and upload it asynchronously
        if (!work.sync) {
          const newDir = `/output-${work.id}`;
          await fs.rename(OUTPUT_DIR, newDir);
          await fs.mkdir(OUTPUT_DIR, { recursive: true });
          uploadAndCompleteJob(work, newDir, heartbeatManager, log);
        } else if (work.sync.after && work.sync.after.length) {
          Promise.all(
            work.sync.after.map(async (syncConfig) => {
              const newDir = `${syncConfig.local_path}-${work.id}`;
              await fs.rename(syncConfig.local_path, newDir);
              syncConfig.local_path = newDir;
              await uploadSyncConfig(syncConfig, !!work.compression, log);
            })
          )
            .then(async () => {
              await reportCompleted(work.id, log);
            })
            .catch(async (e: any) => {
              log.error("Error processing sync config: ", e);
              await reportFailed(work.id, log);
            })
            .finally(async () => {
              await heartbeatManager.stopHeartbeat();
            });
        }
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
    await Promise.all(
      directoryWatchers.map((watcher) => watcher.stopWatching())
    );

    let dirsToClear = [INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR];
    if (work.sync) {
      if (work.sync.before && work.sync.before.length) {
        dirsToClear.push(
          ...work.sync.before.map((syncConfig) => syncConfig.local_path)
        );
      }
      if (work.sync.during && work.sync.during.length) {
        dirsToClear.push(
          ...work.sync.during.map((syncConfig) => syncConfig.local_path)
        );
      }
      if (work.sync.after && work.sync.after.length) {
        dirsToClear.push(
          ...work.sync.after.map((syncConfig) => syncConfig.local_path)
        );
      }
    }
    dirsToClear = Array.from(new Set(dirsToClear));

    await clearAllDirectories(dirsToClear);
  }
}

main().then(() => baseLogger.info("Kelpie Exiting"));
