import { mkdirSync } from "fs";

import {
  getWork,
  HeartbeatManager,
  reportFailed,
  reportCompleted,
  reallocateMe,
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
import { SyncConfig, Task } from "./types";
import state from "./state";

const {
  INPUT_DIR = "/input",
  OUTPUT_DIR = "/output",
  CHECKPOINT_DIR = "/checkpoint",

  // Default to 0, which means no timeout
  MAX_TIME_WITH_NO_WORK_S = "0",

  // There are backend implications to this, so we aren't documenting it yet.
  HEARTBEAT_INTERVAL_S = "10",
} = process.env;

mkdirSync(INPUT_DIR, { recursive: true });
mkdirSync(OUTPUT_DIR, { recursive: true });
mkdirSync(CHECKPOINT_DIR, { recursive: true });

const maxTimeWithNoWorkMs = parseInt(MAX_TIME_WITH_NO_WORK_S, 10) * 1000;
const heartbeatIntervalMs = parseInt(HEARTBEAT_INTERVAL_S, 10) * 1000;

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
      jobId: work.id,
      directory: dirToUpload,
      bucket: work.output_bucket!,
      prefix: work.output_prefix!,
      batchSize: 2,
      compress: !!work.compression,
      log,
    });
  } catch (e: any) {
    log.error(`Error uploading output directory: ${e.message}`);
    await reportFailed(work.id, log);
    return;
  }

  try {
    await reportCompleted(work.id, log);
  } catch (e: any) {
    log.error(`Error reporting job completion: ${e.message}`);
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
  await state.saveState(baseLogger);
  await clearAllDirectories(
    Array.from(new Set([INPUT_DIR, OUTPUT_DIR, CHECKPOINT_DIR]))
  );

  let lastWorkReceived = Date.now();
  while (keepAlive) {
    let work;
    try {
      work = await getWork();
    } catch (e: any) {
      baseLogger.error("Error fetching work: ", e);
      await sleep(heartbeatIntervalMs);
      continue;
    }

    if (!work) {
      if (
        maxTimeWithNoWorkMs > 0 &&
        Date.now() - lastWorkReceived > maxTimeWithNoWorkMs
      ) {
        baseLogger.info(
          `No work received for ${
            maxTimeWithNoWorkMs / 1000
          } seconds, exiting...`
        );
        keepAlive = false;
        /**
         * A common reason to have no work for too long is that the instance is
         * banned from a particular workload. In this case, we should reallocate
         * the instance to get a new machine id. However, we want to make sure any uploads
         * that are currently in progress complete.
         */
        const currentState = state.getState();
        let uploadsInProgress = false;
        await Promise.all(
          currentState.jobs.map(async (job) => {
            if (job.activeUploads.size) {
              uploadsInProgress = true;
              baseLogger.info(
                `Waiting for uploads to finish before reallocation...`
              );
              await state.waitForUploads(job.id, baseLogger);
            }
          })
        );

        if (uploadsInProgress) {
          /**
           * If there were uploads in progress, we should check for work one more time
           */
          continue;
        }

        /**
         * If there are no uploads in progress, we can reallocate the instance.
         */
        await reallocateMe(
          `Kelpie: Max idle time exceeded: ${maxTimeWithNoWorkMs}`,
          baseLogger
        );
        break;
      }
      baseLogger.info("No work available, sleeping for 10 seconds...");
      await sleep(heartbeatIntervalMs);
      continue;
    }
    lastWorkReceived = Date.now();
    const log = baseLogger.child({ job_id: work.id });
    log.info(`Received work: ${work.id}`);
    state.startJob(work.id, log);

    log.info("Starting heartbeat manager...");
    const heartbeatManager = new HeartbeatManager(work.id, log);

    const directoryWatchers: DirectoryWatcher[] = [];

    /**
     * The heartbeat endpoint may return a status of "canceled" if the job has been cancelled,
     * in which case we should stop the job and ask for a new one.
     */
    async function onJobCancel() {
      await Promise.all(
        directoryWatchers.map((watcher) => watcher.stopWatching())
      );
      commandExecutor.interrupt();
    }

    heartbeatManager
      .startHeartbeat(work.heartbeat_interval, onJobCancel)
      .catch(async (e: any) => {
        /**
         * This occurs if a heartbeat fails config.maxRetries times, meaning the machine
         * has lost communication with kelpie
         *  */
        log.error(`Heartbeat error: ${e.message}`);

        /**
         * If the heartbeat throws an error, we should restart it.
         * This is because the error is likely due to a network issue which
         * may be transient, and the job is still running. This way,
         * the job can continue to run and the heartbeat will be re-established.
         *
         * The alternative is to abort the job or reallocate the instance, but this is
         * not ideal because the job is still running and may complete successfully.
         */
        await heartbeatManager.stopHeartbeat();
        await heartbeatManager.startHeartbeat(
          work.heartbeat_interval,
          onJobCancel
        );
      });

    /**
     * This block is event-driven, triggered by file changes in configured directories.
     */
    if (work.sync) {
      if (work.sync.before && work.sync.before.length) {
        for (const syncConfig of work.sync.before) {
          await downloadSyncConfig(
            work.id,
            syncConfig,
            !!work.compression,
            log
          );
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
                  work.id,
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
    } else if (work.input_bucket && work.input_prefix) {
      // Download required files
      if (work.input_bucket && work.input_prefix) {
        try {
          await downloadAllFilesFromPrefix({
            jobId: work.id,
            bucket: work.input_bucket,
            prefix: work.input_prefix,
            outputDir: INPUT_DIR,
            batchSize: 20,
            decompress: !!work.compression,
            log,
          });
        } catch (e: any) {
          log.error(`Error downloading input files: ${e.message}`);
          // await reportFailed(work.id);
          continue;
        }
      }

      if (work.checkpoint_bucket && work.checkpoint_prefix) {
        try {
          await downloadAllFilesFromPrefix({
            jobId: work.id,
            bucket: work.checkpoint_bucket,
            prefix: work.checkpoint_prefix,
            outputDir: CHECKPOINT_DIR,
            batchSize: 20,
            decompress: !!work.compression,
            log,
          });
        } catch (e: any) {
          log.error(`Error downloading checkpoint files: ${e.message}`);
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
                work.id,
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
                work.id,
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
    } else {
      log.info("No storage configuration provided, skipping file sync");
    }

    /**
     * Run the command configured by the job, and then handle the outcome of that.
     */
    try {
      const exitCode = await commandExecutor.execute(
        work.command,
        work.arguments,
        {
          ...work.environment,
          INPUT_DIR,
          OUTPUT_DIR,
          CHECKPOINT_DIR,
          KELPIE_STATE_FILE: state.filename,
          KELPIE_JOB_ID: work.id,
        }
      );
      /**
       * Once the command exits, we can update the job's status in the state.
       * In the event the exitCode is null, we will default to -2, which is
       * an error code that is not used by any system commands.
       */
      state.jobExited(work.id, exitCode ?? -2, log);
      /**
       * Once the script updates, we can stop watching the directories.
       * This will stop the event-driven file sync behavior that is
       * defined above, but it will not interrupt any ongoing uploads.
       */
      await Promise.all(
        directoryWatchers.map((watcher) => watcher.stopWatching())
      );

      /**
       * If the command exits with a 0 status code, we can consider the job
       * to be successful. Otherwise, we should report the job as failed.
       */
      if (exitCode === 0) {
        log.info(`Work completed successfully on job ${work.id}`);

        // Sleep for a second to ensure the output files are written
        await sleep(1000);

        // Move the output directory to a separate location and upload it asynchronously
        if (!work.sync) {
          /**
           * THIS IS LEGACY BEHAVIOR.
           */
          const newDir = `/output-${work.id}`;
          await fs.rename(OUTPUT_DIR, newDir);
          await fs.mkdir(OUTPUT_DIR, { recursive: true });

          /**
           * This part is not awaited, so that the primary event loop can continue during this closing
           * I/O driven operation.
           */
          uploadAndCompleteJob(work, newDir, heartbeatManager, log);
        } else if (work.sync.after && work.sync.after.length) {
          /**
           * work.sync.after is an array of upload sync blocks.
           */
          // Move the output directory to a separate location and upload it asynchronously
          const modifiedOutputs: SyncConfig[] = [];
          for (let syncConfig of work.sync.after) {
            const newDir = `${path.resolve(syncConfig.local_path)}-${work.id}`;
            log.info(`Moving ${syncConfig.local_path} to ${newDir} for upload`);
            try {
              /**
               * Try moving the folder, because it's faster than copying.
               */
              await fs.rename(syncConfig.local_path, newDir);
            } catch (e: any) {
              /**
               * If the move fails, it's likely due to a cross-device link error,
               * so we should copy the folder instead.
               */
              if (e.code && e.code === "EXDEV") {
                log.warn(
                  `Cannot move ${syncConfig.local_path} to ${newDir} due to cross-device link, copying instead`
                );
                await fs.cp(syncConfig.local_path, newDir, { recursive: true });
                await fs.rm(syncConfig.local_path, { recursive: true });
              } else {
                throw e;
              }
            } finally {
              await fs.mkdir(syncConfig.local_path, { recursive: true });
            }

            modifiedOutputs.push({
              ...syncConfig,
              local_path: newDir,
            });
            log.info(`Moved ${syncConfig.local_path} to ${newDir} for upload`);
          }

          /**
           * This part is not awaited, so that the primary event loop can continue during this closing
           * I/O driven operation.
           */
          Promise.all(
            modifiedOutputs.map(async (syncConfig) => {
              await uploadSyncConfig(
                work.id,
                syncConfig,
                !!work.compression,
                log
              );
            })
          )
            .then(async () => {
              /**
               * Now that all uploads are complete, we can report the job as completed.
               * Only now do we stop the job's heartbeat, because otherwise the job may
               * be handed out again during final upload.
               */
              await heartbeatManager.stopHeartbeat();
              await reportCompleted(work.id, log);
            })
            .catch(async (e: any) => {
              log.error(`Error processing sync config: ${e.message}`);
              await reportFailed(work.id, log);
            })
            .finally(async () => {
              /**
               * Finally, we can clear the directories that were used for the sync.
               */
              await heartbeatManager.stopHeartbeat();
              await clearAllDirectories(
                modifiedOutputs.map((syncConfig) => syncConfig.local_path)
              );
            });
        } else {
          /**
           * If there's no IO to process at all, we can just report the job as completed.
           */
          await heartbeatManager.stopHeartbeat();
          await reportCompleted(work.id, log);
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
        log.error(`Error processing work: ${e.message}`);
        await reportFailed(work.id, log);
      }
      await heartbeatManager.stopHeartbeat();
    }

    /**
     * While the previous job is being finalized from temporary directories,
     * we can clear the directories that were used for the job, in preparation for the next job
     */
    await Promise.all(
      directoryWatchers.map((watcher) => watcher.stopWatching())
    );

    // Clear all directories, including the ones used for sync
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
    // Remove duplicates
    dirsToClear = Array.from(new Set(dirsToClear));

    await clearAllDirectories(dirsToClear);
  }
}

main().then(() => baseLogger.info("Kelpie Exiting"));
