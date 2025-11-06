import chokidar, { FSWatcher } from "chokidar";
import fsPromises from "fs/promises";
import { Stats } from "fs";
import fs from "fs";
import { Logger } from "pino";

const {
  FILE_WATCHER_DEBOUNCE_MS = "0",
  FILE_WATCHER_STABILITY_CHECK_MS = "50",
  FILE_WATCHER_MAX_STABILITY_WAIT_MS = "120000",
} = process.env;
const fileWatcherStabilityCheckMs = parseInt(
  FILE_WATCHER_STABILITY_CHECK_MS,
  10
);
const fileWatcherDebounceMs = parseInt(FILE_WATCHER_DEBOUNCE_MS, 10);

// Function to check if the file has stopped changing
function waitForFileStability(filePath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    let lastKnownSize = -1;
    let retries = 0;

    const checkFile = () => {
      fs.stat(filePath, (err, stats) => {
        if (err) {
          // If file disappeared, treat as “stable” (nothing to process)
          if ((err as NodeJS.ErrnoException).code === "ENOENT") {
            return resolve();
          }
          return reject(`Error accessing file: ${err}`);
        }

        if (stats.size === lastKnownSize) {
          if (retries >= 3) {
            // Consider the file stable after 3 checks
            resolve();
          } else {
            retries++;
            setTimeout(checkFile, fileWatcherStabilityCheckMs);
          }
        } else {
          lastKnownSize = stats.size;
          retries = 0;
          setTimeout(checkFile, fileWatcherStabilityCheckMs);
        }
      });
    };

    checkFile();
  });
}

type DebouncedResult<T> =
  | { ok: true; value: T }
  | { ok: false; reason: "canceled" };

function debounceByArg<T extends (...a: any[]) => any, K>(
  fn: T,
  delay: number,
  keyFn: (...a: Parameters<T>) => K
): (...a: Parameters<T>) => Promise<DebouncedResult<Awaited<ReturnType<T>>>> {
  const timers = new Map<K, NodeJS.Timeout>();
  const settles = new Map<K, (v: DebouncedResult<any>) => void>(); // last settles per key

  return (...args: Parameters<T>) =>
    new Promise((resolve, reject) => {
      const key = keyFn(...args);

      // cancel previous
      if (timers.has(key)) {
        clearTimeout(timers.get(key)!);
        settles.get(key)?.({ ok: false, reason: "canceled" }); // settle previous
      }

      settles.set(key, resolve);

      const t = setTimeout(async () => {
        timers.delete(key);
        settles.delete(key);
        try {
          const v = await fn(...args);
          resolve({ ok: true, value: v });
        } catch (e: any) {
          reject(e);
        }
      }, delay);

      timers.set(key, t);
    });
}

const debouncedWaitForFileStability = debounceByArg(
  waitForFileStability,
  fileWatcherDebounceMs,
  (filePath: string) => filePath
);

export class DirectoryWatcher {
  private watcher: FSWatcher | null = null;
  private directory: string;
  private activeTasks: Set<Promise<any>> = new Set();
  private log: Logger;
  private ready = false;

  constructor(directory: string, log: Logger) {
    this.directory = directory;
    this.log = log;
  }

  // Start watching the directory
  watchDirectory(
    forEachFile: (localFilePath: string, eventType: string) => Promise<void>
  ): void {
    this.log.info(`Watching directory: ${this.directory}`);
    this.watcher = chokidar.watch(this.directory, {
      ignored: /^\./,
      persistent: true,
    });

    this.watcher.on("add", async (path: string, stats?: Stats) => {
      if (!this.ready) {
        return;
      }
      this.log.debug(`Event: add on ${path}`);
      const task = debouncedWaitForFileStability(path)
        .then(({ ok }) => {
          if (!ok) {
            this.log.debug(
              `Add event for ${path} was canceled due to debounce`
            );
            return;
          }
          forEachFile(path, "add").catch((err) => {
            this.log.error(`Error processing file ${path} after add: ${err}`);
          });
        })
        .catch(async (err) => {
          // Check if file still exists
          try {
            await fsPromises.stat(path);
          } catch (e) {
            return;
          }
          this.log.error(`Error processing file ${path} after add: ${err}`);
        })
        .finally(() => {
          this.activeTasks.delete(task);
        });
      this.activeTasks.add(task);
    });

    this.watcher.on("change", async (path: string, stats?: Stats) => {
      if (!this.ready) {
        return;
      }
      this.log.debug(`Event: change on ${path}`);
      const task = debouncedWaitForFileStability(path)
        .then(({ ok }) => {
          if (!ok) {
            this.log.debug(
              `Change event for ${path} was canceled due to debounce`
            );
            return;
          }
          forEachFile(path, "change").catch((err) => {
            this.log.error(
              `Error processing file ${path} after change: ${err}`
            );
          });
        })
        .catch(async (err) => {
          // Check if file still exists
          try {
            await fsPromises.stat(path);
          } catch {
            return;
          }
          this.log.error(`Error processing file ${path} after change: ${err}`);
          this.log.error(`Error processing file ${path} after change: ${err}`);
        })
        .finally(() => {
          this.activeTasks.delete(task);
        });
      this.activeTasks.add(task);
    });

    this.watcher.on("unlink", async (path: string) => {
      if (!this.ready) {
        return;
      }
      this.log.debug(`Event: unlink on ${path}`);
      const task = forEachFile(path, "unlink")
        .catch((err) => {
          this.log.error(`Error processing file ${path} after unlink: ${err}`);
        })
        .finally(() => {
          this.activeTasks.delete(task);
        });
      this.activeTasks.add(task);
    });

    this.watcher.on("ready", () => {
      this.log.info("Directory watcher is ready");
      this.ready = true;
    });
  }

  // Stop watching the directory
  async stopWatching(): Promise<void> {
    if (this.watcher) {
      this.log.info(`Stopping directory watcher: ${this.directory}`);
      await Promise.all(this.activeTasks); // Wait for all tasks to complete
      this.log.info("All tasks completed.");
      await this.watcher.close(); // Close the watcher to free up resources
      this.watcher = null;
      this.log.info("Stopped watching directory.");
    }
  }
}

export async function purgeDirectory(
  directory: string,
  log: Logger
): Promise<void> {
  try {
    log.info(`Clearing files in directory: ${directory}`);
    await fsPromises.rm(directory, { recursive: true, force: true });
    await fsPromises.mkdir(directory, { recursive: true });
    log.info("Directory cleared successfully");
  } catch (err: any) {
    log.error("Error clearing directory: ", err);
  }
}
