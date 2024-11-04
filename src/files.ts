import chokidar, { FSWatcher } from "chokidar";
import fsPromises from "fs/promises";
import { Stats } from "fs";
import fs from "fs";
import { Logger } from "pino";

// Function to check if the file has stopped changing
function waitForFileStability(filePath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    let lastKnownSize = -1;
    let retries = 0;

    const checkFile = () => {
      fs.stat(filePath, (err, stats) => {
        if (err) {
          reject(`Error accessing file: ${err}`);
          return;
        }

        if (stats.size === lastKnownSize) {
          if (retries >= 3) {
            // Consider the file stable after 3 checks
            resolve();
          } else {
            retries++;
            setTimeout(checkFile, 50);
          }
        } else {
          lastKnownSize = stats.size;
          retries = 0;
          setTimeout(checkFile, 50);
        }
      });
    };

    checkFile();
  });
}

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
      const task = waitForFileStability(path)
        .then(() => forEachFile(path, "add"))
        .catch(async (err) => {
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
      const task = waitForFileStability(path)
        .then(() => forEachFile(path, "change"))
        .catch((err) => {
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
      this.watcher.close(); // Close the watcher to free up resources
      this.watcher = null;
      this.log.info("Stopped watching directory.");
    }
  }
}

export async function recursivelyClearFilesInDirectory(
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
