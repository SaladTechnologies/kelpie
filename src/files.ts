import chokidar, { FSWatcher } from "chokidar";
import { join } from "path";
import fsPromises from "fs/promises";
import { Stats } from "fs";
import fs from "fs";
import { log } from "./logger";
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
      this.log.debug(`Event: add on ${path}`);
      const task = waitForFileStability(path)
        .then(() => forEachFile(path, "add"))
        .catch((err) => {
          this.log.error(`Error processing file ${path} after add: ${err}`);
        })
        .finally(() => {
          this.activeTasks.delete(task);
        });
      this.activeTasks.add(task);
    });

    this.watcher.on("change", async (path: string, stats?: Stats) => {
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
    const files = await fsPromises.readdir(directory);
    await Promise.all(
      files.map(async (file) => {
        const filePath = join(directory, file);
        const stats = await fsPromises.stat(filePath);
        if (stats.isFile()) {
          log.debug(`Removing file: ${filePath}`);
          await fsPromises.unlink(filePath);
        } else if (stats.isDirectory()) {
          await recursivelyClearFilesInDirectory(filePath, log);
          log.debug(`Removing directory: ${filePath}`);
          await fsPromises.rmdir(filePath);
        }
      })
    );
    log.info("Directory cleared successfully");
  } catch (err) {
    log.error("Error clearing directory: ", err);
  }
}
