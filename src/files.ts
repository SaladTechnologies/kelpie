import chokidar, { FSWatcher } from "chokidar";
import { join } from "path";
import fsPromises from "fs/promises";
import { Stats } from "fs";
import fs from "fs";

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
            setTimeout(checkFile, 100); // Shortened interval due to smaller file size
          }
        } else {
          lastKnownSize = stats.size;
          retries = 0;
          setTimeout(checkFile, 100);
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

  constructor(directory: string) {
    this.directory = directory;
  }

  // Start watching the directory
  watchDirectory(
    forEachFile: (localFilePath: string, eventType: string) => Promise<void>
  ): void {
    console.log(`Watching directory: ${this.directory}`);
    this.watcher = chokidar.watch(this.directory, {
      ignored: /^\./,
      persistent: true,
    });

    this.watcher.on("add", async (path: string, stats?: Stats) => {
      console.log(`Event: add on ${path}`);
      const task = waitForFileStability(path)
        .then(() => forEachFile(path, "add"))
        .finally(() => {
          this.activeTasks.delete(task);
        });
      this.activeTasks.add(task);
    });

    this.watcher.on("change", async (path: string, stats?: Stats) => {
      console.log(`Event: change on ${path}`);
      const task = waitForFileStability(path)
        .then(() => forEachFile(path, "change"))
        .finally(() => {
          this.activeTasks.delete(task);
        });
      this.activeTasks.add(task);
    });

    this.watcher.on("unlink", async (path: string) => {
      console.log(`Event: unlink on ${path}`);
      const task = forEachFile(path, "unlink").finally(() => {
        this.activeTasks.delete(task);
      });
      this.activeTasks.add(task);
    });
  }

  // Stop watching the directory
  async stopWatching(): Promise<void> {
    if (this.watcher) {
      await Promise.all(this.activeTasks); // Wait for all tasks to complete
      console.log("All tasks completed.");
      this.watcher.close(); // Close the watcher to free up resources
      this.watcher = null;
      console.log("Stopped watching directory.");
    }
  }
}

export async function recursivelyClearFilesInDirectory(
  directory: string
): Promise<void> {
  try {
    console.log(`Clearing files in directory: ${directory}`);
    const files = await fsPromises.readdir(directory);
    await Promise.all(
      files.map(async (file) => {
        const filePath = join(directory, file);
        const stats = await fsPromises.stat(filePath);
        if (stats.isFile()) {
          console.log(`Removing file: ${filePath}`);
          await fsPromises.unlink(filePath);
        } else if (stats.isDirectory()) {
          await recursivelyClearFilesInDirectory(filePath);
          console.log(`Removing directory: ${filePath}`);
          await fsPromises.rmdir(filePath);
        }
      })
    );
    console.log("Directory cleared successfully");
  } catch (err) {
    console.error("Error clearing directory: ", err);
  }
}
