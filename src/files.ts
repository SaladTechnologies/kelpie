import { watch, FSWatcher } from "fs";
import { join } from "path";
import fs from "fs/promises";

export class DirectoryWatcher {
  private watcher: FSWatcher | null = null;
  private directory: string;
  private activeTasks: Set<Promise<any>> = new Set();

  constructor(directory: string) {
    this.directory = directory;
  }

  // Start watching the directory
  watchDirectory(forEachFile: (localFilePath: string) => Promise<void>): void {
    console.log(`Watching directory: ${this.directory}`);
    this.watcher = watch(
      this.directory,
      { recursive: true },
      async (eventType, filename) => {
        console.log(`Event: ${eventType} on ${filename}`);
        if (filename && eventType === "change") {
          const fullPath = join(this.directory, filename);
          const task = forEachFile(fullPath).finally(() => {
            this.activeTasks.delete(task);
          });
          this.activeTasks.add(task);
        }
      }
    );
  }

  // Stop watching the directory
  async stopWatching(): Promise<void> {
    if (this.watcher) {
      this.watcher.close(); // Close the watcher to free up resources
      this.watcher = null;
      console.log("Stopped watching directory.");
      await Promise.all(this.activeTasks); // Wait for all tasks to complete
      console.log("All tasks completed.");
    }
  }
}

export async function recursivelyClearFilesInDirectory(
  directory: string
): Promise<void> {
  try {
    console.log(`Clearing files in directory: ${directory}`);
    const files = await fs.readdir(directory);
    await Promise.all(
      files.map(async (file) => {
        const filePath = join(directory, file);
        const stats = await fs.stat(filePath);
        if (stats.isFile()) {
          console.log(`Removing file: ${filePath}`);
          await fs.unlink(filePath);
        } else if (stats.isDirectory()) {
          await recursivelyClearFilesInDirectory(filePath);
          console.log(`Removing directory: ${filePath}`);
          await fs.rmdir(filePath);
        }
      })
    );
    console.log("Directory cleared successfully");
  } catch (err) {
    console.error("Error clearing directory: ", err);
  }
}
