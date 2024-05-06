import { spawn, ChildProcess } from "child_process";
import { log } from "./logger";

export class CommandExecutor {
  private process: ChildProcess | null = null;

  /**
   * Executes a command with arguments and custom environment variables.
   * Returns a promise that resolves with the exit code of the subprocess.
   * @param command The command to execute.
   * @param args An array of arguments for the command.
   * @param envAdditions Object with additional environment variables.
   */
  async execute(
    command: string,
    args: string[],
    envAdditions: NodeJS.ProcessEnv
  ): Promise<number | null> {
    const env = { ...process.env, ...envAdditions }; // Merge parent environment with additions

    return new Promise((resolve, reject) => {
      this.process = spawn(command, args, {
        env: env,
        stdio: "inherit", // Use the parent's stdin, stdout, and stderr
      });

      this.process.on("error", (err) => {
        log.error(`Failed to start subprocess: ${err.message}`);
        reject(err);
      });

      this.process.on("exit", (code, signal) => {
        log.info(`Process exited with code ${code}, signal ${signal}`);
        this.process = null;
        if (code !== null) {
          resolve(code);
        } else {
          reject(new Error(`Process terminated due to signal: ${signal}`));
        }
      });
    });
  }

  /**
   * Interrupts the currently running subprocess.
   */
  interrupt(): void {
    if (this.process) {
      this.process.kill("SIGINT"); // Sends the interrupt signal
      log.info("Process was interrupted");
    } else {
      log.info("No process to interrupt");
    }
  }
}
