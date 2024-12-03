import path from "path";
import fs from "fs/promises";
import { Logger } from "pino";

const { KELPIE_STATE_FILE = "./kelpie-state.json" } = process.env;

export const filename = path.resolve(KELPIE_STATE_FILE);

export type JobState = {
  id: string; // Job ID
  status: "running" | "completed" | "failed";
  start: string; // ISO 8601 date string when this worker started the job
  exitTime?: string; // ISO 8601 date string when the job exited
  exitCode?: number; // Exit code of the job
  end?: string; // ISO 8601 date string when this worker finished the job, including all uploads
  activeUploads: Set<string>;
  activeDownloads: Set<string>;
};

const state = {
  start: new Date().toISOString(),
  jobs: [] as JobState[],
};

export async function startDownload(
  jobId: string,
  filePath: string,
  log: Logger
): Promise<void> {
  state.jobs.find((job) => job.id === jobId)?.activeDownloads.add(filePath);
  await saveState(log);
}

export async function finishDownload(
  jobId: string,
  filePath: string,
  log: Logger
): Promise<void> {
  state.jobs.find((job) => job.id === jobId)?.activeDownloads.delete(filePath);
  await saveState(log);
}

export function hasDownload(jobId: string, filePath: string): boolean {
  return (
    state.jobs.find((job) => job.id === jobId)?.activeDownloads.has(filePath) ??
    false
  );
}

export async function startUpload(
  jobId: string,
  filePath: string,
  log: Logger
): Promise<void> {
  state.jobs.find((job) => job.id === jobId)?.activeUploads.add(filePath);
  await saveState(log);
}

export async function finishUpload(
  jobId: string,
  filePath: string,
  log: Logger
): Promise<void> {
  state.jobs.find((job) => job.id === jobId)?.activeUploads.delete(filePath);
  await saveState(log);
}

export function hasUpload(jobId: string, filePath: string): boolean {
  return (
    state.jobs.find((job) => job.id === jobId)?.activeUploads.has(filePath) ??
    false
  );
}

function getJSONState(): any {
  const { jobs, start } = state;
  return JSON.stringify(
    {
      start,
      jobs: jobs.map((job) => ({
        ...job,
        activeDownloads: Array.from(job.activeDownloads).sort(),
        activeUploads: Array.from(job.activeUploads).sort(),
      })),
    },
    null,
    2
  );
}

export async function saveState(log: Logger): Promise<void> {
  try {
    await fs.writeFile(filename, getJSONState());
  } catch (err) {
    log.error("Error saving state: ", err);
  }
}

export function getState() {
  return state;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitForUploads(
  jobId: string,
  log: Logger,
  intervalMs: number = 1000
): Promise<void> {
  while (state.jobs.find((job) => job.id === jobId)?.activeUploads.size) {
    log.info("Waiting for uploads to finish...");
    await sleep(intervalMs);
  }
}

export async function startJob(jobId: string, log: Logger): Promise<void> {
  state.jobs.push({
    id: jobId,
    status: "running",
    start: new Date().toISOString(),
    activeUploads: new Set(),
    activeDownloads: new Set(),
  });

  await saveState(log);
}

export async function jobExited(
  jobId: string,
  exitCode: number,
  log: Logger
): Promise<void> {
  const job = state.jobs.find((job) => job.id === jobId);
  if (job) {
    job.exitTime = new Date().toISOString();
    job.exitCode = exitCode;
  }

  await saveState(log);
}

export async function finishJob(
  jobId: string,
  status: "completed" | "failed",
  log: Logger
): Promise<void> {
  const job = state.jobs.find((job) => job.id === jobId);
  if (job) {
    job.status = status;
    job.end = new Date().toISOString();
  }

  await saveState(log);
}

export default {
  startDownload,
  finishDownload,
  hasDownload,
  startUpload,
  finishUpload,
  hasUpload,
  saveState,
  getState,
  waitForUploads,
  startJob,
  jobExited,
  finishJob,
  filename,
};
