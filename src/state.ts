import path from "path";
import fs from "fs/promises";
import { Logger } from "pino";

const { KELPIE_STATE_FILE = "./kelpie-state.json" } = process.env;

const kelpieStateFile = path.resolve(KELPIE_STATE_FILE);

const state = {
  start: new Date().toISOString(),
  uploads: new Set<string>(),
  downloads: new Set<string>(),
};

export async function startDownload(
  filePath: string,
  log: Logger
): Promise<void> {
  state.downloads.add(filePath);
  await saveState(log);
}

export async function finishDownload(
  filePath: string,
  log: Logger
): Promise<void> {
  state.downloads.delete(filePath);
  await saveState(log);
}

export function hasDownload(filePath: string): boolean {
  return state.downloads.has(filePath);
}

export async function startUpload(
  filePath: string,
  log: Logger
): Promise<void> {
  state.uploads.add(filePath);
  await saveState(log);
}

export async function finishUpload(
  filePath: string,
  log: Logger
): Promise<void> {
  state.uploads.delete(filePath);
  await saveState(log);
}

export function hasUpload(filePath: string): boolean {
  return state.uploads.has(filePath);
}

function getJSONState(): any {
  const { uploads, downloads, start } = state;
  return JSON.stringify(
    {
      start,
      uploads: Array.from(uploads),
      downloads: Array.from(downloads),
    },
    null,
    2
  );
}

export async function saveState(log: Logger): Promise<void> {
  try {
    await fs.writeFile(kelpieStateFile, getJSONState());
  } catch (err) {
    log.error("Error saving state: ", err);
  }
}

export default {
  startDownload,
  finishDownload,
  hasDownload,
  startUpload,
  finishUpload,
  hasUpload,
  saveState,
};
