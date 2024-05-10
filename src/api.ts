import assert from "assert";
import { log as baseLogger } from "./logger";
import { Logger } from "pino";
import { Task } from "./types";

let {
  KELPIE_API_URL,
  KELPIE_API_KEY,
  SALAD_MACHINE_ID = "",
  SALAD_CONTAINER_GROUP_ID = "",
  MAX_RETRIES = "3",
} = process.env;

assert(KELPIE_API_URL, "KELPIE_API_URL is required");
assert(KELPIE_API_KEY, "KELPIE_API_KEY is required");

if (KELPIE_API_URL.endsWith("/")) {
  KELPIE_API_URL = KELPIE_API_URL.slice(0, -1);
}

const maxRetries = parseInt(MAX_RETRIES, 10);

const headers = {
  "Content-Type": "application/json",
  "X-Kelpie-Key": KELPIE_API_KEY,
};

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchUpToNTimes<T>(
  url: string,
  params: any,
  n: number,
  log: Logger = baseLogger
): Promise<T> {
  let retries = 0;
  while (retries < n) {
    try {
      const response = await fetch(url, params);
      if (response.ok) {
        return response.json() as Promise<T>;
      } else {
        const body = await response.text();
        log.warn(`Error fetching data, retrying: ${body}`);
        retries++;
        await sleep(retries * 1000);
        continue;
      }
    } catch (err: any) {
      log.warn(`Error fetching data, retrying: ${err.message}`);
      retries++;
      await sleep(retries * 1000);
      continue;
    }
  }
  throw new Error(`Failed to fetch data: ${url}`);
}

export async function getWork(): Promise<Task | null> {
  const query = new URLSearchParams({
    machine_id: SALAD_MACHINE_ID,
    container_group_id: SALAD_CONTAINER_GROUP_ID,
  }).toString();
  const work = await fetchUpToNTimes<Task[]>(
    `${KELPIE_API_URL}/work?${query}`,
    {
      method: "GET",
      headers,
    },
    maxRetries
  );
  if (work.length) {
    return work[0];
  }
  return null;
}

export async function sendHeartbeat(
  jobId: string,
  log: Logger
): Promise<{ status: Task["status"] }> {
  log.debug(`Sending heartbeat`);
  const { status } = await fetchUpToNTimes<{ status: Task["status"] }>(
    `${KELPIE_API_URL}/jobs/${jobId}/heartbeat`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        machine_id: SALAD_MACHINE_ID,
        container_group_id: SALAD_CONTAINER_GROUP_ID,
      }),
    },
    maxRetries,
    log
  );
  return { status };
}

export async function reportFailed(jobId: string, log: Logger): Promise<void> {
  log.info(`Reporting job failed`);
  await fetchUpToNTimes(
    `${KELPIE_API_URL}/jobs/${jobId}/failed`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        machine_id: SALAD_MACHINE_ID,
        container_group_id: SALAD_CONTAINER_GROUP_ID,
      }),
    },
    maxRetries,
    log
  );
}

export async function reportCompleted(
  jobId: string,
  log: Logger
): Promise<void> {
  log.info("Reporting job completed");
  await fetchUpToNTimes(
    `${KELPIE_API_URL}/jobs/${jobId}/completed`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        machine_id: SALAD_MACHINE_ID,
        container_group_id: SALAD_CONTAINER_GROUP_ID,
      }),
    },
    maxRetries,
    log
  );
}

export class HeartbeatManager {
  private active: boolean = false;
  private jobId: string;
  private waiter: Promise<void> | null = null;
  private log: Logger;

  constructor(jobId: string, log: Logger) {
    this.log = log;
    this.jobId = jobId;
  }

  // Starts the heartbeat loop
  async startHeartbeat(
    interval_s: number = 30,
    onCanceled: () => Promise<void>
  ): Promise<void> {
    this.active = true; // Set the loop to be active
    this.log.info("Heartbeat started.");

    while (this.active) {
      const { status } = await sendHeartbeat(this.jobId, this.log); // Call your sendHeartbeat function
      if (status === "canceled") {
        this.log.info("Job was canceled, stopping heartbeat.");
        await onCanceled();
        break;
      }
      this.waiter = sleep(interval_s * 1000);
      await this.waiter; // Wait for 30 seconds before the next heartbeat
    }

    this.log.info(`Heartbeat stopped.`);
  }

  // Stops the heartbeat loop
  async stopHeartbeat(): Promise<void> {
    this.log.info("Stopping heartbeat");
    this.active = false; // Set the loop to be inactive
    if (this.waiter) {
      await this.waiter; // Wait for the last heartbeat to complete
      this.waiter = null;
    }
  }
}
