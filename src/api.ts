import assert from "assert";
import { log } from "./logger";

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

interface Task {
  id: string;
  user_id: string;
  status: "pending" | "running" | "completed" | "canceled" | "failed";
  created: string;
  started?: string;
  completed?: string;
  canceled?: string;
  failed?: string;
  heartbeat?: string;
  num_failures: number;
  machine_id: string;
  command: string;
  arguments: any[];
  environment: Record<string, string>;
  input_bucket: string;
  input_prefix: string;
  checkpoint_bucket: string;
  checkpoint_prefix: string;
  output_bucket: string;
  output_prefix: string;
  heartbeat_interval: number;
  max_failures: number;
  webhook?: string;
  container_group_id: string;
  compression?: boolean;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchUpToNTimes<T>(
  url: string,
  params: any,
  n: number
): Promise<T> {
  let retries = 0;
  while (retries < n) {
    try {
      const response = await fetch(url, params);
      if (response.ok) {
        return response.json() as Promise<T>;
      } else {
        log.error("Error fetching data, retrying: ", await response.text());
        retries++;
        await sleep(retries * 1000);
        continue;
      }
    } catch (err: any) {
      log.error("Error fetching data, retrying: ", err.message);
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
  jobId: string
): Promise<{ status: Task["status"] }> {
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
    maxRetries
  );
  return { status };
}

export async function reportFailed(jobId: string): Promise<void> {
  log.info(`Reporting job ${jobId} as failed`);
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
    maxRetries
  );
}

export async function reportCompleted(jobId: string): Promise<void> {
  log.info(`Reporting job ${jobId} as completed`);
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
    maxRetries
  );
}

export class HeartbeatManager {
  private active: boolean = false;
  private jobId: string = "";
  private waiter: Promise<void> | null = null;

  // Starts the heartbeat loop
  async startHeartbeat(
    jobId: string,
    interval_s: number = 30,
    onCanceled: () => Promise<void>
  ): Promise<void> {
    this.active = true; // Set the loop to be active
    this.jobId = jobId;
    log.info("Heartbeat started.");

    while (this.active) {
      const { status } = await sendHeartbeat(jobId); // Call your sendHeartbeat function
      if (status === "canceled") {
        log.info(`Job ${this.jobId} was canceled, stopping heartbeat.`);
        await onCanceled();
        break;
      }
      this.waiter = sleep(interval_s * 1000);
      await this.waiter; // Wait for 30 seconds before the next heartbeat
    }

    log.info(`Heartbeat stopped fir job ${this.jobId}`);
  }

  // Stops the heartbeat loop
  async stopHeartbeat(): Promise<void> {
    log.info(`Stopping heartbeat for job ${this.jobId}`);
    this.active = false; // Set the loop to be inactive
    if (this.waiter) {
      await this.waiter; // Wait for the last heartbeat to complete
      this.waiter = null;
    }
  }
}
