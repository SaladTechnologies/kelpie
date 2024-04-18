import assert from "assert";

let {
  KELPIE_API_URL,
  KELPIE_API_KEY,
  SALAD_MACHINE_ID = "",
  SALAD_CONTAINER_GROUP_ID = "",
} = process.env;

assert(KELPIE_API_URL, "KELPIE_API_URL is required");
assert(KELPIE_API_KEY, "KELPIE_API_KEY is required");

if (KELPIE_API_URL.endsWith("/")) {
  KELPIE_API_URL = KELPIE_API_URL.slice(0, -1);
}

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
  input_bucket: string;
  input_prefix: string;
  checkpoint_bucket: string;
  checkpoint_prefix: string;
  output_bucket: string;
  output_prefix: string;
  webhook?: string;
  container_group_id: string;
}

export async function getWork(): Promise<Task | null> {
  try {
    const query = new URLSearchParams({
      machine_id: SALAD_MACHINE_ID,
      container_group_id: SALAD_CONTAINER_GROUP_ID,
    }).toString();
    const response = await fetch(`${KELPIE_API_URL}/work?${query}`, {
      method: "GET",
      headers,
    });
    if (response.ok) {
      const work = (await response.json()) as Task[];
      if (work.length) {
        return work[0];
      }
    }
    return null;
  } catch (err) {
    console.error("Error getting work: ", err);
    return null;
  }
}

export async function sendHeartbeat(
  jobId: string
): Promise<{ status: Task["status"] }> {
  const response = await fetch(`${KELPIE_API_URL}/jobs/${jobId}/heartbeat`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      machine_id: SALAD_MACHINE_ID,
      container_group_id: SALAD_CONTAINER_GROUP_ID,
    }),
  });
  if (!response.ok) {
    throw new Error(`Error sending heartbeat: ${await response.text()}`);
  }
  return response.json() as Promise<{ status: Task["status"] }>;
}

export async function reportFailed(jobId: string): Promise<void> {
  const response = await fetch(`${KELPIE_API_URL}/jobs/${jobId}/failed`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      machine_id: SALAD_MACHINE_ID,
      container_group_id: SALAD_CONTAINER_GROUP_ID,
    }),
  });
  if (!response.ok) {
    throw new Error(`Error reporting failed: ${await response.text()}`);
  }
}

export async function reportCompleted(jobId: string): Promise<void> {
  const response = await fetch(`${KELPIE_API_URL}/jobs/${jobId}/completed`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      machine_id: SALAD_MACHINE_ID,
      container_group_id: SALAD_CONTAINER_GROUP_ID,
    }),
  });
  if (!response.ok) {
    throw new Error(`Error reporting completed: ${await response.text()}`);
  }
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class HeartbeatManager {
  private active: boolean = false;

  // Starts the heartbeat loop
  async startHeartbeat(
    jobId: string,
    onCanceled: () => Promise<void>
  ): Promise<void> {
    this.active = true; // Set the loop to be active
    console.log("Heartbeat started.");

    while (this.active) {
      const { status } = await sendHeartbeat(jobId); // Call your sendHeartbeat function
      if (status === "canceled") {
        console.log("Job was canceled, stopping heartbeat.");
        await onCanceled();
        break;
      }
      await sleep(30000); // Wait for 30 seconds before the next heartbeat
    }

    console.log("Heartbeat stopped.");
  }

  // Stops the heartbeat loop
  stopHeartbeat(): void {
    this.active = false; // Set the loop to be inactive
  }
}
