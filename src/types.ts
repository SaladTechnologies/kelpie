export type TaskStatus =
  | "pending"
  | "running"
  | "completed"
  | "canceled"
  | "failed";

export type SyncConfig = {
  bucket: string;
  prefix: string;
  local_path: string;
  direction: "download" | "upload";
  pattern?: string;
};

export interface Task {
  id: string;
  user_id: string;
  status: TaskStatus;
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
  input_bucket?: string;
  input_prefix?: string;
  checkpoint_bucket?: string;
  checkpoint_prefix?: string;
  output_bucket?: string;
  output_prefix?: string;
  heartbeat_interval: number;
  max_failures: number;
  webhook?: string;
  container_group_id: string;
  compression?: boolean;
  sync?: {
    before?: SyncConfig[];
    during?: SyncConfig[];
    after?: SyncConfig[];
  };
}
