import {
  DEFAULT_DLQ_NAME,
  DEFAULT_JOB_TIMEOUT_SECONDS,
  DEFAULT_MAX_RETRIES,
  DEFAULT_QUEUE_NAME,
  DEFAULT_RESULT_TTL_SECONDS,
  DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
} from "./constants.js";

export interface RRQSettings {
  redisDsn: string;
  defaultQueueName: string;
  defaultDlqName: string;
  defaultMaxRetries: number;
  defaultJobTimeoutSeconds: number;
  defaultResultTtlSeconds: number;
  defaultUniqueJobLockTtlSeconds: number;
}

export const DEFAULT_SETTINGS: RRQSettings = {
  redisDsn: "redis://localhost:6379/0",
  defaultQueueName: DEFAULT_QUEUE_NAME,
  defaultDlqName: DEFAULT_DLQ_NAME,
  defaultMaxRetries: DEFAULT_MAX_RETRIES,
  defaultJobTimeoutSeconds: DEFAULT_JOB_TIMEOUT_SECONDS,
  defaultResultTtlSeconds: DEFAULT_RESULT_TTL_SECONDS,
  defaultUniqueJobLockTtlSeconds: DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
};

export function resolveSettings(overrides?: Partial<RRQSettings>): RRQSettings {
  return {
    ...DEFAULT_SETTINGS,
    ...overrides,
  };
}
