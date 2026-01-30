export const JOB_KEY_PREFIX = "rrq:job:";
export const QUEUE_KEY_PREFIX = "rrq:queue:";
export const DLQ_KEY_PREFIX = "rrq:dlq:";
export const UNIQUE_JOB_LOCK_PREFIX = "rrq:lock:unique:";

export const DEFAULT_QUEUE_NAME = "rrq:queue:default";
export const DEFAULT_DLQ_NAME = "rrq:dlq:default";
export const DEFAULT_MAX_RETRIES = 5;
export const DEFAULT_JOB_TIMEOUT_SECONDS = 300;
export const DEFAULT_RESULT_TTL_SECONDS = 24 * 60 * 60;
export const DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS = 6 * 60 * 60;
