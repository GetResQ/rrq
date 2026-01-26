pub const DEFAULT_QUEUE_NAME: &str = "rrq:queue:default";
pub const DEFAULT_DLQ_NAME: &str = "rrq:dlq:default";

pub const JOB_KEY_PREFIX: &str = "rrq:job:";
pub const QUEUE_KEY_PREFIX: &str = "rrq:queue:";
pub const DLQ_KEY_PREFIX: &str = "rrq:dlq:";
pub const ACTIVE_JOBS_PREFIX: &str = "rrq:active:jobs:";
pub const LOCK_KEY_PREFIX: &str = "rrq:lock:job:";
pub const UNIQUE_JOB_LOCK_PREFIX: &str = "rrq:lock:unique:";
pub const HEALTH_KEY_PREFIX: &str = "rrq:health:worker:";

pub const DEFAULT_MAX_RETRIES: i64 = 5;
pub const DEFAULT_JOB_TIMEOUT_SECONDS: i64 = 300;
pub const DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS: i64 = 60;
pub const DEFAULT_RESULT_TTL_SECONDS: i64 = 60 * 60 * 24;
pub const DEFAULT_DLQ_RESULT_TTL_SECONDS: i64 = 60 * 60 * 24 * 7;
pub const DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS: i64 = 60 * 60 * 6;

pub const DEFAULT_POLL_DELAY_SECONDS: f64 = 0.1;

pub const DEFAULT_WORKER_ID_PREFIX: &str = "rrq_worker_";
