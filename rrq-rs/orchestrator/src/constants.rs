pub use rrq_config::defaults::{
    DEFAULT_DLQ_NAME, DEFAULT_EXPECTED_JOB_TTL, DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS, DEFAULT_MAX_RETRIES, DEFAULT_POLL_DELAY_SECONDS,
    DEFAULT_QUEUE_NAME, DEFAULT_RESULT_TTL_SECONDS, DEFAULT_RUNNER_CONNECT_TIMEOUT_MS,
    DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
};

pub const JOB_KEY_PREFIX: &str = "rrq:job:";
pub const QUEUE_KEY_PREFIX: &str = "rrq:queue:";
pub const DLQ_KEY_PREFIX: &str = "rrq:dlq:";
pub const ACTIVE_JOBS_PREFIX: &str = "rrq:active:jobs:";
pub const LOCK_KEY_PREFIX: &str = "rrq:lock:job:";
pub const UNIQUE_JOB_LOCK_PREFIX: &str = "rrq:lock:unique:";
pub const HEALTH_KEY_PREFIX: &str = "rrq:health:worker:";

pub const DEFAULT_DLQ_RESULT_TTL_SECONDS: i64 = 60 * 60 * 24 * 7;

pub const DEFAULT_WORKER_ID_PREFIX: &str = "rrq_worker_";
