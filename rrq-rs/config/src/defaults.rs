pub const DEFAULT_REDIS_DSN: &str = "redis://localhost:6379/0";
pub const DEFAULT_QUEUE_NAME: &str = "rrq:queue:default";
pub const DEFAULT_DLQ_NAME: &str = "rrq:dlq:default";

pub const DEFAULT_MAX_RETRIES: i64 = 5;
pub const DEFAULT_JOB_TIMEOUT_SECONDS: i64 = 300;
pub const DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS: i64 = 60;
pub const DEFAULT_RESULT_TTL_SECONDS: i64 = 60 * 60 * 24;
pub const DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS: i64 = 60 * 60 * 6;
pub const DEFAULT_RUNNER_CONNECT_TIMEOUT_MS: i64 = 15_000;
pub const DEFAULT_POLL_DELAY_SECONDS: f64 = 0.1;
pub const DEFAULT_EXPECTED_JOB_TTL: i64 = 30;
