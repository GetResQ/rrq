use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::constants::{
    DEFAULT_DLQ_NAME, DEFAULT_JOB_TIMEOUT_SECONDS, DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
    DEFAULT_MAX_RETRIES, DEFAULT_POLL_DELAY_SECONDS, DEFAULT_QUEUE_NAME,
    DEFAULT_RESULT_TTL_SECONDS, DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
};
use crate::cron::CronJob;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorType {
    #[default]
    Socket,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutorConfig {
    #[serde(default = "default_executor_type", rename = "type")]
    pub executor_type: ExecutorType,
    pub cmd: Option<Vec<String>>,
    pub pool_size: Option<usize>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
    pub socket_dir: Option<String>,
    pub response_timeout_seconds: Option<f64>,
}

fn default_executor_type() -> ExecutorType {
    ExecutorType::Socket
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", default)]
pub struct WatchSettings {
    pub path: Option<String>,
    pub include_patterns: Vec<String>,
    pub ignore_patterns: Vec<String>,
    pub no_gitignore: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct RRQSettings {
    pub redis_dsn: String,
    pub default_queue_name: String,
    pub default_dlq_name: String,
    pub default_max_retries: i64,
    pub default_job_timeout_seconds: i64,
    pub default_lock_timeout_extension_seconds: i64,
    pub default_result_ttl_seconds: i64,
    pub default_poll_delay_seconds: f64,
    pub default_unique_job_lock_ttl_seconds: i64,
    pub worker_concurrency: usize,
    pub default_executor_name: String,
    pub executors: HashMap<String, ExecutorConfig>,
    pub executor_routes: HashMap<String, String>,
    pub worker_health_check_interval_seconds: f64,
    pub base_retry_delay_seconds: f64,
    pub max_retry_delay_seconds: f64,
    pub worker_shutdown_grace_period_seconds: f64,
    pub cron_jobs: Vec<CronJob>,
    pub expected_job_ttl: i64,
    pub watch: WatchSettings,
}

impl Default for RRQSettings {
    fn default() -> Self {
        Self {
            redis_dsn: "redis://localhost:6379/0".to_string(),
            default_queue_name: DEFAULT_QUEUE_NAME.to_string(),
            default_dlq_name: DEFAULT_DLQ_NAME.to_string(),
            default_max_retries: DEFAULT_MAX_RETRIES,
            default_job_timeout_seconds: DEFAULT_JOB_TIMEOUT_SECONDS,
            default_lock_timeout_extension_seconds: DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
            default_result_ttl_seconds: DEFAULT_RESULT_TTL_SECONDS,
            default_poll_delay_seconds: DEFAULT_POLL_DELAY_SECONDS,
            default_unique_job_lock_ttl_seconds: DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
            worker_concurrency: 10,
            default_executor_name: "python".to_string(),
            executors: HashMap::new(),
            executor_routes: HashMap::new(),
            worker_health_check_interval_seconds: 60.0,
            base_retry_delay_seconds: 5.0,
            max_retry_delay_seconds: 60.0 * 60.0,
            worker_shutdown_grace_period_seconds: 10.0,
            cron_jobs: Vec::new(),
            expected_job_ttl: 30,
            watch: WatchSettings::default(),
        }
    }
}
