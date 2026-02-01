use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::cron::CronJob;
use crate::defaults::{
    DEFAULT_DLQ_NAME, DEFAULT_EXPECTED_JOB_TTL, DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS, DEFAULT_MAX_RETRIES, DEFAULT_POLL_DELAY_SECONDS,
    DEFAULT_QUEUE_NAME, DEFAULT_REDIS_DSN, DEFAULT_RESULT_TTL_SECONDS,
    DEFAULT_RUNNER_CONNECT_TIMEOUT_MS, DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunnerType {
    #[default]
    Socket,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    #[serde(default = "default_runner_type", rename = "type")]
    pub runner_type: RunnerType,
    pub cmd: Option<Vec<String>>,
    pub pool_size: Option<usize>,
    pub max_in_flight: Option<usize>,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
    pub tcp_socket: Option<String>,
    pub response_timeout_seconds: Option<f64>,
}

fn default_runner_type() -> RunnerType {
    RunnerType::Socket
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
    pub runner_connect_timeout_ms: i64,
    pub default_unique_job_lock_ttl_seconds: i64,
    pub default_runner_name: String,
    pub runners: HashMap<String, RunnerConfig>,
    pub runner_routes: HashMap<String, String>,
    pub worker_health_check_interval_seconds: f64,
    pub worker_health_check_ttl_buffer_seconds: f64,
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
            redis_dsn: DEFAULT_REDIS_DSN.to_string(),
            default_queue_name: DEFAULT_QUEUE_NAME.to_string(),
            default_dlq_name: DEFAULT_DLQ_NAME.to_string(),
            default_max_retries: DEFAULT_MAX_RETRIES,
            default_job_timeout_seconds: DEFAULT_JOB_TIMEOUT_SECONDS,
            default_lock_timeout_extension_seconds: DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS,
            default_result_ttl_seconds: DEFAULT_RESULT_TTL_SECONDS,
            default_poll_delay_seconds: DEFAULT_POLL_DELAY_SECONDS,
            runner_connect_timeout_ms: DEFAULT_RUNNER_CONNECT_TIMEOUT_MS,
            default_unique_job_lock_ttl_seconds: DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
            default_runner_name: "python".to_string(),
            runners: HashMap::new(),
            runner_routes: HashMap::new(),
            worker_health_check_interval_seconds: 60.0,
            worker_health_check_ttl_buffer_seconds: 10.0,
            base_retry_delay_seconds: 5.0,
            max_retry_delay_seconds: 60.0 * 60.0,
            worker_shutdown_grace_period_seconds: 10.0,
            cron_jobs: Vec::new(),
            expected_job_ttl: DEFAULT_EXPECTED_JOB_TTL,
            watch: WatchSettings::default(),
        }
    }
}
