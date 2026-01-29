//! RRQ Producer - A production-ready Redis job queue producer.
//!
//! Features:
//! - Auto-reconnecting connection via ConnectionManager
//! - Atomic job enqueue operations
//! - Job result polling with timeout
//! - Trace context propagation support
//! - Trait-based design for easy mocking in tests

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

const JOB_KEY_PREFIX: &str = "rrq:job:";
const QUEUE_KEY_PREFIX: &str = "rrq:queue:";

/// Job status as stored in Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Active,
    Completed,
    Failed,
    Retrying,
    Unknown,
}

impl JobStatus {
    fn from_str(s: &str) -> Self {
        match s {
            "PENDING" => Self::Pending,
            "ACTIVE" => Self::Active,
            "COMPLETED" => Self::Completed,
            "FAILED" => Self::Failed,
            "RETRYING" => Self::Retrying,
            _ => Self::Unknown,
        }
    }
}

/// Result of a completed or failed job.
#[derive(Debug, Clone)]
pub struct JobResult {
    pub status: JobStatus,
    pub result: Option<Value>,
    pub last_error: Option<String>,
}

/// Options for enqueuing a job.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    pub queue_name: Option<String>,
    pub job_id: Option<String>,
    pub max_retries: Option<i64>,
    pub job_timeout_seconds: Option<i64>,
    pub result_ttl_seconds: Option<i64>,
    pub enqueue_time: Option<DateTime<Utc>>,
    pub scheduled_time: Option<DateTime<Utc>>,
    pub trace_context: Option<HashMap<String, String>>,
}

/// Trait for RRQ producer operations - enables mocking in tests.
#[async_trait]
pub trait ProducerHandle: Send + Sync {
    /// Enqueue a job for processing.
    async fn enqueue(
        &self,
        function_name: &str,
        args: Vec<Value>,
        kwargs: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String>;

    /// Wait for a job to complete, polling at the given interval.
    async fn wait_for_result(
        &self,
        job_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<JobResult>;
}

/// RRQ Producer with auto-reconnecting Redis connection.
#[derive(Clone)]
pub struct Producer {
    manager: ConnectionManager,
    default_queue_name: String,
    default_max_retries: i64,
    default_job_timeout_seconds: i64,
    default_result_ttl_seconds: i64,
}

/// Configuration for creating a Producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub queue_name: String,
    pub max_retries: i64,
    pub job_timeout_seconds: i64,
    pub result_ttl_seconds: i64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            max_retries: 3,
            job_timeout_seconds: 300,
            result_ttl_seconds: 3600,
        }
    }
}

impl Producer {
    /// Create a new Producer with default configuration.
    pub async fn new(redis_dsn: impl AsRef<str>) -> Result<Self> {
        Self::with_config(redis_dsn, ProducerConfig::default()).await
    }

    /// Create a new Producer with custom configuration.
    pub async fn with_config(redis_dsn: impl AsRef<str>, config: ProducerConfig) -> Result<Self> {
        let client = redis::Client::open(redis_dsn.as_ref())
            .with_context(|| "failed to create Redis client")?;
        let manager = ConnectionManager::new(client)
            .await
            .with_context(|| "failed to connect to Redis")?;
        Ok(Self {
            manager,
            default_queue_name: config.queue_name,
            default_max_retries: config.max_retries,
            default_job_timeout_seconds: config.job_timeout_seconds,
            default_result_ttl_seconds: config.result_ttl_seconds,
        })
    }

    /// Create a Producer from an existing ConnectionManager.
    pub fn with_connection(manager: ConnectionManager, config: ProducerConfig) -> Self {
        Self {
            manager,
            default_queue_name: config.queue_name,
            default_max_retries: config.max_retries,
            default_job_timeout_seconds: config.job_timeout_seconds,
            default_result_ttl_seconds: config.result_ttl_seconds,
        }
    }

    /// Enqueue a job for processing.
    pub async fn enqueue(
        &self,
        function_name: &str,
        args: Vec<Value>,
        kwargs: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String> {
        let queue_name = options
            .queue_name
            .unwrap_or_else(|| self.default_queue_name.clone());
        let job_id = options.job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let enqueue_time = options.enqueue_time.unwrap_or_else(Utc::now);
        let scheduled_time = options.scheduled_time.unwrap_or(enqueue_time);
        let max_retries = options.max_retries.unwrap_or(self.default_max_retries);
        let job_timeout_seconds = options
            .job_timeout_seconds
            .unwrap_or(self.default_job_timeout_seconds);
        let result_ttl_seconds = options
            .result_ttl_seconds
            .unwrap_or(self.default_result_ttl_seconds);

        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = format_queue_key(&queue_name);
        let score_ms = scheduled_time.timestamp_millis() as f64;

        let job_args_json = serde_json::to_string(&args)?;
        let job_kwargs_json = serde_json::to_string(&kwargs)?;

        // Build the job hash fields
        let mut conn = self.manager.clone();
        let mut pipe = redis::pipe();
        pipe.atomic();

        pipe.hset(&job_key, "id", &job_id)
            .hset(&job_key, "function_name", function_name)
            .hset(&job_key, "job_args", &job_args_json)
            .hset(&job_key, "job_kwargs", &job_kwargs_json)
            .hset(&job_key, "enqueue_time", enqueue_time.to_rfc3339())
            .hset(&job_key, "status", "PENDING")
            .hset(&job_key, "current_retries", 0i64)
            .hset(
                &job_key,
                "next_scheduled_run_time",
                scheduled_time.to_rfc3339(),
            )
            .hset(&job_key, "max_retries", max_retries)
            .hset(&job_key, "job_timeout_seconds", job_timeout_seconds)
            .hset(&job_key, "result_ttl_seconds", result_ttl_seconds)
            .hset(&job_key, "queue_name", &queue_name)
            .hset(&job_key, "result", "null");

        if let Some(trace_context) = options.trace_context {
            let trace_json = serde_json::to_string(&trace_context)?;
            pipe.hset(&job_key, "trace_context", trace_json);
        }

        pipe.zadd(&queue_key, &job_id, score_ms);
        pipe.query_async::<()>(&mut conn).await?;

        Ok(job_id)
    }

    /// Wait for a job to complete, polling at the given interval.
    ///
    /// Returns when the job reaches COMPLETED or FAILED status, or when timeout is reached.
    pub async fn wait_for_result(
        &self,
        job_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<JobResult> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timeout waiting for job {job_id}");
            }

            let mut conn = self.manager.clone();
            let data: HashMap<String, String> = conn.hgetall(&job_key).await?;

            if data.is_empty() {
                tokio::time::sleep(poll_interval).await;
                continue;
            }

            let status = data
                .get("status")
                .map(|s| JobStatus::from_str(s))
                .unwrap_or(JobStatus::Unknown);

            match status {
                JobStatus::Completed => {
                    let result = data.get("result").and_then(|r| parse_result(r));
                    return Ok(JobResult {
                        status,
                        result,
                        last_error: None,
                    });
                }
                JobStatus::Failed => {
                    return Ok(JobResult {
                        status,
                        result: None,
                        last_error: data.get("last_error").cloned(),
                    });
                }
                _ => {
                    tokio::time::sleep(poll_interval).await;
                }
            }
        }
    }

    /// Get the current status of a job without waiting.
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<JobResult>> {
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let mut conn = self.manager.clone();
        let data: HashMap<String, String> = conn.hgetall(&job_key).await?;

        if data.is_empty() {
            return Ok(None);
        }

        let status = data
            .get("status")
            .map(|s| JobStatus::from_str(s))
            .unwrap_or(JobStatus::Unknown);

        let result = data.get("result").and_then(|r| parse_result(r));
        let last_error = data.get("last_error").cloned();

        Ok(Some(JobResult {
            status,
            result,
            last_error,
        }))
    }
}

#[async_trait]
impl ProducerHandle for Producer {
    async fn enqueue(
        &self,
        function_name: &str,
        args: Vec<Value>,
        kwargs: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String> {
        self.enqueue(function_name, args, kwargs, options).await
    }

    async fn wait_for_result(
        &self,
        job_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<JobResult> {
        self.wait_for_result(job_id, timeout, poll_interval).await
    }
}

fn format_queue_key(queue_name: &str) -> String {
    if queue_name.starts_with(QUEUE_KEY_PREFIX) {
        queue_name.to_string()
    } else {
        format!("{QUEUE_KEY_PREFIX}{queue_name}")
    }
}

fn parse_result(result: &str) -> Option<Value> {
    if result.is_empty() || result == "null" {
        return None;
    }
    serde_json::from_str(result).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::OnceLock;
    use tokio::sync::Mutex;

    static REDIS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn redis_lock() -> &'static Mutex<()> {
        REDIS_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn format_queue_key_adds_prefix() {
        assert_eq!(format_queue_key("default"), "rrq:queue:default");
    }

    #[test]
    fn format_queue_key_preserves_prefix() {
        assert_eq!(format_queue_key("rrq:queue:custom"), "rrq:queue:custom");
    }

    #[test]
    fn job_status_from_str() {
        assert_eq!(JobStatus::from_str("PENDING"), JobStatus::Pending);
        assert_eq!(JobStatus::from_str("ACTIVE"), JobStatus::Active);
        assert_eq!(JobStatus::from_str("COMPLETED"), JobStatus::Completed);
        assert_eq!(JobStatus::from_str("FAILED"), JobStatus::Failed);
        assert_eq!(JobStatus::from_str("RETRYING"), JobStatus::Retrying);
        assert_eq!(JobStatus::from_str("UNKNOWN"), JobStatus::Unknown);
        assert_eq!(JobStatus::from_str("garbage"), JobStatus::Unknown);
    }

    #[tokio::test]
    async fn producer_enqueue_writes_job_and_queue() {
        let _guard = redis_lock().lock().await;
        let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
            .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
        let client = redis::Client::open(dsn.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

        let producer = Producer::new(&dsn).await.unwrap();
        let job_id = producer
            .enqueue(
                "work",
                vec![json!(1)],
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await
            .unwrap();

        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let status: String = conn.hget(&job_key, "status").await.unwrap();
        assert_eq!(status, "PENDING");
        let queue_key = format_queue_key("default");
        let score: Option<f64> = conn.zscore(queue_key, &job_id).await.unwrap();
        assert!(score.is_some());
    }

    #[tokio::test]
    async fn producer_with_config() {
        let _guard = redis_lock().lock().await;
        let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
            .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
        let client = redis::Client::open(dsn.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

        let config = ProducerConfig {
            queue_name: "custom-queue".to_string(),
            max_retries: 5,
            job_timeout_seconds: 600,
            result_ttl_seconds: 7200,
        };
        let producer = Producer::with_config(&dsn, config).await.unwrap();
        let job_id = producer
            .enqueue(
                "work",
                vec![],
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await
            .unwrap();

        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let max_retries: i64 = conn.hget(&job_key, "max_retries").await.unwrap();
        assert_eq!(max_retries, 5);
        let queue_name: String = conn.hget(&job_key, "queue_name").await.unwrap();
        assert_eq!(queue_name, "custom-queue");
    }

    #[tokio::test]
    async fn producer_get_job_status() {
        let _guard = redis_lock().lock().await;
        let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
            .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
        let client = redis::Client::open(dsn.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

        let producer = Producer::new(&dsn).await.unwrap();
        let job_id = producer
            .enqueue(
                "work",
                vec![],
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await
            .unwrap();

        let result = producer.get_job_status(&job_id).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().status, JobStatus::Pending);

        // Non-existent job
        let result = producer.get_job_status("non-existent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn producer_connects_with_tls_when_configured() {
        let Ok(dsn) = std::env::var("RRQ_TEST_REDIS_TLS_DSN") else {
            eprintln!("Skipping TLS test: RRQ_TEST_REDIS_TLS_DSN not set");
            return;
        };

        let result = Producer::new(&dsn).await;
        assert!(
            result.is_ok(),
            "Failed to connect to Redis with TLS: {:?}",
            result.err()
        );
    }
}
