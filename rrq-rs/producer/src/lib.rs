//! RRQ Producer - A production-ready Redis job queue producer.
//!
//! Features:
//! - Auto-reconnecting connection via ConnectionManager
//! - Atomic job enqueue operations
//! - Job result polling with timeout
//! - Trace context propagation support
//! - Trait-based design for easy mocking in tests
//!
//! # TLS Support
//!
//! This library uses rustls with embedded Mozilla CA roots for TLS connections.
//! Before creating a Producer with a TLS Redis URL, you must initialize the
//! crypto provider by calling [`init_crypto_provider`] once at application startup.
//!
//! ```no_run
//! rrq_producer::init_crypto_provider();
//! ```

use anyhow::{Context, Result};
use opentelemetry::global;
use opentelemetry::propagation::Injector;
use std::sync::Once;
use tracing_opentelemetry::OpenTelemetrySpanExt;

static CRYPTO_PROVIDER_INIT: Once = Once::new();

/// Initialize the rustls crypto provider (ring) for TLS connections.
///
/// This must be called once before creating a Producer with a TLS Redis URL.
/// It is safe to call multiple times; subsequent calls are no-ops.
///
/// # Example
///
/// ```no_run
/// rrq_producer::init_crypto_provider();
/// // Now you can create producers with TLS Redis URLs
/// ```
pub fn init_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::Script;
use redis::aio::ConnectionManager;
use rrq_config::defaults::{
    DEFAULT_JOB_TIMEOUT_SECONDS, DEFAULT_MAX_RETRIES, DEFAULT_QUEUE_NAME,
    DEFAULT_RESULT_TTL_SECONDS, DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

#[allow(dead_code)]
mod ffi;

const JOB_KEY_PREFIX: &str = "rrq:job:";
const JOB_EVENTS_KEY_PREFIX: &str = "rrq:events:job:";
const QUEUE_KEY_PREFIX: &str = "rrq:queue:";
const IDEMPOTENCY_KEY_PREFIX: &str = "rrq:idempotency:";
const RATE_LIMIT_KEY_PREFIX: &str = "rrq:rate_limit:";
const DEBOUNCE_KEY_PREFIX: &str = "rrq:debounce:";

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
    pub idempotency_key: Option<String>,
    pub idempotency_ttl_seconds: Option<i64>,
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
        params: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String>;

    /// Wait for a queued job to reach a terminal state (COMPLETED or FAILED).
    async fn wait_for_completion(
        &self,
        job_id: &str,
        timeout: Duration,
        block_interval: Duration,
    ) -> Result<Option<JobResult>>;
}

/// RRQ Producer with auto-reconnecting Redis connection.
#[derive(Clone)]
pub struct Producer {
    manager: ConnectionManager,
    default_queue_name: String,
    default_max_retries: i64,
    default_job_timeout_seconds: i64,
    default_result_ttl_seconds: i64,
    default_idempotency_ttl_seconds: i64,
    correlation_mappings: HashMap<String, String>,
    enqueue_script: Script,
    rate_limit_script: Script,
    debounce_script: Script,
}

/// Configuration for creating a Producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub queue_name: String,
    pub max_retries: i64,
    pub job_timeout_seconds: i64,
    pub result_ttl_seconds: i64,
    pub idempotency_ttl_seconds: i64,
    pub correlation_mappings: HashMap<String, String>,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            queue_name: DEFAULT_QUEUE_NAME.to_string(),
            max_retries: DEFAULT_MAX_RETRIES,
            job_timeout_seconds: DEFAULT_JOB_TIMEOUT_SECONDS,
            result_ttl_seconds: DEFAULT_RESULT_TTL_SECONDS,
            idempotency_ttl_seconds: DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS,
            correlation_mappings: HashMap::new(),
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
            default_idempotency_ttl_seconds: config.idempotency_ttl_seconds,
            correlation_mappings: config.correlation_mappings,
            enqueue_script: build_enqueue_script(),
            rate_limit_script: build_rate_limit_script(),
            debounce_script: build_debounce_script(),
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
            default_idempotency_ttl_seconds: config.idempotency_ttl_seconds,
            correlation_mappings: config.correlation_mappings,
            enqueue_script: build_enqueue_script(),
            rate_limit_script: build_rate_limit_script(),
            debounce_script: build_debounce_script(),
        }
    }

    /// Enqueue a job for processing.
    pub async fn enqueue(
        &self,
        function_name: &str,
        params: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String> {
        validate_name("function_name", function_name)?;
        let queue_name = options
            .queue_name
            .unwrap_or_else(|| self.default_queue_name.clone());
        validate_name("queue_name", &queue_name)?;
        let queue_name = format_queue_key(&queue_name);
        let job_id = options.job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let enqueue_time = options.enqueue_time.unwrap_or_else(Utc::now);
        let scheduled_time = options.scheduled_time.unwrap_or(enqueue_time);
        let max_retries = options.max_retries.unwrap_or(self.default_max_retries);
        let job_timeout_seconds = options
            .job_timeout_seconds
            .unwrap_or(self.default_job_timeout_seconds);
        if job_timeout_seconds <= 0 {
            anyhow::bail!("job_timeout_seconds must be positive");
        }
        let result_ttl_seconds = options
            .result_ttl_seconds
            .unwrap_or(self.default_result_ttl_seconds);

        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = queue_name.clone();
        let score_ms = scheduled_time.timestamp_millis() as f64;
        let idempotency_key = if let Some(key) = options.idempotency_key.as_deref() {
            validate_name("idempotency_key", key)?;
            format_idempotency_key(key)
        } else {
            String::new()
        };
        let mut idempotency_ttl_seconds = options
            .idempotency_ttl_seconds
            .unwrap_or(self.default_idempotency_ttl_seconds);
        if !idempotency_key.is_empty() {
            if idempotency_ttl_seconds <= 0 {
                anyhow::bail!("idempotency_ttl_seconds must be positive");
            }
            let deferral_ms = scheduled_time
                .signed_duration_since(enqueue_time)
                .num_milliseconds();
            if deferral_ms > 0 {
                let deferral_seconds = deferral_ms.saturating_add(999) / 1000;
                if deferral_seconds > idempotency_ttl_seconds {
                    idempotency_ttl_seconds = deferral_seconds;
                }
            }
        }

        let trace_context = merge_trace_context(options.trace_context);
        let correlation_context = extract_correlation_context(
            &params,
            &self.correlation_mappings,
            trace_context.as_ref(),
        );

        let job_params_json = serde_json::to_string(&params)?;
        let trace_context_json = if let Some(trace_context) = trace_context {
            serde_json::to_string(&trace_context)?
        } else {
            String::new()
        };
        let correlation_context_json = if let Some(correlation_context) = correlation_context {
            serde_json::to_string(&correlation_context)?
        } else {
            String::new()
        };

        // Enqueue atomically, preventing job_id collisions and supporting idempotency keys.
        let mut conn = self.manager.clone();
        let (status, returned_id): (i64, String) = self
            .enqueue_script
            .key(job_key)
            .key(queue_key)
            .key(idempotency_key)
            .arg(&job_id)
            .arg(function_name)
            .arg(&job_params_json)
            .arg(enqueue_time.to_rfc3339())
            .arg("PENDING")
            .arg(0i64)
            .arg(scheduled_time.to_rfc3339())
            .arg(max_retries)
            .arg(job_timeout_seconds)
            .arg(result_ttl_seconds)
            .arg(&queue_name)
            .arg("null")
            .arg(trace_context_json)
            .arg(correlation_context_json)
            .arg(score_ms)
            .arg(idempotency_ttl_seconds)
            .invoke_async(&mut conn)
            .await?;

        match status {
            1 => Ok(returned_id),
            0 => Ok(returned_id),
            -1 => anyhow::bail!("job_id already exists"),
            _ => anyhow::bail!("unexpected enqueue status"),
        }
    }

    /// Enqueue a job with a leading-edge rate limit.
    ///
    /// Returns Ok(None) when the rate limit key is already held.
    pub async fn enqueue_with_rate_limit(
        &self,
        function_name: &str,
        params: Map<String, Value>,
        rate_limit_key: &str,
        rate_limit_window: Duration,
        options: EnqueueOptions,
    ) -> Result<Option<String>> {
        validate_name("function_name", function_name)?;
        validate_name("rate_limit_key", rate_limit_key)?;
        let queue_name = options
            .queue_name
            .unwrap_or_else(|| self.default_queue_name.clone());
        validate_name("queue_name", &queue_name)?;
        let queue_name = format_queue_key(&queue_name);
        let job_id = options.job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let enqueue_time = options.enqueue_time.unwrap_or_else(Utc::now);
        let scheduled_time = options.scheduled_time.unwrap_or(enqueue_time);
        let max_retries = options.max_retries.unwrap_or(self.default_max_retries);
        let job_timeout_seconds = options
            .job_timeout_seconds
            .unwrap_or(self.default_job_timeout_seconds);
        if job_timeout_seconds <= 0 {
            anyhow::bail!("job_timeout_seconds must be positive");
        }
        let result_ttl_seconds = options
            .result_ttl_seconds
            .unwrap_or(self.default_result_ttl_seconds);

        let ttl_seconds = rate_limit_window.as_secs_f64().ceil() as i64;
        if ttl_seconds <= 0 {
            anyhow::bail!("rate_limit_window must be positive");
        }

        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = queue_name.clone();
        let rate_limit_key = format_rate_limit_key(rate_limit_key);
        let score_ms = scheduled_time.timestamp_millis() as f64;

        let trace_context = merge_trace_context(options.trace_context);
        let correlation_context = extract_correlation_context(
            &params,
            &self.correlation_mappings,
            trace_context.as_ref(),
        );

        let job_params_json = serde_json::to_string(&params)?;
        let trace_context_json = if let Some(trace_context) = trace_context {
            serde_json::to_string(&trace_context)?
        } else {
            String::new()
        };
        let correlation_context_json = if let Some(correlation_context) = correlation_context {
            serde_json::to_string(&correlation_context)?
        } else {
            String::new()
        };

        let mut conn = self.manager.clone();
        let (status, returned_id): (i64, String) = self
            .rate_limit_script
            .key(job_key)
            .key(queue_key)
            .key(rate_limit_key)
            .arg(&job_id)
            .arg(function_name)
            .arg(&job_params_json)
            .arg(enqueue_time.to_rfc3339())
            .arg("PENDING")
            .arg(0i64)
            .arg(scheduled_time.to_rfc3339())
            .arg(max_retries)
            .arg(job_timeout_seconds)
            .arg(result_ttl_seconds)
            .arg(&queue_name)
            .arg("null")
            .arg(trace_context_json)
            .arg(correlation_context_json)
            .arg(score_ms)
            .arg(ttl_seconds)
            .invoke_async(&mut conn)
            .await?;

        match status {
            1 => Ok(Some(returned_id)),
            2 => Ok(None),
            -1 => anyhow::bail!("job_id already exists"),
            _ => anyhow::bail!("unexpected enqueue status"),
        }
    }

    /// Enqueue a job using trailing-edge debounce semantics.
    ///
    /// Reuses a pending job for the debounce key when possible and reschedules it
    /// for `enqueue_time + debounce_window`.
    pub async fn enqueue_with_debounce(
        &self,
        function_name: &str,
        params: Map<String, Value>,
        debounce_key: &str,
        debounce_window: Duration,
        options: EnqueueOptions,
    ) -> Result<String> {
        validate_name("function_name", function_name)?;
        validate_name("debounce_key", debounce_key)?;
        let queue_name = options
            .queue_name
            .unwrap_or_else(|| self.default_queue_name.clone());
        validate_name("queue_name", &queue_name)?;
        let queue_name = format_queue_key(&queue_name);
        let job_id = options.job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let enqueue_time = options.enqueue_time.unwrap_or_else(Utc::now);
        let scheduled_time = options.scheduled_time.unwrap_or_else(|| {
            enqueue_time + chrono::Duration::from_std(debounce_window).unwrap_or_default()
        });
        let max_retries = options.max_retries.unwrap_or(self.default_max_retries);
        let job_timeout_seconds = options
            .job_timeout_seconds
            .unwrap_or(self.default_job_timeout_seconds);
        if job_timeout_seconds <= 0 {
            anyhow::bail!("job_timeout_seconds must be positive");
        }
        let result_ttl_seconds = options
            .result_ttl_seconds
            .unwrap_or(self.default_result_ttl_seconds);

        let ttl_seconds = debounce_window.as_secs_f64().ceil() as i64;
        if ttl_seconds <= 0 {
            anyhow::bail!("debounce_window must be positive");
        }

        let queue_key = queue_name.clone();
        let debounce_key = format_debounce_key(debounce_key);
        let score_ms = scheduled_time.timestamp_millis() as f64;

        let trace_context = merge_trace_context(options.trace_context);
        let correlation_context = extract_correlation_context(
            &params,
            &self.correlation_mappings,
            trace_context.as_ref(),
        );

        let job_params_json = serde_json::to_string(&params)?;
        let trace_context_json = if let Some(trace_context) = trace_context {
            serde_json::to_string(&trace_context)?
        } else {
            String::new()
        };
        let correlation_context_json = if let Some(correlation_context) = correlation_context {
            serde_json::to_string(&correlation_context)?
        } else {
            String::new()
        };

        let mut conn = self.manager.clone();
        let (status, returned_id): (i64, String) = self
            .debounce_script
            .key(queue_key)
            .key(debounce_key)
            .arg(JOB_KEY_PREFIX)
            .arg(&job_id)
            .arg(function_name)
            .arg(&job_params_json)
            .arg(enqueue_time.to_rfc3339())
            .arg("PENDING")
            .arg(0i64)
            .arg(scheduled_time.to_rfc3339())
            .arg(max_retries)
            .arg(job_timeout_seconds)
            .arg(result_ttl_seconds)
            .arg(&queue_name)
            .arg("null")
            .arg(trace_context_json)
            .arg(correlation_context_json)
            .arg(score_ms)
            .arg(ttl_seconds)
            .invoke_async(&mut conn)
            .await?;

        match status {
            1 | 0 => Ok(returned_id),
            -1 => anyhow::bail!("job_id already exists"),
            _ => anyhow::bail!("unexpected enqueue status"),
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

    /// Wait for a job to reach a terminal state using Redis stream notifications.
    pub async fn wait_for_completion(
        &self,
        job_id: &str,
        timeout: Duration,
        block_interval: Duration,
    ) -> Result<Option<JobResult>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut conn = self.manager.clone();
        let event_key = format_job_events_key(job_id);
        let mut stream_offset = "$".to_string();

        loop {
            if let Some(status) = self.get_job_status(job_id).await?
                && matches!(status.status, JobStatus::Completed | JobStatus::Failed)
            {
                return Ok(Some(status));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }

            let remaining = deadline.saturating_duration_since(now);
            let wait_for = if block_interval.is_zero() {
                remaining
            } else {
                block_interval.min(remaining)
            };
            let wait_ms = wait_for.as_millis().clamp(1, i64::MAX as u128) as i64;

            // Wait for a terminal event for this job key. We re-check job status on wake
            // because stream events are advisory and status is the source of truth.
            let response: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK")
                .arg(wait_ms)
                .arg("COUNT")
                .arg(1)
                .arg("STREAMS")
                .arg(&event_key)
                .arg(&stream_offset)
                .query_async(&mut conn)
                .await?;

            if let Some(last_seen_id) = extract_latest_stream_entry_id(&response) {
                stream_offset = last_seen_id;
            }
        }
    }
}

struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> Injector for HashMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.entry(key.to_string()).or_insert(value);
    }
}

fn merge_trace_context(
    trace_context: Option<HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    let mut merged = trace_context.unwrap_or_default();
    let current = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&current, &mut HashMapInjector(&mut merged));
    });
    if merged.is_empty() {
        return None;
    }
    Some(merged)
}

fn extract_correlation_context(
    params: &Map<String, Value>,
    mappings: &HashMap<String, String>,
    trace_context: Option<&HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    if mappings.is_empty() {
        return None;
    }

    const MAX_CORRELATION_KEYS: usize = 16;
    const MAX_CORRELATION_KEY_LEN: usize = 64;
    const MAX_CORRELATION_VALUE_LEN: usize = 256;

    let mut correlation = HashMap::new();

    for (attr_name, path) in mappings {
        if correlation.len() >= MAX_CORRELATION_KEYS {
            break;
        }
        let key = attr_name.trim();
        if key.is_empty() || key.len() > MAX_CORRELATION_KEY_LEN {
            continue;
        }
        if let Some(existing) = trace_context.and_then(|ctx| ctx.get(key))
            && !existing.is_empty()
        {
            correlation.insert(
                key.to_string(),
                truncate_utf8(existing, MAX_CORRELATION_VALUE_LEN),
            );
            continue;
        }

        let Some(raw) = lookup_value_in_params(params, path) else {
            continue;
        };
        let Some(value) = scalar_value_to_string(raw) else {
            continue;
        };
        correlation.insert(
            key.to_string(),
            truncate_utf8(&value, MAX_CORRELATION_VALUE_LEN),
        );
    }

    if correlation.is_empty() {
        return None;
    }
    Some(correlation)
}

fn lookup_value_in_params<'a>(params: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let trimmed = path.trim();
    let cleaned = trimmed.strip_prefix("params.").unwrap_or(trimmed);
    if cleaned.is_empty() {
        return None;
    }
    let mut parts = cleaned.split('.');
    let first = parts.next()?;
    let mut current = params.get(first)?;
    for part in parts {
        if part.is_empty() {
            return None;
        }
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn scalar_value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(v) if !v.is_empty() => Some(v.clone()),
        Value::Bool(v) => Some(v.to_string()),
        Value::Number(v) => Some(v.to_string()),
        _ => None,
    }
}

fn truncate_utf8(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_string();
    }
    let mut out = String::with_capacity(max_len);
    for ch in value.chars() {
        if out.len() + ch.len_utf8() > max_len {
            break;
        }
        out.push(ch);
    }
    out
}

#[async_trait]
impl ProducerHandle for Producer {
    async fn enqueue(
        &self,
        function_name: &str,
        params: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String> {
        self.enqueue(function_name, params, options).await
    }

    async fn wait_for_completion(
        &self,
        job_id: &str,
        timeout: Duration,
        block_interval: Duration,
    ) -> Result<Option<JobResult>> {
        self.wait_for_completion(job_id, timeout, block_interval)
            .await
    }
}

fn format_queue_key(queue_name: &str) -> String {
    if queue_name.starts_with(QUEUE_KEY_PREFIX) {
        queue_name.to_string()
    } else {
        format!("{QUEUE_KEY_PREFIX}{queue_name}")
    }
}

fn format_job_events_key(job_id: &str) -> String {
    format!("{JOB_EVENTS_KEY_PREFIX}{job_id}")
}

fn extract_latest_stream_entry_id(value: &redis::Value) -> Option<String> {
    let redis::Value::Array(streams) = value else {
        return None;
    };
    let redis::Value::Array(stream) = streams.last()? else {
        return None;
    };
    let redis::Value::Array(entries) = stream.get(1)? else {
        return None;
    };
    let redis::Value::Array(last_entry) = entries.last()? else {
        return None;
    };
    redis_value_to_string(last_entry.first()?)
}

fn redis_value_to_string(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

fn format_idempotency_key(key: &str) -> String {
    format!("{IDEMPOTENCY_KEY_PREFIX}{key}")
}

fn format_rate_limit_key(key: &str) -> String {
    if key.starts_with(RATE_LIMIT_KEY_PREFIX) {
        key.to_string()
    } else {
        format!("{RATE_LIMIT_KEY_PREFIX}{key}")
    }
}

fn format_debounce_key(key: &str) -> String {
    if key.starts_with(DEBOUNCE_KEY_PREFIX) {
        key.to_string()
    } else {
        format!("{DEBOUNCE_KEY_PREFIX}{key}")
    }
}

fn validate_name(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{label} cannot be empty");
    }
    Ok(())
}

fn build_enqueue_script() -> Script {
    let script = format!(
        "-- KEYS: [1] = job_key, [2] = queue_key, [3] = idempotency_key (optional)\n\
         -- ARGV: [1] = job_id, [2] = function_name, [3] = job_params\n\
         --       [4] = enqueue_time, [5] = status, [6] = current_retries\n\
         --       [7] = next_scheduled_run_time, [8] = max_retries\n\
         --       [9] = job_timeout_seconds, [10] = result_ttl_seconds\n\
         --       [11] = queue_name, [12] = result, [13] = trace_context_json\n\
         --       [14] = correlation_context_json, [15] = score_ms, [16] = idempotency_ttl_seconds\n\
         local idem_key = KEYS[3]\n\
         if idem_key ~= '' then\n\
             local existing = redis.call('GET', idem_key)\n\
             if existing then\n\
                 local existing_job_key = '{job_prefix}' .. existing\n\
                 if redis.call('EXISTS', existing_job_key) == 1 then\n\
                     return {{0, existing}}\n\
                 end\n\
                 redis.call('DEL', idem_key)\n\
             end\n\
         end\n\
         if redis.call('EXISTS', KEYS[1]) == 1 then\n\
             return {{-1, ARGV[1]}}\n\
         end\n\
         if idem_key ~= '' then\n\
             local ttl = tonumber(ARGV[16])\n\
             local set_ok = nil\n\
             if ttl and ttl > 0 then\n\
                 set_ok = redis.call('SET', idem_key, ARGV[1], 'NX', 'EX', ttl)\n\
             else\n\
                 set_ok = redis.call('SET', idem_key, ARGV[1], 'NX')\n\
             end\n\
             if not set_ok then\n\
                 local winner = redis.call('GET', idem_key)\n\
                 if winner then\n\
                     return {{0, winner}}\n\
                 end\n\
             end\n\
         end\n\
         redis.call('HSET', KEYS[1],\n\
             'id', ARGV[1],\n\
             'function_name', ARGV[2],\n\
             'job_params', ARGV[3],\n\
             'enqueue_time', ARGV[4],\n\
             'status', ARGV[5],\n\
             'current_retries', ARGV[6],\n\
             'next_scheduled_run_time', ARGV[7],\n\
             'max_retries', ARGV[8],\n\
             'job_timeout_seconds', ARGV[9],\n\
             'result_ttl_seconds', ARGV[10],\n\
             'queue_name', ARGV[11],\n\
         'result', ARGV[12])\n\
         if ARGV[13] ~= '' then\n\
             redis.call('HSET', KEYS[1], 'trace_context', ARGV[13])\n\
         end\n\
         if ARGV[14] ~= '' then\n\
             redis.call('HSET', KEYS[1], 'correlation_context', ARGV[14])\n\
         end\n\
         redis.call('ZADD', KEYS[2], ARGV[15], ARGV[1])\n\
         return {{1, ARGV[1]}}",
        job_prefix = JOB_KEY_PREFIX
    );
    Script::new(&script)
}

fn build_rate_limit_script() -> Script {
    let script = "\
        -- KEYS: [1] = job_key, [2] = queue_key, [3] = rate_limit_key\n\
        -- ARGV: [1] = job_id, [2] = function_name, [3] = job_params\n\
        --       [4] = enqueue_time, [5] = status, [6] = current_retries\n\
        --       [7] = next_scheduled_run_time, [8] = max_retries\n\
        --       [9] = job_timeout_seconds, [10] = result_ttl_seconds\n\
        --       [11] = queue_name, [12] = result, [13] = trace_context_json\n\
        --       [14] = correlation_context_json, [15] = score_ms, [16] = rate_limit_ttl_seconds\n\
        local rate_key = KEYS[3]\n\
        local rate_set = false\n\
        if rate_key ~= '' then\n\
            local ttl = tonumber(ARGV[16])\n\
            if not ttl or ttl <= 0 then\n\
                return {-2, ARGV[1]}\n\
            end\n\
            local ok = redis.call('SET', rate_key, ARGV[1], 'NX', 'EX', ttl)\n\
            if not ok then\n\
                return {2, ''}\n\
            end\n\
            rate_set = true\n\
        end\n\
        if redis.call('EXISTS', KEYS[1]) == 1 then\n\
            if rate_set then\n\
                redis.call('DEL', rate_key)\n\
            end\n\
            return {-1, ARGV[1]}\n\
        end\n\
        redis.call('HSET', KEYS[1],\n\
            'id', ARGV[1],\n\
            'function_name', ARGV[2],\n\
            'job_params', ARGV[3],\n\
            'enqueue_time', ARGV[4],\n\
            'status', ARGV[5],\n\
            'current_retries', ARGV[6],\n\
            'next_scheduled_run_time', ARGV[7],\n\
            'max_retries', ARGV[8],\n\
            'job_timeout_seconds', ARGV[9],\n\
            'result_ttl_seconds', ARGV[10],\n\
            'queue_name', ARGV[11],\n\
        'result', ARGV[12])\n\
        if ARGV[13] ~= '' then\n\
            redis.call('HSET', KEYS[1], 'trace_context', ARGV[13])\n\
        end\n\
        if ARGV[14] ~= '' then\n\
            redis.call('HSET', KEYS[1], 'correlation_context', ARGV[14])\n\
        end\n\
        redis.call('ZADD', KEYS[2], ARGV[15], ARGV[1])\n\
        return {1, ARGV[1]}";
    Script::new(script)
}

fn build_debounce_script() -> Script {
    let script = "\
        -- KEYS: [1] = queue_key, [2] = debounce_key\n\
        -- ARGV: [1] = job_prefix, [2] = job_id, [3] = function_name, [4] = job_params\n\
        --       [5] = enqueue_time, [6] = status, [7] = current_retries\n\
        --       [8] = next_scheduled_run_time, [9] = max_retries\n\
        --       [10] = job_timeout_seconds, [11] = result_ttl_seconds\n\
        --       [12] = queue_name, [13] = result, [14] = trace_context_json\n\
        --       [15] = correlation_context_json, [16] = score_ms, [17] = debounce_ttl_seconds\n\
        local existing_id = redis.call('GET', KEYS[2])\n\
        if existing_id then\n\
            local existing_job_key = ARGV[1] .. existing_id\n\
            if redis.call('EXISTS', existing_job_key) == 1 then\n\
                local status = redis.call('HGET', existing_job_key, 'status')\n\
                if status == 'PENDING' then\n\
                    redis.call('HSET', existing_job_key,\n\
                        'function_name', ARGV[3],\n\
                        'job_params', ARGV[4],\n\
                        'next_scheduled_run_time', ARGV[8],\n\
                        'max_retries', ARGV[9],\n\
                        'job_timeout_seconds', ARGV[10],\n\
                        'result_ttl_seconds', ARGV[11],\n\
                        'queue_name', ARGV[12])\n\
                    if ARGV[14] ~= '' then\n\
                        redis.call('HSET', existing_job_key, 'trace_context', ARGV[14])\n\
                    end\n\
                    if ARGV[15] ~= '' then\n\
                        redis.call('HSET', existing_job_key, 'correlation_context', ARGV[15])\n\
                    else\n\
                        redis.call('HDEL', existing_job_key, 'correlation_context')\n\
                    end\n\
                    redis.call('ZADD', KEYS[1], ARGV[16], existing_id)\n\
                    local ttl = tonumber(ARGV[17])\n\
                    if ttl and ttl > 0 then\n\
                        redis.call('EXPIRE', KEYS[2], ttl)\n\
                    end\n\
                    return {0, existing_id}\n\
                end\n\
            end\n\
            redis.call('DEL', KEYS[2])\n\
        end\n\
        local job_key = ARGV[1] .. ARGV[2]\n\
        if redis.call('EXISTS', job_key) == 1 then\n\
            return {-1, ARGV[2]}\n\
        end\n\
        local ttl = tonumber(ARGV[17])\n\
        if ttl and ttl > 0 then\n\
            local ok = redis.call('SET', KEYS[2], ARGV[2], 'NX', 'EX', ttl)\n\
            if not ok then\n\
                local other = redis.call('GET', KEYS[2])\n\
                if other then\n\
                    return {0, other}\n\
                end\n\
                return {2, ''}\n\
            end\n\
        else\n\
            redis.call('SET', KEYS[2], ARGV[2], 'NX')\n\
        end\n\
        redis.call('HSET', job_key,\n\
            'id', ARGV[2],\n\
            'function_name', ARGV[3],\n\
            'job_params', ARGV[4],\n\
            'enqueue_time', ARGV[5],\n\
            'status', ARGV[6],\n\
            'current_retries', ARGV[7],\n\
            'next_scheduled_run_time', ARGV[8],\n\
            'max_retries', ARGV[9],\n\
            'job_timeout_seconds', ARGV[10],\n\
            'result_ttl_seconds', ARGV[11],\n\
            'queue_name', ARGV[12],\n\
        'result', ARGV[13])\n\
        if ARGV[14] ~= '' then\n\
            redis.call('HSET', job_key, 'trace_context', ARGV[14])\n\
        end\n\
        if ARGV[15] ~= '' then\n\
            redis.call('HSET', job_key, 'correlation_context', ARGV[15])\n\
        end\n\
        redis.call('ZADD', KEYS[1], ARGV[16], ARGV[2])\n\
        return {1, ARGV[2]}";
    Script::new(script)
}

fn parse_result(result: &str) -> Option<Value> {
    if result.is_empty() || result == "null" {
        return None;
    }
    serde_json::from_str(result).ok()
}

#[cfg(test)]
#[path = "lib/tests.rs"]
mod tests;
