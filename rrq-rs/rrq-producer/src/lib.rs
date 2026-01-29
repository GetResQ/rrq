use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Once;
use uuid::Uuid;

const JOB_KEY_PREFIX: &str = "rrq:job:";
const QUEUE_KEY_PREFIX: &str = "rrq:queue:";
const DEFAULT_QUEUE_NAME: &str = "rrq:queue:default";

static CRYPTO_PROVIDER_INIT: Once = Once::new();

/// Ensures the rustls crypto provider is installed for TLS connections (rediss://).
/// This is idempotent and safe to call multiple times.
fn ensure_crypto_provider() {
    CRYPTO_PROVIDER_INIT.call_once(|| {
        // Ignore errors - if already installed by the application, that's fine
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

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

pub struct Producer {
    conn: redis::aio::MultiplexedConnection,
}

impl Producer {
    pub async fn new(redis_dsn: impl AsRef<str>) -> Result<Self> {
        // Ensure TLS crypto provider is available for rediss:// connections
        ensure_crypto_provider();

        let client = redis::Client::open(redis_dsn.as_ref())
            .with_context(|| "failed to create Redis client")?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| "failed to connect to Redis")?;
        Ok(Self { conn })
    }

    pub fn with_connection(conn: redis::aio::MultiplexedConnection) -> Self {
        Self { conn }
    }

    pub async fn enqueue(
        &mut self,
        function_name: &str,
        args: Vec<Value>,
        kwargs: Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<String> {
        let queue_name = options
            .queue_name
            .unwrap_or_else(|| DEFAULT_QUEUE_NAME.to_string());
        let job_id = options.job_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let enqueue_time = options.enqueue_time.unwrap_or_else(Utc::now);
        let scheduled_time = options.scheduled_time.unwrap_or(enqueue_time);

        let job_key = format!("{JOB_KEY_PREFIX}{job_id}");
        let queue_key = format_queue_key(&queue_name);

        let job_args_json = serde_json::to_string(&args)?;
        let job_kwargs_json = serde_json::to_string(&kwargs)?;
        let result_json = "null".to_string();

        let mut mapping: Vec<(String, String)> = vec![
            ("id".to_string(), job_id.clone()),
            ("function_name".to_string(), function_name.to_string()),
            ("job_args".to_string(), job_args_json),
            ("job_kwargs".to_string(), job_kwargs_json),
            ("enqueue_time".to_string(), enqueue_time.to_rfc3339()),
            ("status".to_string(), "PENDING".to_string()),
            ("current_retries".to_string(), "0".to_string()),
            ("queue_name".to_string(), queue_name.clone()),
            (
                "next_scheduled_run_time".to_string(),
                scheduled_time.to_rfc3339(),
            ),
            ("result".to_string(), result_json),
        ];

        if let Some(max_retries) = options.max_retries {
            mapping.push(("max_retries".to_string(), max_retries.to_string()));
        }
        if let Some(job_timeout_seconds) = options.job_timeout_seconds {
            mapping.push((
                "job_timeout_seconds".to_string(),
                job_timeout_seconds.to_string(),
            ));
        }
        if let Some(result_ttl_seconds) = options.result_ttl_seconds {
            mapping.push((
                "result_ttl_seconds".to_string(),
                result_ttl_seconds.to_string(),
            ));
        }
        if let Some(trace_context) = options.trace_context {
            let trace_json = serde_json::to_string(&trace_context)?;
            mapping.push(("trace_context".to_string(), trace_json));
        }

        let mapping_ref: Vec<(&str, &str)> = mapping
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();

        self.conn
            .hset_multiple::<_, _, _, ()>(&job_key, &mapping_ref)
            .await?;
        self.conn
            .zadd::<_, _, _, ()>(&queue_key, &job_id, scheduled_time.timestamp_millis())
            .await?;

        Ok(job_id)
    }
}

fn format_queue_key(queue_name: &str) -> String {
    if queue_name.starts_with(QUEUE_KEY_PREFIX) {
        queue_name.to_string()
    } else {
        format!("{QUEUE_KEY_PREFIX}{queue_name}")
    }
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

    #[tokio::test]
    async fn producer_enqueue_writes_job_and_queue() {
        let _guard = redis_lock().lock().await;
        let dsn = std::env::var("RRQ_TEST_REDIS_DSN")
            .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
        let client = redis::Client::open(dsn.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

        let mut producer = Producer::new(dsn).await.unwrap();
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

    #[test]
    fn ensure_crypto_provider_is_idempotent() {
        // Should not panic when called multiple times
        ensure_crypto_provider();
        ensure_crypto_provider();
        ensure_crypto_provider();
    }

    #[tokio::test]
    async fn producer_connects_with_tls_when_configured() {
        // Skip if no TLS Redis DSN is configured
        let Ok(dsn) = std::env::var("RRQ_TEST_REDIS_TLS_DSN") else {
            eprintln!("Skipping TLS test: RRQ_TEST_REDIS_TLS_DSN not set");
            return;
        };

        // This should succeed if TLS is properly configured
        let result = Producer::new(&dsn).await;
        assert!(
            result.is_ok(),
            "Failed to connect to Redis with TLS: {:?}",
            result.err()
        );
    }
}
