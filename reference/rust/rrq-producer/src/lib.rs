use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

const JOB_KEY_PREFIX: &str = "rrq:job:";
const QUEUE_KEY_PREFIX: &str = "rrq:queue:";

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
            .unwrap_or_else(|| "default".to_string());
        let job_id = options
            .job_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
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

    #[test]
    fn format_queue_key_adds_prefix() {
        assert_eq!(format_queue_key("default"), "rrq:queue:default");
    }

    #[test]
    fn format_queue_key_preserves_prefix() {
        assert_eq!(format_queue_key("rrq:queue:custom"), "rrq:queue:custom");
    }
}
