use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rrq_config::normalize_queue_name;
use serde_json::Value;
use std::time::Instant;

use crate::job::{Job, JobStatus};
use crate::store::JobStore;
use crate::telemetry;
use rrq_config::RRQSettings;

#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    pub queue_name: Option<String>,
    pub job_id: Option<String>,
    pub unique_key: Option<String>,
    pub max_retries: Option<i64>,
    pub job_timeout_seconds: Option<i64>,
    pub defer_until: Option<DateTime<Utc>>,
    pub defer_by: Option<Duration>,
    pub result_ttl_seconds: Option<i64>,
    pub trace_context: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone)]
pub struct RRQClient {
    settings: RRQSettings,
    job_store: JobStore,
}

impl RRQClient {
    pub fn new(settings: RRQSettings, job_store: JobStore) -> Self {
        Self {
            settings,
            job_store,
        }
    }

    pub async fn enqueue(
        &mut self,
        function_name: &str,
        params: serde_json::Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<Job> {
        let enqueue_started = Instant::now();
        let job_id = options.job_id.unwrap_or_else(Job::new_id);
        let queue_name = normalize_queue_name(
            &options
                .queue_name
                .unwrap_or_else(|| self.settings.default_queue_name.clone()),
        );
        let record_enqueue_result = |result: &str| {
            telemetry::record_enqueue(
                &queue_name,
                result,
                enqueue_started.elapsed().as_secs_f64() * 1000.0,
            );
        };

        let span = tracing::info_span!(
            "rrq.enqueue",
            job_id = %job_id,
            function_name = %function_name,
            queue_name = %queue_name
        );
        if let Some(provided_trace_context) = options.trace_context.as_ref() {
            telemetry::set_parent_from_trace_context(&span, provided_trace_context);
        }
        let _enter = span.enter();
        let trace_context = telemetry::inject_current_trace_context(options.trace_context.clone());
        let correlation_context = telemetry::extract_correlation_context(
            &params,
            &self.settings.correlation_mappings,
            trace_context.as_ref(),
        );
        if let Some(correlation_context) = correlation_context.as_ref() {
            telemetry::apply_correlation_context_to_span(&span, correlation_context);
        }

        let job_timeout_seconds = options
            .job_timeout_seconds
            .unwrap_or(self.settings.default_job_timeout_seconds);
        if job_timeout_seconds <= 0 {
            record_enqueue_result("invalid_timeout");
            return Err(anyhow::anyhow!("job_timeout_seconds must be positive"));
        }
        let lock_ttl_floor = match job_timeout_seconds
            .checked_add(self.settings.default_lock_timeout_extension_seconds)
            .and_then(|value| value.checked_add(1))
        {
            Some(lock_ttl_floor) => lock_ttl_floor,
            None => {
                record_enqueue_result("lock_ttl_overflow");
                return Err(anyhow::anyhow!("lock_ttl_seconds overflow"));
            }
        };
        if lock_ttl_floor <= 0 {
            record_enqueue_result("invalid_lock_ttl");
            return Err(anyhow::anyhow!("lock_ttl_seconds must be positive"));
        }

        let enqueue_time = Utc::now();
        let mut desired_run_time = enqueue_time;
        let mut lock_ttl_seconds = self
            .settings
            .default_unique_job_lock_ttl_seconds
            .max(lock_ttl_floor);

        if let Some(defer_until) = options.defer_until {
            desired_run_time = defer_until;
            let diff = defer_until
                .signed_duration_since(enqueue_time)
                .num_seconds();
            if diff > 0 {
                lock_ttl_seconds = lock_ttl_seconds.max(diff + 1);
            }
        } else if let Some(defer_by) = options.defer_by {
            let defer_secs = defer_by.num_seconds().max(0);
            desired_run_time = enqueue_time + defer_by;
            lock_ttl_seconds = lock_ttl_seconds.max(defer_secs + 1);
        }

        let mut unique_acquired = false;
        if let Some(unique_key) = options.unique_key.as_deref() {
            let remaining_ttl = self.job_store.get_lock_ttl(unique_key).await?;
            if remaining_ttl > 0 {
                desired_run_time =
                    desired_run_time.max(enqueue_time + Duration::seconds(remaining_ttl));
            } else {
                let acquired = self
                    .job_store
                    .acquire_unique_job_lock(unique_key, &job_id, lock_ttl_seconds)
                    .await?;
                if acquired {
                    unique_acquired = true;
                } else {
                    let remaining = self.job_store.get_lock_ttl(unique_key).await?;
                    if remaining > 0 {
                        desired_run_time =
                            desired_run_time.max(enqueue_time + Duration::seconds(remaining));
                    }
                }
            }
        }

        let job = Job {
            id: job_id.clone(),
            function_name: function_name.to_string(),
            job_params: params,
            enqueue_time,
            start_time: None,
            status: JobStatus::Pending,
            current_retries: 0,
            next_scheduled_run_time: Some(desired_run_time),
            max_retries: options
                .max_retries
                .unwrap_or(self.settings.default_max_retries),
            job_timeout_seconds: Some(job_timeout_seconds),
            result_ttl_seconds: Some(
                options
                    .result_ttl_seconds
                    .unwrap_or(self.settings.default_result_ttl_seconds),
            ),
            job_unique_key: options.unique_key.clone(),
            completion_time: None,
            result: None,
            last_error: None,
            queue_name: Some(queue_name.clone()),
            dlq_name: None,
            worker_id: None,
            trace_context,
            correlation_context,
        };

        let score_ms = desired_run_time.timestamp_millis() as f64;
        match self
            .job_store
            .atomic_enqueue_job(&job, &queue_name, score_ms)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                if unique_acquired && let Some(unique_key) = options.unique_key.as_deref() {
                    let _ = self.job_store.release_unique_job_lock(unique_key).await;
                }
                record_enqueue_result("duplicate_job_id");
                anyhow::bail!("job_id already exists");
            }
            Err(err) => {
                if unique_acquired && let Some(unique_key) = options.unique_key.as_deref() {
                    let _ = self.job_store.release_unique_job_lock(unique_key).await;
                }
                record_enqueue_result("error");
                return Err(err);
            }
        }

        record_enqueue_result("enqueued");
        tracing::info!("job enqueued");
        Ok(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::UNIQUE_JOB_LOCK_PREFIX;
    use crate::test_support::RedisTestContext;
    use chrono::Duration as ChronoDuration;
    use serde_json::json;
    use std::collections::HashMap;

    #[tokio::test]
    async fn enqueue_with_defer_by_and_unique_lock() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let options = EnqueueOptions {
            unique_key: Some("unique-key".to_string()),
            defer_by: Some(ChronoDuration::seconds(5)),
            job_timeout_seconds: Some(5),
            result_ttl_seconds: Some(10),
            ..Default::default()
        };
        let job = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap();
        let scheduled = job.next_scheduled_run_time.unwrap();
        let delta = scheduled
            .signed_duration_since(job.enqueue_time)
            .num_seconds();
        assert!(delta >= 4);
        assert_eq!(job.job_unique_key.as_deref(), Some("unique-key"));
        let ttl = ctx.store.get_lock_ttl("unique-key").await.unwrap();
        assert!(ttl > 0);
    }

    #[tokio::test]
    async fn enqueue_respects_defer_until() {
        let ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let defer_until = Utc::now() + ChronoDuration::seconds(8);
        let options = EnqueueOptions {
            defer_until: Some(defer_until),
            ..Default::default()
        };
        let job = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap();
        let scheduled = job.next_scheduled_run_time.unwrap();
        assert!(scheduled >= defer_until);
    }

    #[tokio::test]
    async fn enqueue_respects_existing_unique_lock() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let acquired = ctx
            .store
            .acquire_unique_job_lock("held", "other", 30)
            .await
            .unwrap();
        assert!(acquired);
        let options = EnqueueOptions {
            unique_key: Some("held".to_string()),
            ..Default::default()
        };
        let job = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap();
        let scheduled = job.next_scheduled_run_time.unwrap();
        let delta = scheduled
            .signed_duration_since(job.enqueue_time)
            .num_seconds();
        assert!(delta >= 25);
    }

    #[tokio::test]
    async fn enqueue_handles_lock_without_ttl() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let lock_key = format!("{UNIQUE_JOB_LOCK_PREFIX}no-ttl");
        let redis = redis::Client::open(ctx.settings.redis_dsn.as_str()).unwrap();
        let mut conn = redis.get_multiplexed_async_connection().await.unwrap();
        let _: () = redis::cmd("SET")
            .arg(&lock_key)
            .arg("locked")
            .query_async(&mut conn)
            .await
            .unwrap();
        let options = EnqueueOptions {
            unique_key: Some("no-ttl".to_string()),
            ..Default::default()
        };
        let job = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap();
        let scheduled = job.next_scheduled_run_time.unwrap();
        let delta = scheduled
            .signed_duration_since(job.enqueue_time)
            .num_seconds();
        assert!(delta <= 1);
        let ttl = ctx.store.get_lock_ttl("no-ttl").await.unwrap();
        assert_eq!(ttl, 0);
    }

    #[tokio::test]
    async fn enqueue_rejects_non_positive_timeout() {
        let ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let options = EnqueueOptions {
            job_timeout_seconds: Some(0),
            ..Default::default()
        };
        let err = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("job_timeout_seconds must be positive")
        );
    }

    #[tokio::test]
    async fn enqueue_releases_unique_lock_on_invalid_timeout() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let options = EnqueueOptions {
            unique_key: Some("invalid-timeout-lock".to_string()),
            job_timeout_seconds: Some(0),
            ..Default::default()
        };
        let err = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("job_timeout_seconds must be positive")
        );
        let ttl = ctx
            .store
            .get_lock_ttl("invalid-timeout-lock")
            .await
            .unwrap();
        assert_eq!(ttl, 0);
    }

    #[tokio::test]
    async fn enqueue_rejects_duplicate_job_id_preserves_existing_job() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let options = EnqueueOptions {
            job_id: Some("fixed-id".to_string()),
            ..Default::default()
        };
        let first = client
            .enqueue("task", serde_json::Map::new(), options.clone())
            .await
            .unwrap();
        assert_eq!(first.id, "fixed-id");

        let err = client
            .enqueue("task", serde_json::Map::new(), options)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("job_id already exists"));

        let stored = ctx
            .store
            .get_job_definition("fixed-id")
            .await
            .unwrap()
            .unwrap();
        assert!(stored.job_params.is_empty());
        let queue_size = ctx
            .store
            .queue_size(&ctx.settings.default_queue_name)
            .await
            .unwrap();
        assert_eq!(queue_size, 1);
    }

    #[tokio::test]
    async fn enqueue_normalizes_bare_queue_name_in_job_metadata() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let queue_name = "bare-queue";
        let job = client
            .enqueue(
                "task",
                serde_json::Map::new(),
                EnqueueOptions {
                    queue_name: Some(queue_name.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(job.queue_name.as_deref(), Some("rrq:queue:bare-queue"));
        let stored = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.queue_name.as_deref(), Some("rrq:queue:bare-queue"));
    }

    #[tokio::test]
    async fn enqueue_preserves_prefixed_queue_name_in_job_metadata() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let queue_name = "rrq:queue:already-normalized";
        let job = client
            .enqueue(
                "task",
                serde_json::Map::new(),
                EnqueueOptions {
                    queue_name: Some(queue_name.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(job.queue_name.as_deref(), Some(queue_name));
        let stored = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.queue_name.as_deref(), Some(queue_name));
    }

    #[tokio::test]
    async fn enqueue_extracts_and_persists_correlation_context_from_mappings() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        ctx.settings.correlation_mappings = HashMap::from([
            ("session_id".to_string(), "session.id".to_string()),
            ("message_id".to_string(), "params.message_id".to_string()),
        ]);
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let params = json!({
            "session": { "id": "sess-99" },
            "message_id": "msg-123"
        })
        .as_object()
        .expect("params object")
        .clone();

        let job = client
            .enqueue("task", params, EnqueueOptions::default())
            .await
            .unwrap();
        let correlation = job
            .correlation_context
            .as_ref()
            .expect("correlation context should be set");
        assert_eq!(
            correlation.get("session_id").map(String::as_str),
            Some("sess-99")
        );
        assert_eq!(
            correlation.get("message_id").map(String::as_str),
            Some("msg-123")
        );

        let stored = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        let stored_correlation = stored
            .correlation_context
            .as_ref()
            .expect("stored correlation context should be set");
        assert_eq!(
            stored_correlation.get("session_id").map(String::as_str),
            Some("sess-99")
        );
        assert_eq!(
            stored_correlation.get("message_id").map(String::as_str),
            Some("msg-123")
        );
    }

    #[tokio::test]
    async fn enqueue_prefers_trace_context_values_for_correlation() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        ctx.settings.correlation_mappings =
            HashMap::from([("session_id".to_string(), "session.id".to_string())]);
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let params = json!({
            "session": { "id": "sess-from-params" }
        })
        .as_object()
        .expect("params object")
        .clone();
        let trace_context =
            HashMap::from([("session_id".to_string(), "sess-from-trace".to_string())]);

        let job = client
            .enqueue(
                "task",
                params,
                EnqueueOptions {
                    trace_context: Some(trace_context),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let correlation = job
            .correlation_context
            .as_ref()
            .expect("correlation context should be set");
        assert_eq!(
            correlation.get("session_id").map(String::as_str),
            Some("sess-from-trace")
        );
    }
}
