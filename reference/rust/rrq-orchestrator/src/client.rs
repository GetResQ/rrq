use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;

use crate::job::{Job, JobStatus};
use crate::settings::RRQSettings;
use crate::store::JobStore;

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
        args: Vec<Value>,
        kwargs: serde_json::Map<String, Value>,
        options: EnqueueOptions,
    ) -> Result<Job> {
        let job_id = options.job_id.unwrap_or_else(Job::new_id);
        let queue_name = options
            .queue_name
            .unwrap_or_else(|| self.settings.default_queue_name.clone());

        let span = tracing::info_span!(
            "rrq.enqueue",
            job_id = %job_id,
            function_name = %function_name,
            queue_name = %queue_name
        );
        let _enter = span.enter();

        let enqueue_time = Utc::now();
        let mut desired_run_time = enqueue_time;
        let mut lock_ttl_seconds = self.settings.default_unique_job_lock_ttl_seconds;

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
            job_args: args,
            job_kwargs: kwargs,
            enqueue_time,
            start_time: None,
            status: JobStatus::Pending,
            current_retries: 0,
            next_scheduled_run_time: Some(desired_run_time),
            max_retries: options
                .max_retries
                .unwrap_or(self.settings.default_max_retries),
            job_timeout_seconds: Some(
                options
                    .job_timeout_seconds
                    .unwrap_or(self.settings.default_job_timeout_seconds),
            ),
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
            trace_context: options.trace_context,
        };

        let score_ms = desired_run_time.timestamp_millis() as f64;
        if let Err(err) = self.job_store.save_job_definition(&job).await {
            if unique_acquired {
                if let Some(unique_key) = options.unique_key.as_deref() {
                    let _ = self.job_store.release_unique_job_lock(unique_key).await;
                }
            }
            return Err(err);
        }
        if let Err(err) = self
            .job_store
            .add_job_to_queue(&queue_name, &job.id, score_ms)
            .await
        {
            if unique_acquired {
                if let Some(unique_key) = options.unique_key.as_deref() {
                    let _ = self.job_store.release_unique_job_lock(unique_key).await;
                }
            }
            return Err(err);
        }

        tracing::info!("job enqueued");
        Ok(job)
    }
}
