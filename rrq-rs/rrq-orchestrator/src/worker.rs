use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::Result;
use chrono::{TimeZone, Utc};
use rand::Rng;
use rrq_protocol::{ExecutionContext, ExecutionOutcome, ExecutionRequest, OutcomeStatus};
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{Duration, sleep, timeout};
use uuid::Uuid;

use crate::client::{EnqueueOptions, RRQClient};
use crate::constants::DEFAULT_WORKER_ID_PREFIX;
use crate::cron::CronJob;
use crate::executor::Executor;
use crate::job::{Job, JobStatus};
use crate::settings::RRQSettings;
use crate::store::JobStore;

#[derive(Debug, Clone)]
struct RunningJobInfo {
    queue_name: String,
    executor_name: Option<String>,
    request_id: Option<String>,
}

pub struct RRQWorker {
    settings: RRQSettings,
    queues: Vec<String>,
    worker_id: String,
    job_store: JobStore,
    client: RRQClient,
    executors: HashMap<String, Arc<dyn Executor>>,
    default_executor_name: String,
    executor_routes: HashMap<String, String>,
    semaphore: Arc<Semaphore>,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    running_aborts: Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>,
    status: Arc<Mutex<String>>,
    cron_jobs: Arc<Mutex<Vec<CronJob>>>,
    burst: bool,
    shutdown: Arc<AtomicBool>,
}

impl RRQWorker {
    pub async fn new(
        settings: RRQSettings,
        queues: Option<Vec<String>>,
        worker_id: Option<String>,
        executors: HashMap<String, Arc<dyn Executor>>,
        burst: bool,
    ) -> Result<Self> {
        if executors.is_empty() {
            return Err(anyhow::anyhow!("RRQWorker requires at least one executor"));
        }
        let default_executor_name = settings.default_executor_name.clone();
        if !executors.contains_key(&default_executor_name) {
            return Err(anyhow::anyhow!(
                "default executor '{}' is not configured",
                default_executor_name
            ));
        }
        let worker_concurrency = settings.worker_concurrency;
        let executor_routes = settings.executor_routes.clone();
        let job_store = JobStore::new(settings.clone()).await?;
        let client = RRQClient::new(settings.clone(), job_store.clone());
        let queues = queues.unwrap_or_else(|| vec![settings.default_queue_name.clone()]);
        if queues.is_empty() {
            return Err(anyhow::anyhow!(
                "worker must be configured with at least one queue"
            ));
        }
        let worker_id = worker_id.unwrap_or_else(|| {
            let short_id = Uuid::new_v4().to_string();
            let suffix = &short_id[..6];
            format!(
                "{DEFAULT_WORKER_ID_PREFIX}{}_{}",
                std::process::id(),
                suffix
            )
        });
        Ok(Self {
            settings,
            queues,
            worker_id,
            job_store,
            client,
            executors,
            default_executor_name,
            executor_routes,
            semaphore: Arc::new(Semaphore::new(worker_concurrency)),
            running_jobs: Arc::new(Mutex::new(HashMap::new())),
            running_aborts: Arc::new(Mutex::new(HashMap::new())),
            status: Arc::new(Mutex::new("initializing".to_string())),
            cron_jobs: Arc::new(Mutex::new(Vec::new())),
            burst,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn settings(&self) -> &RRQSettings {
        &self.settings
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    pub async fn close_executors(&self) {
        for executor in self.executors.values() {
            if let Err(err) = executor.close().await {
                tracing::debug!("executor close error: {err}");
            }
        }
    }

    async fn set_status(&self, value: &str) {
        let mut status = self.status.lock().await;
        *status = value.to_string();
    }

    fn calculate_jittered_delay(&self, base_delay: f64, jitter_factor: f64) -> Duration {
        let jitter = jitter_factor.clamp(0.0, 0.99);
        let min_delay = (base_delay * (1.0 - jitter)).max(0.001);
        let max_delay = base_delay * (1.0 + jitter);
        let mut rng = rand::rng();
        let delay = rng.random_range(min_delay..=max_delay);
        Duration::from_secs_f64(delay)
    }

    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(worker_id = %self.worker_id, "worker started");
        self.set_status("running").await;
        {
            let mut cron_jobs = self.cron_jobs.lock().await;
            cron_jobs.extend(self.settings.cron_jobs.clone());
            let now = Utc::now();
            for job in cron_jobs.iter_mut() {
                let _ = job.schedule_next(now);
            }
        }

        let heartbeat_handle = {
            let shutdown = self.shutdown.clone();
            let job_store = self.job_store.clone();
            let worker_id = self.worker_id.clone();
            let queues = self.queues.clone();
            let status = self.status.clone();
            let running_jobs = self.running_jobs.clone();
            let settings = self.settings.clone();
            tokio::spawn(async move {
                heartbeat_loop(
                    shutdown,
                    job_store,
                    worker_id,
                    queues,
                    status,
                    running_jobs,
                    settings,
                )
                .await;
            })
        };

        let cron_handle = {
            let shutdown = self.shutdown.clone();
            let job_store = self.job_store.clone();
            let client = self.client.clone();
            let cron_jobs = self.cron_jobs.clone();
            tokio::spawn(async move {
                cron_loop(shutdown, cron_jobs, client, job_store).await;
            })
        };

        while !self.shutdown.load(Ordering::SeqCst) {
            let running = self
                .settings
                .worker_concurrency
                .saturating_sub(self.semaphore.available_permits());
            let fetch_count = self.settings.worker_concurrency.saturating_sub(running);
            if fetch_count == 0 {
                self.set_status("idle (concurrency limit)").await;
                let delay =
                    self.calculate_jittered_delay(self.settings.default_poll_delay_seconds, 0.5);
                sleep_with_shutdown(&self.shutdown, delay).await;
                continue;
            }

            self.set_status("polling").await;
            let fetched = self.poll_for_jobs(fetch_count).await?;
            if fetched == 0 {
                let running_jobs = self.running_jobs.lock().await.len();
                if self.burst && running_jobs == 0 {
                    break;
                }
                self.set_status("idle (no jobs)").await;
                let delay =
                    self.calculate_jittered_delay(self.settings.default_poll_delay_seconds, 0.5);
                sleep_with_shutdown(&self.shutdown, delay).await;
            }
        }

        self.shutdown.store(true, Ordering::SeqCst);
        self.drain_tasks().await?;
        let _ = heartbeat_handle.await;
        let _ = cron_handle.await;
        tracing::info!(worker_id = %self.worker_id, "worker stopped");
        Ok(())
    }

    async fn poll_for_jobs(&mut self, count: usize) -> Result<usize> {
        let mut fetched = 0;
        for queue_name in &self.queues {
            if fetched >= count || self.shutdown.load(Ordering::SeqCst) {
                break;
            }
            let ready = self
                .job_store
                .get_ready_job_ids(queue_name, count - fetched)
                .await?;
            if ready.is_empty() {
                continue;
            }

            for job_id in ready {
                if fetched >= count || self.shutdown.load(Ordering::SeqCst) {
                    break;
                }
                let job_opt = self.job_store.get_job_definition(&job_id).await?;
                let job = match job_opt {
                    Some(job) => job,
                    None => {
                        let _ = self
                            .job_store
                            .remove_job_from_queue(queue_name, &job_id)
                            .await;
                        continue;
                    }
                };

                let job_timeout = job
                    .job_timeout_seconds
                    .unwrap_or(self.settings.default_job_timeout_seconds);
                let lock_timeout_ms = job_timeout
                    .checked_add(self.settings.default_lock_timeout_extension_seconds)
                    .and_then(|sum| sum.checked_mul(1000))
                    .ok_or_else(|| anyhow::anyhow!("lock_timeout_ms overflow"))?;
                if lock_timeout_ms <= 0 {
                    return Err(anyhow::anyhow!("lock_timeout_ms must be positive"));
                }

                let start_time = Utc::now();
                let (locked, removed) = self
                    .job_store
                    .atomic_lock_and_start_job(
                        &job.id,
                        queue_name,
                        &self.worker_id,
                        lock_timeout_ms,
                        start_time,
                    )
                    .await?;
                if !locked || removed == 0 {
                    continue;
                }

                let permit = self.semaphore.clone().acquire_owned().await?;
                let job_store = self.job_store.clone();
                let executors = self.executors.clone();
                let executor_routes = self.executor_routes.clone();
                let default_executor_name = self.default_executor_name.clone();
                let settings = self.settings.clone();
                let worker_id = self.worker_id.clone();
                let running_jobs = self.running_jobs.clone();
                let running_aborts = self.running_aborts.clone();
                let queue_name = queue_name.clone();
                let mut job_for_task = job.clone();
                job_for_task.start_time = Some(start_time);
                {
                    let mut running = running_jobs.lock().await;
                    running.insert(
                        job.id.clone(),
                        RunningJobInfo {
                            queue_name: queue_name.clone(),
                            executor_name: None,
                            request_id: None,
                        },
                    );
                }

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    let context = ExecuteJobContext {
                        settings,
                        job_store,
                        executors,
                        default_executor_name,
                        executor_routes,
                        worker_id,
                        running_jobs,
                        running_aborts,
                    };
                    if let Err(err) = execute_job(job_for_task, queue_name, context).await {
                        tracing::error!("job execution error: {err}");
                    }
                });
                {
                    let mut aborts = self.running_aborts.lock().await;
                    aborts.insert(job.id.clone(), handle.abort_handle());
                }

                fetched += 1;
            }
        }
        Ok(fetched)
    }

    async fn drain_tasks(&self) -> Result<()> {
        let grace = Duration::from_secs_f64(self.settings.worker_shutdown_grace_period_seconds);
        let deadline = tokio::time::Instant::now() + grace;

        loop {
            let remaining = {
                let running = self.running_jobs.lock().await;
                running.len()
            };
            if remaining == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let running = self.running_jobs.lock().await.clone();
        let aborts = self.running_aborts.lock().await.clone();
        for (job_id, info) in &running {
            let resolved_executor = info.executor_name.clone().unwrap_or_else(|| {
                self.executor_routes
                    .get(&info.queue_name)
                    .cloned()
                    .unwrap_or_else(|| self.default_executor_name.clone())
            });
            if let Some(executor) = self.executors.get(&resolved_executor) {
                let _ = executor.cancel(job_id, info.request_id.as_deref()).await;
            }
        }
        for (_job_id, abort) in aborts {
            abort.abort();
        }
        for (job_id, info) in running {
            tracing::warn!("re-queueing job {} after shutdown", job_id);
            let mut store = self.job_store.clone();
            let _ = store
                .mark_job_pending(
                    &job_id,
                    Some("Job execution interrupted by worker shutdown. Re-queued."),
                )
                .await;
            let _ = store
                .add_job_to_queue(
                    &info.queue_name,
                    &job_id,
                    Utc::now().timestamp_millis() as f64,
                )
                .await;
            let _ = store.release_job_lock(&job_id).await;
        }

        Ok(())
    }
}

fn split_executor_name(function_name: &str) -> (Option<String>, String) {
    if let Some((prefix, handler)) = function_name.split_once('#') {
        if handler.is_empty() {
            return (Some(prefix.to_string()), String::new());
        }
        let executor = if prefix.is_empty() {
            None
        } else {
            Some(prefix.to_string())
        };
        return (executor, handler.to_string());
    }
    (None, function_name.to_string())
}

struct ExecuteJobContext {
    settings: RRQSettings,
    job_store: JobStore,
    executors: HashMap<String, Arc<dyn Executor>>,
    default_executor_name: String,
    executor_routes: HashMap<String, String>,
    worker_id: String,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    running_aborts: Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>,
}

async fn execute_job(job: Job, queue_name: String, context: ExecuteJobContext) -> Result<()> {
    let ExecuteJobContext {
        settings,
        mut job_store,
        executors,
        default_executor_name,
        executor_routes,
        worker_id,
        running_jobs,
        running_aborts,
    } = context;
    let span = tracing::info_span!(
        "rrq.job",
        job_id = %job.id,
        function_name = %job.function_name,
        queue_name = %queue_name,
        attempt = job.current_retries + 1
    );
    let _enter = span.enter();
    let job_timeout = job
        .job_timeout_seconds
        .unwrap_or(settings.default_job_timeout_seconds);
    let attempt = job.current_retries + 1;
    let deadline = Utc::now() + chrono::Duration::seconds(job_timeout);

    let (executor_name, handler_name) = split_executor_name(&job.function_name);
    if handler_name.is_empty() {
        handle_fatal_job_error(&job, &queue_name, "Handler name is missing", &mut job_store)
            .await?;
        cleanup_running(
            &job.id,
            &mut job_store,
            &worker_id,
            running_jobs,
            running_aborts,
        )
        .await?;
        return Ok(());
    }

    let resolved_executor = match executor_name {
        Some(name) => name,
        None => executor_routes
            .get(&queue_name)
            .cloned()
            .unwrap_or(default_executor_name.clone()),
    };
    {
        let mut running = running_jobs.lock().await;
        if let Some(info) = running.get_mut(&job.id) {
            info.executor_name = Some(resolved_executor.clone());
        }
    }

    let executor = executors.get(&resolved_executor).cloned();
    let executor = match executor {
        Some(exec) => exec,
        None => {
            let message = format!("No executor configured for '{resolved_executor}'.");
            handle_fatal_job_error(&job, &queue_name, &message, &mut job_store).await?;
            cleanup_running(
                &job.id,
                &mut job_store,
                &worker_id,
                running_jobs,
                running_aborts,
            )
            .await?;
            return Ok(());
        }
    };

    let kwargs = job
        .job_kwargs
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let request = ExecutionRequest {
        protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
        request_id: Uuid::new_v4().to_string(),
        job_id: job.id.clone(),
        function_name: handler_name,
        args: job.job_args.clone(),
        kwargs,
        context: ExecutionContext {
            job_id: job.id.clone(),
            attempt: attempt as u32,
            enqueue_time: job.enqueue_time,
            queue_name: queue_name.clone(),
            deadline: Some(deadline),
            trace_context: job.trace_context.clone(),
            worker_id: Some(worker_id.clone()),
        },
    };
    let request_id = request.request_id.clone();
    {
        let mut running = running_jobs.lock().await;
        if let Some(info) = running.get_mut(&job.id) {
            info.request_id = Some(request_id.clone());
        }
    }

    let exec_result = timeout(
        Duration::from_secs(job_timeout as u64),
        executor.execute(request),
    )
    .await;

    let outcome_result = match exec_result {
        Ok(outcome_result) => {
            let outcome = outcome_result?;
            handle_execution_outcome(&job, &queue_name, &settings, &mut job_store, outcome).await
        }
        Err(_) => {
            let _ = executor.cancel(&job.id, Some(request_id.as_str())).await;
            let message = format!("Job timed out after {}s.", job_timeout);
            handle_job_timeout(&job, &queue_name, &mut job_store, &message).await
        }
    };

    let cleanup_result = cleanup_running(
        &job.id,
        &mut job_store,
        &worker_id,
        running_jobs,
        running_aborts,
    )
    .await;

    if let Err(err) = outcome_result {
        if let Err(cleanup_err) = cleanup_result {
            tracing::error!("cleanup failed after outcome error: {cleanup_err}");
        }
        return Err(err);
    }

    cleanup_result?;
    Ok(())
}

async fn cleanup_running(
    job_id: &str,
    job_store: &mut JobStore,
    worker_id: &str,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    running_aborts: Arc<Mutex<HashMap<String, tokio::task::AbortHandle>>>,
) -> Result<()> {
    job_store.remove_active_job(worker_id, job_id).await?;
    job_store.release_job_lock(job_id).await?;
    let mut running = running_jobs.lock().await;
    running.remove(job_id);
    let mut aborts = running_aborts.lock().await;
    aborts.remove(job_id);
    Ok(())
}

async fn handle_execution_outcome(
    job: &Job,
    queue_name: &str,
    settings: &RRQSettings,
    job_store: &mut JobStore,
    outcome: ExecutionOutcome,
) -> Result<()> {
    match outcome.status {
        OutcomeStatus::Success => {
            let result = outcome.result.unwrap_or(Value::Null);
            let ttl = job
                .result_ttl_seconds
                .unwrap_or(settings.default_result_ttl_seconds);
            job_store.save_job_result(&job.id, &result, ttl).await?;
            if let Some(unique_key) = job.job_unique_key.as_ref() {
                job_store.release_unique_job_lock(unique_key).await?;
            }
            tracing::info!("job completed");
        }
        OutcomeStatus::Retry => {
            let message = outcome
                .error
                .as_ref()
                .map(|error| error.message.clone())
                .unwrap_or_else(|| "Job requested retry".to_string());
            process_retry_job(
                job,
                queue_name,
                settings,
                job_store,
                &message,
                outcome.retry_after_seconds,
            )
            .await?;
            tracing::info!("job retry requested");
        }
        OutcomeStatus::Timeout => {
            let message = outcome
                .error
                .as_ref()
                .map(|error| error.message.clone())
                .unwrap_or_else(|| {
                    format!(
                        "Job timed out after {}s.",
                        job.job_timeout_seconds
                            .unwrap_or(settings.default_job_timeout_seconds)
                    )
                });
            handle_job_timeout(job, queue_name, job_store, &message).await?;
            tracing::info!("job timeout");
        }
        OutcomeStatus::Error => {
            let error_type = outcome
                .error
                .as_ref()
                .and_then(|error| error.error_type.as_deref());
            if error_type == Some("handler_not_found") {
                let message = outcome
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "Handler not found".to_string());
                handle_fatal_job_error(job, queue_name, &message, job_store).await?;
                tracing::info!("job fatal error");
            } else {
                let message = outcome
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "Job failed".to_string());
                process_failure_job(job, queue_name, settings, job_store, &message).await?;
                tracing::info!("job failed");
            }
        }
    }

    Ok(())
}

async fn handle_job_timeout(
    job: &Job,
    queue_name: &str,
    job_store: &mut JobStore,
    error_message: &str,
) -> Result<()> {
    job_store.increment_job_retries(&job.id).await?;
    move_to_dlq(job, queue_name, job_store, error_message).await?;
    Ok(())
}

async fn handle_fatal_job_error(
    job: &Job,
    queue_name: &str,
    error_message: &str,
    job_store: &mut JobStore,
) -> Result<()> {
    job_store.increment_job_retries(&job.id).await?;
    move_to_dlq(job, queue_name, job_store, error_message).await?;
    Ok(())
}

async fn move_to_dlq(
    job: &Job,
    _queue_name: &str,
    job_store: &mut JobStore,
    error_message: &str,
) -> Result<()> {
    let dlq_name = job
        .dlq_name
        .clone()
        .unwrap_or_else(|| job_store.settings().default_dlq_name.clone());
    job_store
        .move_job_to_dlq(&job.id, &dlq_name, error_message, Utc::now())
        .await?;
    if let Some(unique_key) = job.job_unique_key.as_ref() {
        job_store.release_unique_job_lock(unique_key).await?;
    }
    Ok(())
}

async fn process_retry_job(
    job: &Job,
    queue_name: &str,
    settings: &RRQSettings,
    job_store: &mut JobStore,
    error_message: &str,
    retry_after_seconds: Option<f64>,
) -> Result<()> {
    let anticipated_retry = job.current_retries + 1;
    if anticipated_retry >= job.max_retries {
        job_store.increment_job_retries(&job.id).await?;
        move_to_dlq(job, queue_name, job_store, error_message).await?;
        return Ok(());
    }

    let delay_seconds = match retry_after_seconds {
        Some(delay) => delay,
        None => calculate_backoff_seconds(settings, anticipated_retry),
    };

    let retry_at_score = (Utc::now().timestamp_millis() as f64) + delay_seconds * 1000.0;
    let target_queue = job
        .queue_name
        .as_deref()
        .unwrap_or(&settings.default_queue_name);
    let new_retry = job_store
        .atomic_retry_job(
            &job.id,
            target_queue,
            retry_at_score,
            error_message,
            JobStatus::Retrying,
        )
        .await?;

    let next_run_time = Utc
        .timestamp_millis_opt(retry_at_score as i64)
        .single()
        .unwrap_or_else(Utc::now);
    let _ = job_store
        .update_job_next_scheduled_run_time(&job.id, next_run_time)
        .await;

    tracing::info!(
        "retrying job {} attempt {}/{}",
        job.id,
        new_retry,
        job.max_retries
    );
    Ok(())
}

async fn process_failure_job(
    job: &Job,
    queue_name: &str,
    settings: &RRQSettings,
    job_store: &mut JobStore,
    error_message: &str,
) -> Result<()> {
    let anticipated_retry = job.current_retries + 1;
    if anticipated_retry >= job.max_retries {
        job_store.increment_job_retries(&job.id).await?;
        move_to_dlq(job, queue_name, job_store, error_message).await?;
        return Ok(());
    }

    let delay_seconds = calculate_backoff_seconds(settings, anticipated_retry);
    let retry_at_score = (Utc::now().timestamp_millis() as f64) + delay_seconds * 1000.0;
    let target_queue = job
        .queue_name
        .as_deref()
        .unwrap_or(&settings.default_queue_name);

    let new_retry = job_store
        .atomic_retry_job(
            &job.id,
            target_queue,
            retry_at_score,
            error_message,
            JobStatus::Retrying,
        )
        .await?;

    let next_run_time = Utc
        .timestamp_millis_opt(retry_at_score as i64)
        .single()
        .unwrap_or_else(Utc::now);
    let _ = job_store
        .update_job_next_scheduled_run_time(&job.id, next_run_time)
        .await;

    tracing::info!(
        "retrying job {} attempt {}/{}",
        job.id,
        new_retry,
        job.max_retries
    );
    Ok(())
}

fn calculate_backoff_seconds(settings: &RRQSettings, retry_attempt: i64) -> f64 {
    let attempt = if retry_attempt <= 0 { 1 } else { retry_attempt } as u32;
    let exponent = attempt.saturating_sub(1).min(30);
    let delay = settings.base_retry_delay_seconds * (2u64.pow(exponent) as f64);
    delay.min(settings.max_retry_delay_seconds)
}

async fn heartbeat_loop(
    shutdown: Arc<AtomicBool>,
    mut job_store: JobStore,
    worker_id: String,
    queues: Vec<String>,
    status: Arc<Mutex<String>>,
    running_jobs: Arc<Mutex<HashMap<String, RunningJobInfo>>>,
    settings: RRQSettings,
) {
    while !shutdown.load(Ordering::SeqCst) {
        let status_value = { status.lock().await.clone() };
        let active_jobs = running_jobs.lock().await.len();
        let mut health_data = serde_json::Map::new();
        health_data.insert("worker_id".to_string(), Value::String(worker_id.clone()));
        health_data.insert(
            "timestamp".to_string(),
            Value::String(Utc::now().to_rfc3339()),
        );
        health_data.insert("status".to_string(), Value::String(status_value));
        health_data.insert(
            "active_jobs".to_string(),
            Value::Number((active_jobs as i64).into()),
        );
        health_data.insert(
            "concurrency_limit".to_string(),
            Value::Number((settings.worker_concurrency as i64).into()),
        );
        health_data.insert(
            "queues".to_string(),
            Value::Array(queues.iter().map(|q| Value::String(q.clone())).collect()),
        );
        let ttl = settings.worker_health_check_interval_seconds + 10.0;
        if let Err(err) = job_store
            .set_worker_health(&worker_id, &health_data, ttl as i64)
            .await
        {
            tracing::error!("failed to update worker health: {err}");
        }
        if let Err(err) = recover_orphaned_jobs(&mut job_store, &settings, &shutdown).await {
            tracing::error!("failed to recover orphaned jobs: {err}");
        }

        let sleep_duration =
            Duration::from_secs_f64(settings.worker_health_check_interval_seconds.min(60.0));
        sleep_with_shutdown(&shutdown, sleep_duration).await;
    }
}

async fn recover_orphaned_jobs(
    job_store: &mut JobStore,
    settings: &RRQSettings,
    shutdown: &Arc<AtomicBool>,
) -> Result<()> {
    let mut cursor: u64 = 0;
    let mut recovered = 0u64;
    loop {
        let (next, keys) = job_store.scan_active_job_keys(cursor, 100).await?;
        for key in keys {
            if !key.starts_with(crate::constants::ACTIVE_JOBS_PREFIX) {
                continue;
            }
            let worker_id = key.trim_start_matches(crate::constants::ACTIVE_JOBS_PREFIX);
            if worker_id.is_empty() {
                continue;
            }
            let job_ids = job_store.get_active_job_ids(worker_id).await?;
            for job_id in job_ids {
                if shutdown.load(Ordering::SeqCst) {
                    return Ok(());
                }
                if job_store.get_job_lock_owner(&job_id).await?.is_some() {
                    continue;
                }
                let job_opt = job_store.get_job_definition(&job_id).await?;
                let job = match job_opt {
                    Some(job) => job,
                    None => {
                        let _ = job_store.remove_active_job(worker_id, &job_id).await;
                        continue;
                    }
                };
                let queue_name = job
                    .queue_name
                    .clone()
                    .unwrap_or_else(|| settings.default_queue_name.clone());
                if job_store.is_job_queued(&queue_name, &job_id).await? {
                    if job.status == JobStatus::Active {
                        let _ = job_store.mark_job_pending(&job_id, None).await;
                    }
                    let _ = job_store.remove_active_job(worker_id, &job_id).await;
                    continue;
                }
                if matches!(
                    job.status,
                    JobStatus::Active | JobStatus::Pending | JobStatus::Retrying
                ) {
                    let requeue_time = job.next_scheduled_run_time.unwrap_or_else(Utc::now);
                    let score_ms = requeue_time.timestamp_millis() as f64;
                    job_store
                        .add_job_to_queue(&queue_name, &job.id, score_ms)
                        .await?;
                    if job.status == JobStatus::Active {
                        let _ = job_store
                            .mark_job_pending(
                                &job.id,
                                Some("Recovered after lock expiry or worker crash."),
                            )
                            .await;
                    }
                    let _ = job_store
                        .update_job_next_scheduled_run_time(&job.id, requeue_time)
                        .await;
                    let _ = job_store.remove_active_job(worker_id, &job.id).await;
                    recovered += 1;
                } else {
                    let _ = job_store.remove_active_job(worker_id, &job.id).await;
                }
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    if recovered > 0 {
        tracing::warn!("re-queued {recovered} orphaned job(s)");
    }
    Ok(())
}

async fn cron_loop(
    shutdown: Arc<AtomicBool>,
    cron_jobs: Arc<Mutex<Vec<CronJob>>>,
    mut client: RRQClient,
    mut job_store: JobStore,
) {
    while !shutdown.load(Ordering::SeqCst) {
        let now = Utc::now();
        let mut jobs = cron_jobs.lock().await;
        for job in jobs.iter_mut() {
            if shutdown.load(Ordering::SeqCst) {
                return;
            }
            let due = match job.due(now) {
                Ok(value) => value,
                Err(err) => {
                    tracing::error!("cron job schedule error: {err}");
                    continue;
                }
            };
            if !due {
                continue;
            }
            let unique_key = if job.unique {
                Some(format!("cron:{}", job.function_name))
            } else {
                None
            };
            if let Some(ref key) = unique_key
                && let Ok(ttl) = job_store.get_lock_ttl(key).await
                && ttl > 0
            {
                let _ = job.schedule_next(now);
                continue;
            }
            let options = EnqueueOptions {
                queue_name: job.queue_name.clone(),
                unique_key: unique_key.clone(),
                max_retries: None,
                job_timeout_seconds: None,
                defer_until: None,
                defer_by: None,
                result_ttl_seconds: None,
                trace_context: None,
                job_id: None,
            };
            let _ = client
                .enqueue(
                    &job.function_name,
                    job.args.clone(),
                    job.kwargs.clone(),
                    options,
                )
                .await;
            let _ = job.schedule_next(now);
        }
        let delay = Duration::from_secs(30);
        sleep_with_shutdown(&shutdown, delay).await;
    }
}

async fn sleep_with_shutdown(shutdown: &Arc<AtomicBool>, duration: Duration) {
    let mut remaining = duration;
    let step = Duration::from_millis(100);
    while remaining > Duration::ZERO && !shutdown.load(Ordering::SeqCst) {
        let next = if remaining > step { step } else { remaining };
        sleep(next).await;
        remaining = remaining.saturating_sub(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::RedisTestContext;
    use serde_json::json;
    use tokio::sync::Mutex as TokioMutex;
    use uuid::Uuid;

    #[derive(Clone)]
    enum TestOutcome {
        Success(Value),
        Retry,
    }

    #[derive(Clone)]
    struct StaticExecutor {
        outcome: TestOutcome,
        delay: Duration,
        last_request_id: Arc<TokioMutex<Option<String>>>,
        cancelled: Arc<TokioMutex<Vec<String>>>,
    }

    impl StaticExecutor {
        fn new(outcome: TestOutcome, delay: Duration) -> Self {
            Self {
                outcome,
                delay,
                last_request_id: Arc::new(TokioMutex::new(None)),
                cancelled: Arc::new(TokioMutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl Executor for StaticExecutor {
        async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
            {
                let mut guard = self.last_request_id.lock().await;
                *guard = Some(request.request_id.clone());
            }
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let outcome = match &self.outcome {
                TestOutcome::Success(value) => ExecutionOutcome::success(
                    request.job_id.clone(),
                    request.request_id.clone(),
                    value.clone(),
                ),
                TestOutcome::Retry => ExecutionOutcome::retry(
                    request.job_id.clone(),
                    request.request_id.clone(),
                    "retry",
                    Some(30.0),
                ),
            };
            Ok(outcome)
        }

        async fn cancel(&self, job_id: &str, request_id: Option<&str>) -> Result<()> {
            let mut cancelled = self.cancelled.lock().await;
            cancelled.push(request_id.unwrap_or(job_id).to_string());
            Ok(())
        }
    }

    fn build_job(queue_name: &str, dlq_name: &str, unique_key: Option<String>) -> Job {
        Job {
            id: Job::new_id(),
            function_name: "task".to_string(),
            job_args: vec![],
            job_kwargs: serde_json::Map::new(),
            enqueue_time: Utc::now(),
            start_time: None,
            status: JobStatus::Pending,
            current_retries: 0,
            next_scheduled_run_time: None,
            max_retries: 3,
            job_timeout_seconds: Some(1),
            result_ttl_seconds: Some(30),
            job_unique_key: unique_key,
            completion_time: None,
            result: None,
            last_error: None,
            queue_name: Some(queue_name.to_string()),
            dlq_name: Some(dlq_name.to_string()),
            worker_id: None,
            trace_context: None,
        }
    }

    #[test]
    fn split_executor_name_variants() {
        let (exec, handler) = split_executor_name("exec#handler");
        assert_eq!(exec, Some("exec".to_string()));
        assert_eq!(handler, "handler");

        let (exec, handler) = split_executor_name("#handler");
        assert_eq!(exec, None);
        assert_eq!(handler, "handler");

        let (exec, handler) = split_executor_name("exec#");
        assert_eq!(exec, Some("exec".to_string()));
        assert!(handler.is_empty());

        let (exec, handler) = split_executor_name("plain");
        assert_eq!(exec, None);
        assert_eq!(handler, "plain");
    }

    #[tokio::test]
    async fn handle_execution_outcome_success_releases_lock() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();
        let job = build_job(&queue_name, &dlq_name, Some("unique-1".to_string()));
        ctx.store.save_job_definition(&job).await.unwrap();
        let acquired = ctx
            .store
            .acquire_unique_job_lock("unique-1", &job.id, 10)
            .await
            .unwrap();
        assert!(acquired);

        let outcome = ExecutionOutcome::success(&job.id, "req-1", json!({"ok": true}));
        handle_execution_outcome(&job, &queue_name, &ctx.settings, &mut ctx.store, outcome)
            .await
            .unwrap();

        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Completed);
        assert_eq!(loaded.result, Some(json!({"ok": true})));
        let ttl = ctx.store.get_lock_ttl("unique-1").await.unwrap();
        assert_eq!(ttl, 0);
    }

    #[tokio::test]
    async fn handle_execution_outcome_retry_after_sets_schedule() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();
        let job = build_job(&queue_name, &dlq_name, None);
        ctx.store.save_job_definition(&job).await.unwrap();

        let outcome = ExecutionOutcome::retry(&job.id, "req-1", "retry", Some(0.01));
        handle_execution_outcome(&job, &queue_name, &ctx.settings, &mut ctx.store, outcome)
            .await
            .unwrap();

        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Retrying);
        assert!(loaded.current_retries >= 1);
        assert!(loaded.next_scheduled_run_time.is_some());
        assert!(ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
    }

    #[tokio::test]
    async fn handle_execution_outcome_timeout_moves_to_dlq() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();
        let job = build_job(&queue_name, &dlq_name, Some("unique-timeout".to_string()));
        ctx.store.save_job_definition(&job).await.unwrap();
        let acquired = ctx
            .store
            .acquire_unique_job_lock("unique-timeout", &job.id, 10)
            .await
            .unwrap();
        assert!(acquired);

        let outcome = ExecutionOutcome::timeout(&job.id, "req-1", "timeout");
        handle_execution_outcome(&job, &queue_name, &ctx.settings, &mut ctx.store, outcome)
            .await
            .unwrap();

        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Failed);
        assert!(ctx.store.dlq_len(&dlq_name).await.unwrap() >= 1);
        let ttl = ctx.store.get_lock_ttl("unique-timeout").await.unwrap();
        assert_eq!(ttl, 0);
    }

    #[tokio::test]
    async fn handle_execution_outcome_handler_not_found_moves_to_dlq() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();
        let job = build_job(&queue_name, &dlq_name, None);
        ctx.store.save_job_definition(&job).await.unwrap();

        let outcome = ExecutionOutcome::handler_not_found(&job.id, "req-1", "missing");
        handle_execution_outcome(&job, &queue_name, &ctx.settings, &mut ctx.store, outcome)
            .await
            .unwrap();

        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Failed);
        assert!(ctx.store.dlq_len(&dlq_name).await.unwrap() >= 1);
    }

    #[tokio::test]
    async fn handle_execution_outcome_error_exceeds_retries() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();
        let mut job = build_job(&queue_name, &dlq_name, None);
        job.max_retries = 1;
        job.current_retries = 0;
        ctx.store.save_job_definition(&job).await.unwrap();

        let outcome = ExecutionOutcome::error(&job.id, "req-1", "failed");
        handle_execution_outcome(&job, &queue_name, &ctx.settings, &mut ctx.store, outcome)
            .await
            .unwrap();

        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Failed);
        assert!(ctx.store.dlq_len(&dlq_name).await.unwrap() >= 1);
    }

    #[tokio::test]
    async fn recover_orphaned_jobs_requeues_and_marks_pending() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();
        let mut job = build_job(&queue_name, &dlq_name, None);
        job.status = JobStatus::Active;
        job.next_scheduled_run_time = Some(Utc::now());
        ctx.store.save_job_definition(&job).await.unwrap();
        let worker_id = format!("worker-{}", Uuid::new_v4());
        ctx.store
            .track_active_job(&worker_id, &job.id, Utc::now())
            .await
            .unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));

        recover_orphaned_jobs(&mut ctx.store, &ctx.settings, &shutdown)
            .await
            .unwrap();

        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Pending);
        assert!(ctx.store.is_job_queued(&queue_name, &job.id).await.unwrap());
        let active = ctx.store.get_active_job_ids(&worker_id).await.unwrap();
        assert!(!active.contains(&job.id));
    }
    #[tokio::test]
    async fn calculate_backoff_respects_max_delay() {
        let settings = RRQSettings {
            base_retry_delay_seconds: 2.0,
            max_retry_delay_seconds: 5.0,
            ..Default::default()
        };
        assert_eq!(calculate_backoff_seconds(&settings, 1), 2.0);
        assert_eq!(calculate_backoff_seconds(&settings, 2), 4.0);
        assert_eq!(calculate_backoff_seconds(&settings, 3), 5.0);
    }

    #[tokio::test]
    async fn worker_processes_success_job() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        ctx.settings.default_executor_name = "test".to_string();
        let executor = Arc::new(StaticExecutor::new(
            TestOutcome::Success(json!({"ok": true})),
            Duration::from_millis(0),
        ));
        let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
        executors.insert("test".to_string(), executor);
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let job = client
            .enqueue(
                "success",
                Vec::new(),
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await
            .unwrap();
        let mut worker = RRQWorker::new(
            ctx.settings.clone(),
            None,
            Some("worker-1".to_string()),
            executors,
            true,
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Completed);
        assert_eq!(loaded.result, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn worker_processes_retry_job() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        ctx.settings.default_executor_name = "test".to_string();
        let executor = Arc::new(StaticExecutor::new(
            TestOutcome::Retry,
            Duration::from_millis(0),
        ));
        let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
        executors.insert("test".to_string(), executor);
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let job = client
            .enqueue(
                "retry",
                Vec::new(),
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await
            .unwrap();
        let mut worker = RRQWorker::new(
            ctx.settings.clone(),
            None,
            Some("worker-1".to_string()),
            executors,
            true,
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Retrying);
        assert_eq!(loaded.current_retries, 1);
        assert!(loaded.next_scheduled_run_time.is_some());
    }

    #[tokio::test]
    async fn worker_timeout_triggers_cancel() {
        let mut ctx = RedisTestContext::new().await.unwrap();
        ctx.settings.default_executor_name = "test".to_string();
        let executor = Arc::new(StaticExecutor::new(
            TestOutcome::Success(json!({"ok": true})),
            Duration::from_millis(50),
        ));
        let last_request_id = executor.last_request_id.clone();
        let cancelled = executor.cancelled.clone();
        let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
        executors.insert("test".to_string(), executor);
        let mut client = RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let options = EnqueueOptions {
            job_timeout_seconds: Some(0),
            ..Default::default()
        };
        let job = client
            .enqueue("timeout", Vec::new(), serde_json::Map::new(), options)
            .await
            .unwrap();
        let mut worker = RRQWorker::new(
            ctx.settings.clone(),
            None,
            Some("worker-1".to_string()),
            executors,
            true,
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(5), worker.run())
            .await
            .unwrap()
            .unwrap();
        let request_id = last_request_id.lock().await.clone().unwrap();
        let cancelled = cancelled.lock().await;
        assert!(cancelled.contains(&request_id));
        let loaded = ctx
            .store
            .get_job_definition(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status, JobStatus::Failed);
    }
}
