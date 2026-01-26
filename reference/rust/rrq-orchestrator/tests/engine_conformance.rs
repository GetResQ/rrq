use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Duration;
use rrq_orchestrator::executor::Executor;
use rrq_orchestrator::store::JobStore;
use rrq_orchestrator::{EnqueueOptions, Job, JobStatus, RRQClient, RRQSettings, RRQWorker};
use rrq_protocol::{ExecutionOutcome, ExecutionRequest, OutcomeStatus};
use serde_json::{json, Value};

#[derive(Clone)]
struct ScenarioExecutor {
    prefix: String,
}

impl ScenarioExecutor {
    fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl Default for ScenarioExecutor {
    fn default() -> Self {
        Self::new("ok")
    }
}

#[async_trait]
impl Executor for ScenarioExecutor {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
        let attempt = request.context.attempt;
        let value = request
            .args
            .first()
            .and_then(|arg| arg.as_str())
            .unwrap_or("unknown");
        match request.function_name.as_str() {
            "success" => Ok(ExecutionOutcome {
                job_id: Some(request.job_id),
                status: OutcomeStatus::Success,
                result: Some(Value::String(format!("{}:{value}", self.prefix))),
                error_message: None,
                error_type: None,
                retry_after_seconds: None,
            }),
            "retry" => {
                if attempt == 1 {
                    Ok(ExecutionOutcome {
                        job_id: Some(request.job_id),
                        status: OutcomeStatus::Retry,
                        result: None,
                        error_message: Some("retry".to_string()),
                        error_type: None,
                        retry_after_seconds: Some(0.01),
                    })
                } else {
                    Ok(ExecutionOutcome {
                        job_id: Some(request.job_id),
                        status: OutcomeStatus::Success,
                        result: Some(Value::String(format!("{}:{value}", self.prefix))),
                        error_message: None,
                        error_type: None,
                        retry_after_seconds: None,
                    })
                }
            }
            "fail" => Ok(ExecutionOutcome {
                job_id: Some(request.job_id),
                status: OutcomeStatus::Error,
                result: None,
                error_message: Some(format!("boom: {value}")),
                error_type: None,
                retry_after_seconds: None,
            }),
            "timeout" => {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                Ok(ExecutionOutcome {
                    job_id: Some(request.job_id),
                    status: OutcomeStatus::Success,
                    result: Some(Value::String("late".to_string())),
                    error_message: None,
                    error_type: None,
                    retry_after_seconds: None,
                })
            }
            "missing_handler" => Ok(ExecutionOutcome {
                job_id: Some(request.job_id),
                status: OutcomeStatus::Error,
                result: None,
                error_message: Some(
                    "No handler registered for function 'missing_handler'".to_string(),
                ),
                error_type: Some("handler_not_found".to_string()),
                retry_after_seconds: None,
            }),
            _ => Ok(ExecutionOutcome {
                job_id: Some(request.job_id),
                status: OutcomeStatus::Error,
                result: None,
                error_message: Some("unknown".to_string()),
                error_type: None,
                retry_after_seconds: None,
            }),
        }
    }
}

async fn wait_for_terminal(store: &mut JobStore, job_ids: &[&str], timeout: StdDuration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut all_terminal = true;
        for job_id in job_ids {
            let job = store.get_job_definition(job_id).await.unwrap();
            if let Some(job) = job {
                if !matches!(job.status, JobStatus::Completed | JobStatus::Failed) {
                    all_terminal = false;
                    break;
                }
            } else {
                all_terminal = false;
                break;
            }
        }
        if all_terminal {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            let mut states = Vec::new();
            for job_id in job_ids {
                let status = store
                    .get_job_definition(job_id)
                    .await
                    .ok()
                    .flatten()
                    .map(|job| job.status.as_str().to_string())
                    .unwrap_or_else(|| "missing".to_string());
                states.push(format!("{job_id}={status}"));
            }
            panic!(
                "Timed out waiting for jobs to reach terminal state: {}",
                states.join(", ")
            );
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

fn normalize_job(job: &Job, in_dlq: bool) -> Value {
    let last_error = if job.status == JobStatus::Completed {
        Value::Null
    } else {
        match &job.last_error {
            Some(value) => Value::String(value.clone()),
            None => Value::Null,
        }
    };
    json!({
        "status": job.status.as_str(),
        "current_retries": job.current_retries,
        "max_retries": job.max_retries,
        "result": job.result,
        "last_error": last_error,
        "in_dlq": in_dlq,
    })
}

fn normalize_job_extended(job: &Job, in_dlq: bool) -> Value {
    let mut payload = normalize_job(job, in_dlq);
    let deferred = job
        .next_scheduled_run_time
        .map(|next| next > job.enqueue_time + Duration::milliseconds(1))
        .unwrap_or(false);
    if let Value::Object(ref mut map) = payload {
        map.insert(
            "queue_name".to_string(),
            job.queue_name
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        map.insert(
            "unique_key".to_string(),
            job.job_unique_key
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        map.insert("deferred".to_string(), Value::Bool(deferred));
    }
    payload
}

#[tokio::test]
async fn test_engine_conformance_basic() -> Result<()> {
    let settings = RRQSettings {
        redis_dsn: "redis://localhost:6379/3".to_string(),
        default_job_timeout_seconds: 1,
        default_result_ttl_seconds: 10,
        default_max_retries: 3,
        worker_concurrency: 2,
        default_poll_delay_seconds: 0.01,
        worker_shutdown_grace_period_seconds: 0.2,
        base_retry_delay_seconds: 0.01,
        max_retry_delay_seconds: 0.05,
        ..Default::default()
    };

    let mut store = JobStore::new(settings.clone()).await?;
    store.flushdb().await?;

    let mut client = RRQClient::new(settings.clone(), store.clone());
    let job_ids = [
        "job-success",
        "job-retry",
        "job-fail",
        "job-timeout",
        "job-missing",
    ];

    client
        .enqueue(
            "success",
            vec![Value::String("alpha".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-success".to_string()),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "retry",
            vec![Value::String("beta".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-retry".to_string()),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "fail",
            vec![Value::String("gamma".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-fail".to_string()),
                max_retries: Some(2),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "timeout",
            vec![],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-timeout".to_string()),
                job_timeout_seconds: Some(1),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "missing_handler",
            vec![],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-missing".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
    executors.insert("python".to_string(), Arc::new(ScenarioExecutor::default()));

    let mut worker = RRQWorker::new(settings.clone(), None, None, executors, false).await?;
    let shutdown = worker.shutdown_handle();
    let handle = tokio::spawn(async move { worker.run().await });

    wait_for_terminal(&mut store, &job_ids, StdDuration::from_secs(4)).await;
    shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = handle.await;

    let dlq_ids = store
        .get_dlq_job_ids(&settings.default_dlq_name)
        .await?
        .into_iter()
        .collect::<Vec<String>>();

    let mut jobs = serde_json::Map::new();
    for job_id in job_ids {
        let job = store.get_job_definition(job_id).await?.unwrap();
        let in_dlq = dlq_ids.iter().any(|id| id == job_id);
        jobs.insert(job_id.to_string(), normalize_job(&job, in_dlq));
    }

    let golden_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/data/engine_golden/basic.json");
    let golden = std::fs::read_to_string(&golden_path)?;
    let golden_json: Value = serde_json::from_str(&golden)?;
    let expected_jobs = golden_json.get("jobs").cloned().unwrap_or(Value::Null);

    assert_eq!(Value::Object(jobs), expected_jobs);

    store.flushdb().await?;

    Ok(())
}

#[tokio::test]
async fn test_engine_conformance_extended() -> Result<()> {
    let settings = RRQSettings {
        redis_dsn: "redis://localhost:6379/4".to_string(),
        default_job_timeout_seconds: 1,
        default_result_ttl_seconds: 10,
        default_max_retries: 3,
        worker_concurrency: 2,
        default_poll_delay_seconds: 0.01,
        worker_shutdown_grace_period_seconds: 0.2,
        base_retry_delay_seconds: 0.01,
        max_retry_delay_seconds: 0.05,
        default_unique_job_lock_ttl_seconds: 1,
        ..Default::default()
    };

    let mut store = JobStore::new(settings.clone()).await?;
    store.flushdb().await?;

    let mut client = RRQClient::new(settings.clone(), store.clone());
    let job_ids = [
        "job-unique-1",
        "job-unique-2",
        "job-defer",
        "job-custom-queue",
        "job-dlq",
    ];

    client
        .enqueue(
            "success",
            vec![Value::String("alpha".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-unique-1".to_string()),
                unique_key: Some("unique-alpha".to_string()),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "success",
            vec![Value::String("beta".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-unique-2".to_string()),
                unique_key: Some("unique-alpha".to_string()),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "success",
            vec![Value::String("gamma".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-defer".to_string()),
                defer_by: Some(Duration::seconds(1)),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "success",
            vec![Value::String("delta".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-custom-queue".to_string()),
                queue_name: Some("rrq:queue:custom".to_string()),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "fail",
            vec![Value::String("epsilon".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-dlq".to_string()),
                max_retries: Some(1),
                ..Default::default()
            },
        )
        .await?;

    let custom_size = store.queue_size("rrq:queue:custom").await?;
    assert_eq!(custom_size, 1, "custom queue should have 1 job");

    let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
    executors.insert("python".to_string(), Arc::new(ScenarioExecutor::default()));

    let queues = vec![
        settings.default_queue_name.clone(),
        "rrq:queue:custom".to_string(),
    ];
    let mut worker = RRQWorker::new(settings.clone(), Some(queues), None, executors, false).await?;
    let shutdown = worker.shutdown_handle();
    let handle = tokio::spawn(async move { worker.run().await });

    wait_for_terminal(&mut store, &job_ids, StdDuration::from_secs(5)).await;
    shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = handle.await;

    let dlq_ids = store
        .get_dlq_job_ids(&settings.default_dlq_name)
        .await?
        .into_iter()
        .collect::<Vec<String>>();

    let mut jobs = serde_json::Map::new();
    for job_id in job_ids {
        let job = store.get_job_definition(job_id).await?.unwrap();
        let in_dlq = dlq_ids.iter().any(|id| id == job_id);
        jobs.insert(job_id.to_string(), normalize_job_extended(&job, in_dlq));
    }

    let golden_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/data/engine_golden/extended.json");
    let golden = std::fs::read_to_string(&golden_path)?;
    let golden_json: Value = serde_json::from_str(&golden)?;
    let expected_jobs = golden_json.get("jobs").cloned().unwrap_or(Value::Null);

    assert_eq!(Value::Object(jobs), expected_jobs);

    store.flushdb().await?;

    Ok(())
}

#[tokio::test]
async fn test_engine_conformance_retry_backoff() -> Result<()> {
    let settings = RRQSettings {
        redis_dsn: "redis://localhost:6379/5".to_string(),
        default_job_timeout_seconds: 1,
        default_result_ttl_seconds: 10,
        default_max_retries: 3,
        worker_concurrency: 1,
        default_poll_delay_seconds: 0.01,
        worker_shutdown_grace_period_seconds: 0.2,
        base_retry_delay_seconds: 0.01,
        max_retry_delay_seconds: 0.05,
        ..Default::default()
    };

    let mut store = JobStore::new(settings.clone()).await?;
    store.flushdb().await?;

    let mut client = RRQClient::new(settings.clone(), store.clone());
    let job_ids = ["job-retry-backoff"];

    client
        .enqueue(
            "fail",
            vec![Value::String("backoff".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-retry-backoff".to_string()),
                max_retries: Some(3),
                ..Default::default()
            },
        )
        .await?;

    let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
    executors.insert("python".to_string(), Arc::new(ScenarioExecutor::default()));

    let mut worker = RRQWorker::new(settings.clone(), None, None, executors, false).await?;
    let shutdown = worker.shutdown_handle();
    let handle = tokio::spawn(async move { worker.run().await });

    wait_for_terminal(&mut store, &job_ids, StdDuration::from_secs(5)).await;
    shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = handle.await;

    let dlq_ids = store
        .get_dlq_job_ids(&settings.default_dlq_name)
        .await?
        .into_iter()
        .collect::<Vec<String>>();

    let mut jobs = serde_json::Map::new();
    for job_id in job_ids {
        let job = store.get_job_definition(job_id).await?.unwrap();
        let in_dlq = dlq_ids.iter().any(|id| id == job_id);
        jobs.insert(job_id.to_string(), normalize_job(&job, in_dlq));
    }

    let golden_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/data/engine_golden/retry_backoff.json");
    let golden = std::fs::read_to_string(&golden_path)?;
    let golden_json: Value = serde_json::from_str(&golden)?;
    let expected_jobs = golden_json.get("jobs").cloned().unwrap_or(Value::Null);

    assert_eq!(Value::Object(jobs), expected_jobs);

    store.flushdb().await?;

    Ok(())
}

#[tokio::test]
async fn test_engine_conformance_routing() -> Result<()> {
    let mut executor_routes = HashMap::new();
    executor_routes.insert("rrq:queue:routed".to_string(), "alt".to_string());
    let settings = RRQSettings {
        redis_dsn: "redis://localhost:6379/6".to_string(),
        default_job_timeout_seconds: 1,
        default_result_ttl_seconds: 10,
        default_max_retries: 3,
        worker_concurrency: 2,
        default_poll_delay_seconds: 0.01,
        worker_shutdown_grace_period_seconds: 0.2,
        executor_routes,
        ..Default::default()
    };

    let mut store = JobStore::new(settings.clone()).await?;
    store.flushdb().await?;

    let mut client = RRQClient::new(settings.clone(), store.clone());
    let job_ids = ["job-route-default", "job-route-alt"];

    client
        .enqueue(
            "success",
            vec![Value::String("alpha".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-route-default".to_string()),
                ..Default::default()
            },
        )
        .await?;
    client
        .enqueue(
            "success",
            vec![Value::String("beta".to_string())],
            serde_json::Map::new(),
            EnqueueOptions {
                job_id: Some("job-route-alt".to_string()),
                queue_name: Some("rrq:queue:routed".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
    executors.insert("python".to_string(), Arc::new(ScenarioExecutor::new("py")));
    executors.insert("alt".to_string(), Arc::new(ScenarioExecutor::new("alt")));

    let queues = vec![
        settings.default_queue_name.clone(),
        "rrq:queue:routed".to_string(),
    ];
    let mut worker = RRQWorker::new(settings.clone(), Some(queues), None, executors, false).await?;
    let shutdown = worker.shutdown_handle();
    let handle = tokio::spawn(async move { worker.run().await });

    wait_for_terminal(&mut store, &job_ids, StdDuration::from_secs(4)).await;
    shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = handle.await;

    let dlq_ids = store
        .get_dlq_job_ids(&settings.default_dlq_name)
        .await?
        .into_iter()
        .collect::<Vec<String>>();

    let mut jobs = serde_json::Map::new();
    for job_id in job_ids {
        let job = store.get_job_definition(job_id).await?.unwrap();
        let in_dlq = dlq_ids.iter().any(|id| id == job_id);
        jobs.insert(job_id.to_string(), normalize_job_extended(&job, in_dlq));
    }

    let golden_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/data/engine_golden/routing.json");
    let golden = std::fs::read_to_string(&golden_path)?;
    let golden_json: Value = serde_json::from_str(&golden)?;
    let expected_jobs = golden_json.get("jobs").cloned().unwrap_or(Value::Null);

    assert_eq!(Value::Object(jobs), expected_jobs);

    store.flushdb().await?;

    Ok(())
}
