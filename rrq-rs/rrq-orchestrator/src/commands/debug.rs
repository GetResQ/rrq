use std::time::{Duration as StdDuration, Instant};

use anyhow::Result;
use rand::Rng;
use rand::prelude::IndexedRandom;
use serde_json::{Value, json};
use tokio::io::{self as tokio_io, AsyncBufReadExt, BufReader};
use tokio::time::Duration;

use rrq::config::load_toml_settings;
use rrq::store::JobStore;
use rrq::{EnqueueOptions, Job, JobStatus, RRQClient};

pub(crate) async fn debug_generate_jobs(
    config: Option<String>,
    count: usize,
    queue_names: Vec<String>,
    statuses: Vec<String>,
    age_hours: i64,
    batch_size: usize,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let mut rng = rand::rng();
    let queue_names = if queue_names.is_empty() {
        vec![
            "test".to_string(),
            "urgent".to_string(),
            "low_priority".to_string(),
            "default".to_string(),
        ]
    } else {
        queue_names
    };
    let statuses = if statuses.is_empty() {
        vec![
            "pending".to_string(),
            "completed".to_string(),
            "failed".to_string(),
            "retrying".to_string(),
        ]
    } else {
        statuses
    };
    let function_names = vec![
        "process_data",
        "send_email",
        "generate_report",
        "cleanup_files",
        "sync_database",
        "resize_image",
        "calculate_metrics",
        "export_csv",
        "backup_data",
        "validate_input",
    ];
    let now = chrono::Utc::now();
    let start_time = now - chrono::Duration::hours(age_hours);

    let mut created: Vec<Job> = Vec::new();
    for i in 0..count {
        let job_id = format!("test_job_{}_{}", chrono::Utc::now().timestamp_micros(), i);
        let function_name = function_names.choose(&mut rng).unwrap_or(&"process_data");
        let queue_name = queue_names
            .choose(&mut rng)
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        let status = statuses
            .choose(&mut rng)
            .cloned()
            .unwrap_or_else(|| "pending".to_string());
        let enqueue_delta = rng.random_range(0..(age_hours.max(1) * 3600)) as i64;
        let enqueue_time = start_time + chrono::Duration::seconds(enqueue_delta);

        let mut job = Job {
            id: job_id,
            function_name: function_name.to_string(),
            job_args: vec![
                Value::String(format!("arg_{i}")),
                Value::from(rng.random_range(1..=100) as i64),
            ],
            job_kwargs: serde_json::Map::from_iter([
                (
                    "user_id".to_string(),
                    Value::from(rng.random_range(1..=1000) as i64),
                ),
                (
                    "priority".to_string(),
                    Value::String(
                        ["high", "medium", "low"]
                            .choose(&mut rng)
                            .unwrap_or(&"medium")
                            .to_string(),
                    ),
                ),
            ]),
            enqueue_time,
            start_time: None,
            status: JobStatus::Pending,
            current_retries: rng.random_range(0..=3),
            next_scheduled_run_time: None,
            max_retries: 3,
            job_timeout_seconds: None,
            result_ttl_seconds: None,
            job_unique_key: None,
            completion_time: None,
            result: None,
            last_error: None,
            queue_name: Some(queue_name.clone()),
            dlq_name: None,
            worker_id: None,
            trace_context: None,
        };

        match status.as_str() {
            "completed" => {
                job.status = JobStatus::Completed;
                let start = enqueue_time + chrono::Duration::seconds(rng.random_range(1..=60));
                let completed = start + chrono::Duration::seconds(rng.random_range(1..=300));
                job.start_time = Some(start);
                job.completion_time = Some(completed);
                job.worker_id = Some(format!("worker_{}", rng.random_range(1..=10)));
                job.result =
                    Some(json!({"success": true, "processed_items": rng.random_range(1..=100)}));
            }
            "failed" => {
                job.status = JobStatus::Failed;
                let start = enqueue_time + chrono::Duration::seconds(rng.random_range(1..=60));
                let completed = start + chrono::Duration::seconds(rng.random_range(1..=300));
                job.start_time = Some(start);
                job.completion_time = Some(completed);
                job.worker_id = Some(format!("worker_{}", rng.random_range(1..=10)));
                job.last_error = Some(
                    [
                        "Connection timeout",
                        "Invalid input data",
                        "Database error",
                        "File not found",
                        "Permission denied",
                    ]
                    .choose(&mut rng)
                    .unwrap_or(&"Unknown error")
                    .to_string(),
                );
            }
            "active" => {
                job.status = JobStatus::Active;
                let start = enqueue_time + chrono::Duration::seconds(rng.random_range(1..=60));
                job.start_time = Some(start);
                job.worker_id = Some(format!("worker_{}", rng.random_range(1..=10)));
            }
            "retrying" => {
                job.status = JobStatus::Retrying;
            }
            _ => {
                job.status = JobStatus::Pending;
            }
        }

        created.push(job);
        if created.len() >= batch_size {
            insert_job_batch(&mut store, &created).await?;
            created.clear();
        }
    }
    if !created.is_empty() {
        insert_job_batch(&mut store, &created).await?;
    }

    println!(
        "Generated {count} fake jobs across {} queues",
        queue_names.len()
    );
    Ok(())
}

async fn insert_job_batch(store: &mut JobStore, jobs: &[Job]) -> Result<()> {
    for job in jobs {
        store.save_job_definition(job).await?;
        if matches!(job.status, JobStatus::Pending) {
            let score_ms = job.enqueue_time.timestamp_millis() as f64;
            if let Some(queue_name) = job.queue_name.as_deref() {
                store
                    .add_job_to_queue(queue_name, &job.id, score_ms)
                    .await?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn debug_generate_workers(
    config: Option<String>,
    count: usize,
    duration: u64,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let worker_ids: Vec<String> = (0..count).map(|i| format!("test_worker_{i}")).collect();
    println!("Simulating {count} workers for {duration} seconds...");
    let start = Instant::now();
    while start.elapsed() < StdDuration::from_secs(duration) {
        for worker_id in &worker_ids {
            let mut data = serde_json::Map::new();
            data.insert("worker_id".to_string(), Value::String(worker_id.clone()));
            data.insert(
                "status".to_string(),
                Value::String(
                    ["running", "idle", "polling"]
                        .choose(&mut rand::rng())
                        .unwrap_or(&"running")
                        .to_string(),
                ),
            );
            data.insert(
                "active_jobs".to_string(),
                Value::from(rand::rng().random_range(0..=5) as i64),
            );
            data.insert(
                "concurrency_limit".to_string(),
                Value::from(rand::rng().random_range(5..=20) as i64),
            );
            data.insert(
                "queues".to_string(),
                Value::Array(vec![
                    Value::String("test".to_string()),
                    Value::String("default".to_string()),
                ]),
            );
            data.insert(
                "timestamp".to_string(),
                Value::from(chrono::Utc::now().timestamp()),
            );
            store.set_worker_health(worker_id, &data, 60).await?;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        let remaining = duration.saturating_sub(start.elapsed().as_secs());
        print!("\rSimulating workers... {remaining}s remaining");
    }
    println!("\nWorker simulation complete");
    Ok(())
}

pub(crate) async fn debug_submit(
    function_name: String,
    config: Option<String>,
    args: Option<String>,
    kwargs: Option<String>,
    queue: Option<String>,
    delay: Option<i64>,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let store = JobStore::new(settings.clone()).await?;
    let mut client = RRQClient::new(settings.clone(), store.clone());

    let args_value = args
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or(Value::Array(Vec::new()));
    let kwargs_value = kwargs
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .unwrap_or(Value::Object(serde_json::Map::new()));

    let args_vec = match args_value {
        Value::Array(values) => values,
        _ => Vec::new(),
    };
    let kwargs_map = match kwargs_value {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    let job = client
        .enqueue(
            &function_name,
            args_vec,
            kwargs_map,
            EnqueueOptions {
                queue_name: Some(queue.unwrap_or(settings.default_queue_name.clone())),
                defer_by: delay.map(chrono::Duration::seconds),
                ..Default::default()
            },
        )
        .await?;

    println!("Job submitted: {}", job.id);
    println!("Function: {function_name}");
    Ok(())
}

pub(crate) async fn debug_clear(
    config: Option<String>,
    confirm: bool,
    pattern: String,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let keys = store.scan_keys_by_pattern(&pattern).await?;
    if keys.is_empty() {
        println!("No keys found matching pattern: {pattern}");
        return Ok(());
    }
    println!("Found {} keys matching pattern: {pattern}", keys.len());
    let deleted = if confirm {
        store.delete_keys_by_pattern(&pattern).await?
    } else {
        println!("Delete {} keys? [y/N]", keys.len());
        let mut input = String::new();
        let mut stdin = BufReader::new(tokio_io::stdin());
        let _ = stdin.read_line(&mut input).await;
        if input.trim().eq_ignore_ascii_case("y") {
            store.delete_keys_by_pattern(&pattern).await?
        } else {
            println!("Deletion cancelled");
            0
        }
    };
    if deleted > 0 {
        println!("Deleted {deleted} keys");
    }
    Ok(())
}

pub(crate) async fn debug_stress_test(
    config: Option<String>,
    jobs_per_second: u64,
    duration: u64,
    queues: Vec<String>,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let store = JobStore::new(settings.clone()).await?;
    let mut client = RRQClient::new(settings.clone(), store.clone());
    let queues = if queues.is_empty() {
        vec!["stress_test".to_string()]
    } else {
        queues
    };
    println!("Starting stress test: {jobs_per_second} jobs/sec for {duration}s");
    let start = Instant::now();
    let mut total_jobs = 0u64;
    while start.elapsed() < StdDuration::from_secs(duration) {
        let batch_start = Instant::now();
        for i in 0..jobs_per_second {
            let queue_name = queues
                .choose(&mut rand::rng())
                .cloned()
                .unwrap_or_else(|| settings.default_queue_name.clone());
            let _ = client
                .enqueue(
                    "stress_test_job",
                    vec![Value::from((total_jobs + i) as i64)],
                    serde_json::Map::from_iter([(
                        "batch".to_string(),
                        Value::from(chrono::Utc::now().timestamp()),
                    )]),
                    EnqueueOptions {
                        queue_name: Some(queue_name),
                        ..Default::default()
                    },
                )
                .await?;
        }
        total_jobs += jobs_per_second;
        let elapsed = batch_start.elapsed();
        if elapsed < StdDuration::from_secs(1) {
            let remaining = 1.0 - elapsed.as_secs_f64();
            if remaining > 0.0 {
                tokio::time::sleep(Duration::from_secs_f64(remaining)).await;
            }
        }
        let remaining = duration.saturating_sub(start.elapsed().as_secs());
        print!("\rStress test: {total_jobs} jobs submitted, {remaining}s remaining");
    }
    println!("\nStress test complete: {total_jobs} jobs submitted");
    Ok(())
}
