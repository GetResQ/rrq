use std::collections::HashMap;

use anyhow::Result;

use crate::cli_utils;
use rrq::JobStatus;
use rrq::config::load_toml_settings;
use rrq::store::JobStore;

use super::shared::{queue_matches, top_counts};

pub(crate) struct DlqListOptions {
    pub(crate) config: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) function: Option<String>,
    pub(crate) limit: usize,
    pub(crate) offset: usize,
    pub(crate) dlq_name: Option<String>,
    pub(crate) raw: bool,
    pub(crate) batch_size: usize,
}

pub(crate) async fn dlq_list(options: DlqListOptions) -> Result<()> {
    let settings = load_toml_settings(options.config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let dlq_name = options
        .dlq_name
        .unwrap_or(settings.default_dlq_name.clone());
    let job_ids = store.get_dlq_job_ids(&dlq_name).await?;
    let mut jobs: Vec<HashMap<String, String>> = Vec::new();
    let chunk_size = std::cmp::max(1, options.batch_size);
    for batch in job_ids.chunks(chunk_size) {
        let batch_maps = store.get_job_data_maps(batch).await?;
        for job_map in batch_maps.into_iter().flatten() {
            if let Some(filter) = options.queue.as_deref() {
                let job_queue = job_map.get("queue_name").cloned().unwrap_or_default();
                if !queue_matches(filter, &job_queue) {
                    continue;
                }
            }
            if let Some(filter) = options.function.as_deref()
                && job_map.get("function_name").map(|v| v.as_str()) != Some(filter)
            {
                continue;
            }
            jobs.push(job_map);
        }
    }

    jobs.sort_by(|a, b| {
        let a_ts = a
            .get("completion_time")
            .and_then(|v| cli_utils::parse_timestamp(v))
            .unwrap_or(0.0);
        let b_ts = b
            .get("completion_time")
            .and_then(|v| cli_utils::parse_timestamp(v))
            .unwrap_or(0.0);
        b_ts.partial_cmp(&a_ts).unwrap_or(std::cmp::Ordering::Equal)
    });

    let jobs = jobs
        .into_iter()
        .skip(options.offset)
        .take(options.limit)
        .collect::<Vec<_>>();

    if options.raw {
        for job in &jobs {
            println!("{}", serde_json::to_string_pretty(job)?);
        }
        return Ok(());
    }

    if jobs.is_empty() {
        println!("No jobs found in DLQ: {dlq_name}");
        return Ok(());
    }

    println!(
        "{:<20} {:<18} {:<16} {:<25} {:<18} {:>7}",
        "Job ID", "Function", "Queue", "Error", "Failed At", "Retries"
    );
    for job in &jobs {
        let job_id = job.get("id").cloned().unwrap_or_default();
        let function_name = job
            .get("function_name")
            .cloned()
            .unwrap_or_else(|| "N/A".to_string());
        let queue_name = job
            .get("queue_name")
            .cloned()
            .unwrap_or_else(|| "N/A".to_string());
        let error = job
            .get("last_error")
            .or_else(|| job.get("error"))
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string());
        let failed_at = cli_utils::format_timestamp(job.get("completion_time").map(|v| v.as_str()));
        let retries = job
            .get("current_retries")
            .or_else(|| job.get("retries"))
            .cloned()
            .unwrap_or_else(|| "0".to_string());
        println!(
            "{:<20} {:<18} {:<16} {:<25} {:<18} {:>7}",
            cli_utils::truncate(&job_id, 18),
            cli_utils::truncate(&function_name, 16),
            cli_utils::truncate(&queue_name, 14),
            cli_utils::truncate(&error, 23),
            failed_at,
            retries
        );
    }

    let total = store.dlq_len(&dlq_name).await?;
    let showing_start = options.offset + 1;
    let jobs_shown = jobs.len();
    let showing_end = std::cmp::min(options.offset + jobs_shown, total as usize);
    println!("\nShowing {showing_start}-{showing_end} of {total} jobs");
    if showing_end < total as usize {
        println!(
            "Use --offset {} to see more",
            options.offset + options.limit
        );
    }

    Ok(())
}

pub(crate) async fn dlq_stats(config: Option<String>, dlq_name: Option<String>) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let dlq_name = dlq_name.unwrap_or(settings.default_dlq_name.clone());
    let job_ids = store.get_dlq_job_ids(&dlq_name).await?;
    if job_ids.is_empty() {
        println!("DLQ '{dlq_name}' is empty");
        return Ok(());
    }
    let mut jobs: Vec<HashMap<String, String>> = Vec::new();
    for job_id in job_ids {
        if let Some(job_map) = store.get_job_data_map(&job_id).await? {
            jobs.push(job_map);
        }
    }
    let mut completion_times = Vec::new();
    let mut retries = Vec::new();
    let mut by_queue: HashMap<String, usize> = HashMap::new();
    let mut by_function: HashMap<String, usize> = HashMap::new();
    let mut error_counts: HashMap<String, usize> = HashMap::new();

    for job in &jobs {
        if let Some(ts) = job
            .get("completion_time")
            .and_then(|v| cli_utils::parse_timestamp(v))
        {
            completion_times.push(ts);
        }
        let retry_count = job
            .get("current_retries")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        retries.push(retry_count);
        let queue = job
            .get("queue_name")
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string());
        *by_queue.entry(queue).or_insert(0) += 1;
        let function = job
            .get("function_name")
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string());
        *by_function.entry(function).or_insert(0) += 1;
        let error = job
            .get("last_error")
            .or_else(|| job.get("error"))
            .cloned()
            .unwrap_or_else(|| "Unknown error".to_string());
        let key = cli_utils::truncate(&error, 100);
        *error_counts.entry(key).or_insert(0) += 1;
    }

    println!("DLQ: {dlq_name}");
    println!("Total jobs: {}", jobs.len());
    if !completion_times.is_empty() {
        completion_times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let oldest = completion_times.first().cloned().unwrap_or(0.0);
        let newest = completion_times.last().cloned().unwrap_or(0.0);
        let oldest_str = cli_utils::to_utc_rfc3339(oldest);
        let newest_str = cli_utils::to_utc_rfc3339(newest);
        println!("Oldest: {}", cli_utils::format_timestamp(Some(&oldest_str)));
        println!("Newest: {}", cli_utils::format_timestamp(Some(&newest_str)));
    }
    if !retries.is_empty() {
        let sum: i64 = retries.iter().sum();
        let avg = sum as f64 / retries.len() as f64;
        println!("Avg retries: {avg:.2}");
    }

    if !by_queue.is_empty() {
        println!("\nTop queues:");
        for (queue, count) in top_counts(&by_queue, 10) {
            println!("  - {queue}: {count}");
        }
    }

    if !by_function.is_empty() {
        println!("\nTop functions:");
        for (function, count) in top_counts(&by_function, 10) {
            println!("  - {function}: {count}");
        }
    }

    if !error_counts.is_empty() {
        println!("\nTop errors:");
        for (error, count) in top_counts(&error_counts, 5) {
            println!("  - ({count}x) {error}");
        }
    }

    Ok(())
}

pub(crate) async fn dlq_inspect(job_id: String, config: Option<String>, raw: bool) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let Some(job_map) = store.get_job_data_map(&job_id).await? else {
        println!("Job '{job_id}' not found");
        return Ok(());
    };
    if raw {
        println!("{}", serde_json::to_string_pretty(&job_map)?);
    } else {
        println!("DLQ Job {job_id}");
        println!("{}", serde_json::to_string_pretty(&job_map)?);
    }
    Ok(())
}

pub(crate) struct DlqRequeueOptions {
    pub(crate) config: Option<String>,
    pub(crate) dlq_name: Option<String>,
    pub(crate) target_queue: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) function: Option<String>,
    pub(crate) job_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) all: bool,
    pub(crate) dry_run: bool,
}

pub(crate) async fn dlq_requeue(options: DlqRequeueOptions) -> Result<()> {
    let settings = load_toml_settings(options.config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let dlq_name = options
        .dlq_name
        .unwrap_or(settings.default_dlq_name.clone());
    let target_queue = options
        .target_queue
        .unwrap_or(settings.default_queue_name.clone());

    let has_filter =
        options.queue.is_some() || options.function.is_some() || options.job_id.is_some();
    if !has_filter && !options.all {
        println!("Refusing to requeue all jobs without --all or filters.");
        return Ok(());
    }

    let job_ids = store.get_dlq_job_ids(&dlq_name).await?;
    let mut jobs: Vec<HashMap<String, String>> = Vec::new();
    for id in job_ids {
        if let Some(job_map) = store.get_job_data_map(&id).await? {
            if let Some(filter) = options.job_id.as_deref()
                && id != filter
            {
                continue;
            }
            if let Some(filter) = options.queue.as_deref() {
                let job_queue = job_map.get("queue_name").cloned().unwrap_or_default();
                if !queue_matches(filter, &job_queue) {
                    continue;
                }
            }
            if let Some(filter) = options.function.as_deref()
                && job_map.get("function_name").map(|v| v.as_str()) != Some(filter)
            {
                continue;
            }
            jobs.push(job_map);
        }
    }

    if let Some(limit) = options.limit {
        jobs.truncate(limit);
    }

    if options.dry_run {
        println!("Dry run: would requeue {} job(s).", jobs.len());
        for job in &jobs {
            let id = job.get("id").cloned().unwrap_or_default();
            println!("  - {id}");
        }
        return Ok(());
    }

    let mut requeued = 0usize;
    for job in jobs {
        let id = job.get("id").cloned().unwrap_or_default();
        if id.is_empty() {
            continue;
        }
        let removed = store.dlq_remove_job(&dlq_name, &id).await?;
        if removed > 0 {
            let now_ms = chrono::Utc::now().timestamp_millis() as f64;
            store.add_job_to_queue(&target_queue, &id, now_ms).await?;
            let mut mapping: HashMap<String, String> = HashMap::new();
            mapping.insert(
                "status".to_string(),
                JobStatus::Pending.as_str().to_string(),
            );
            mapping.insert("queue_name".to_string(), target_queue.clone());
            let _ = store.update_job_fields(&id, &mapping).await;
            requeued += 1;
        }
    }

    println!("Requeued {requeued} job(s) to {target_queue}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::RedisTestContext;
    use chrono::Utc;
    use rrq::{Job, JobStatus};
    use serde_json::json;

    fn build_job(queue_name: &str, dlq_name: &str, function_name: &str) -> Job {
        Job {
            id: Job::new_id(),
            function_name: function_name.to_string(),
            job_args: vec![json!("arg")],
            job_kwargs: serde_json::Map::new(),
            enqueue_time: Utc::now(),
            start_time: None,
            status: JobStatus::Failed,
            current_retries: 1,
            next_scheduled_run_time: None,
            max_retries: 3,
            job_timeout_seconds: Some(30),
            result_ttl_seconds: Some(60),
            job_unique_key: None,
            completion_time: Some(Utc::now()),
            result: None,
            last_error: Some("boom".to_string()),
            queue_name: Some(queue_name.to_string()),
            dlq_name: Some(dlq_name.to_string()),
            worker_id: Some("worker-1".to_string()),
            trace_context: None,
        }
    }

    #[tokio::test]
    async fn dlq_commands_cover_branches() -> Result<()> {
        let mut ctx = RedisTestContext::new().await?;
        let config = ctx.write_config().await?;
        let config_path = Some(config.path().to_string_lossy().to_string());
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();

        dlq_inspect("missing".to_string(), config_path.clone(), false).await?;

        let job1 = build_job(&queue_name, &dlq_name, "one");
        ctx.store.save_job_definition(&job1).await?;
        ctx.store
            .move_job_to_dlq(&job1.id, &dlq_name, "boom", Utc::now())
            .await?;
        let job2 = build_job(&queue_name, &dlq_name, "two");
        ctx.store.save_job_definition(&job2).await?;
        ctx.store
            .move_job_to_dlq(&job2.id, &dlq_name, "boom2", Utc::now())
            .await?;

        dlq_list(DlqListOptions {
            config: config_path.clone(),
            queue: None,
            function: None,
            limit: 10,
            offset: 0,
            dlq_name: Some(dlq_name.clone()),
            raw: false,
            batch_size: 1,
        })
        .await?;
        dlq_list(DlqListOptions {
            config: config_path.clone(),
            queue: None,
            function: None,
            limit: 10,
            offset: 0,
            dlq_name: Some(dlq_name.clone()),
            raw: true,
            batch_size: 2,
        })
        .await?;

        dlq_stats(config_path.clone(), Some(dlq_name.clone())).await?;
        dlq_inspect(job1.id.clone(), config_path.clone(), true).await?;

        dlq_requeue(DlqRequeueOptions {
            config: config_path.clone(),
            dlq_name: Some(dlq_name.clone()),
            target_queue: None,
            queue: None,
            function: None,
            job_id: None,
            limit: None,
            all: false,
            dry_run: false,
        })
        .await?;

        dlq_requeue(DlqRequeueOptions {
            config: config_path.clone(),
            dlq_name: Some(dlq_name.clone()),
            target_queue: None,
            queue: None,
            function: None,
            job_id: Some(job1.id.clone()),
            limit: None,
            all: false,
            dry_run: true,
        })
        .await?;

        dlq_requeue(DlqRequeueOptions {
            config: config_path.clone(),
            dlq_name: Some(dlq_name.clone()),
            target_queue: None,
            queue: Some(queue_name.clone()),
            function: None,
            job_id: Some(job1.id.clone()),
            limit: Some(1),
            all: false,
            dry_run: false,
        })
        .await?;

        let remaining = ctx.store.dlq_len(&dlq_name).await?;
        assert!(remaining <= 1);
        let queue_size = ctx.store.queue_size(&queue_name).await?;
        assert!(queue_size >= 1);
        Ok(())
    }
}
