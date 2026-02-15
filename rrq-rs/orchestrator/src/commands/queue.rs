use anyhow::Result;

use crate::cli_utils;
use rrq::load_toml_settings;
use rrq::store::JobStore;
use rrq_config::normalize_queue_name;

use super::shared::queue_matches;

pub(crate) async fn queue_list(config: Option<String>, show_empty: bool) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let mut cursor = 0u64;
    let mut keys: Vec<String> = Vec::new();
    loop {
        let (next, batch) = store.scan_queue_keys(cursor, 100).await?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    if keys.is_empty() {
        println!("No active queues found");
        return Ok(());
    }
    println!(
        "{:<30} {:>10} {:>20} {:>20}",
        "Queue", "Pending", "Oldest(ms)", "Newest(ms)"
    );
    let mut total = 0i64;
    for key in keys {
        let queue_name = normalize_queue_name(&key);
        let size = store.queue_size(&queue_name).await?;
        if size == 0 && !show_empty {
            continue;
        }
        total += size;
        let oldest = store.queue_range_with_scores(&queue_name, 0, 0).await?;
        let newest = store.queue_range_with_scores(&queue_name, -1, -1).await?;
        let oldest_score = oldest.first().map(|(_, score)| *score).unwrap_or(0.0);
        let newest_score = newest.first().map(|(_, score)| *score).unwrap_or(0.0);
        println!(
            "{:<30} {:>10} {:>20.0} {:>20.0}",
            queue_name, size, oldest_score, newest_score
        );
    }
    println!("\nTotal: {total} pending jobs");
    Ok(())
}

pub(crate) async fn queue_stats(
    config: Option<String>,
    queues: Vec<String>,
    max_scan: usize,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let mut queue_names: Vec<String> = queues
        .into_iter()
        .map(|queue| normalize_queue_name(&queue))
        .collect();
    if queue_names.is_empty() {
        let mut cursor = 0u64;
        loop {
            let (next, batch) = store.scan_queue_keys(cursor, 200).await?;
            for key in batch {
                queue_names.push(normalize_queue_name(&key));
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
    }
    queue_names.sort();
    queue_names.dedup();
    if queue_names.is_empty() {
        println!("No queues found");
        return Ok(());
    }

    println!(
        "{:<25} {:>8} {:>8} {:>8} {:>9} {:>8} {:>10} {:>12}",
        "Queue", "Total", "Pending", "Active", "Completed", "Failed", "DLQ", "Avg Wait"
    );

    for queue_name in queue_names {
        let pending = store.queue_size(&queue_name).await?;
        let mut active = 0i64;
        let mut completed = 0i64;
        let mut failed = 0i64;
        let mut scanned = 0usize;
        let mut cursor = 0u64;
        loop {
            let (next, keys) = store.scan_job_keys(cursor, 200).await?;
            for key in keys {
                if max_scan > 0 && scanned >= max_scan {
                    break;
                }
                scanned += 1;
                if let Some(job_map) = store.get_job_data_map_by_key(&key).await? {
                    let job_queue = job_map.get("queue_name").cloned().unwrap_or_default();
                    if !queue_matches(&queue_name, &job_queue) {
                        continue;
                    }
                    let status = job_map
                        .get("status")
                        .map(|value| value.to_lowercase())
                        .unwrap_or_default();
                    match status.as_str() {
                        "active" => active += 1,
                        "completed" => completed += 1,
                        "failed" => failed += 1,
                        _ => {}
                    }
                }
            }
            if max_scan > 0 && scanned >= max_scan {
                break;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }

        let total = pending + active + completed + failed;
        if total == 0 {
            continue;
        }

        let avg_wait = if pending > 0 {
            let entries = store.queue_range_with_scores(&queue_name, 0, 99).await?;
            if entries.is_empty() {
                None
            } else {
                let now_ms = chrono::Utc::now().timestamp_millis() as f64;
                let mut sum_seconds = 0.0;
                for (_, score) in &entries {
                    sum_seconds += (now_ms - score) / 1000.0;
                }
                Some(sum_seconds / entries.len() as f64)
            }
        } else {
            None
        };

        let dlq_jobs =
            count_dlq_for_queue(&mut store, &settings.default_dlq_name, &queue_name).await?;

        println!(
            "{:<25} {:>8} {:>8} {:>8} {:>9} {:>8} {:>10} {:>12}",
            queue_name,
            total,
            pending,
            active,
            completed,
            failed,
            dlq_jobs,
            cli_utils::format_duration(avg_wait)
        );
    }

    if max_scan > 0 {
        println!("\nNote: Active/Completed/Failed counts based on scanning up to {max_scan} jobs.");
        println!("Use --max-scan 0 for complete scan (may be slow for large datasets).");
    }

    Ok(())
}

pub(crate) async fn queue_inspect(
    queue_name: String,
    config: Option<String>,
    limit: usize,
    offset: usize,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let queue_name = normalize_queue_name(&queue_name);
    let exists = store.queue_exists(&queue_name).await?;
    if !exists {
        println!("Queue '{queue_name}' not found");
        return Ok(());
    }
    let total_size = store.queue_size(&queue_name).await?;
    if total_size == 0 {
        println!("Queue '{queue_name}' is empty");
        return Ok(());
    }
    let start = offset as isize;
    let stop = (offset + limit).saturating_sub(1) as isize;
    let entries = store
        .queue_range_with_scores(&queue_name, start, stop)
        .await?;
    if entries.is_empty() {
        println!("No jobs found in queue {queue_name}");
        return Ok(());
    }
    println!(
        "{:<4} {:<36} {:<20} {:<10} {:<20} {:>7}",
        "#", "Job ID", "Function", "Status", "Scheduled", "Retries"
    );
    for (idx, (job_id, score)) in entries.iter().enumerate() {
        let job_map = store.get_job_data_map(job_id).await?;
        let (function, status, retries) = if let Some(map) = job_map {
            (
                map.get("function_name")
                    .cloned()
                    .unwrap_or_else(|| "<unknown>".to_string()),
                cli_utils::format_status(map.get("status").map(|s| s.as_str())),
                map.get("current_retries")
                    .or_else(|| map.get("retries"))
                    .cloned()
                    .unwrap_or_else(|| "0".to_string()),
            )
        } else {
            (
                "<missing>".to_string(),
                "MISSING".to_string(),
                "N/A".to_string(),
            )
        };
        let scheduled = format!("{}", score / 1000.0);
        println!(
            "{:<4} {:<36} {:<20} {:<10} {:<20} {:>7}",
            offset + idx + 1,
            job_id,
            cli_utils::truncate(&function, 18),
            status,
            cli_utils::format_timestamp(Some(&scheduled)),
            retries
        );
    }
    Ok(())
}

async fn count_dlq_for_queue(
    store: &mut JobStore,
    dlq_name: &str,
    queue_name: &str,
) -> Result<i64> {
    let job_ids = store.get_dlq_job_ids(dlq_name).await?;
    let mut count = 0i64;
    for job_id in job_ids {
        if let Some(job_map) = store.get_job_data_map(&job_id).await?
            && let Some(job_queue) = job_map.get("queue_name")
            && queue_matches(queue_name, job_queue)
        {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::RedisTestContext;
    use chrono::Utc;
    use rrq::{Job, JobStatus};

    fn build_job(queue_name: &str, dlq_name: &str, status: JobStatus) -> Job {
        Job {
            id: Job::new_id(),
            function_name: "do_work".to_string(),
            job_params: serde_json::Map::new(),
            enqueue_time: Utc::now(),
            start_time: None,
            status,
            current_retries: 0,
            next_scheduled_run_time: None,
            max_retries: 3,
            job_timeout_seconds: Some(30),
            result_ttl_seconds: Some(60),
            job_unique_key: None,
            completion_time: None,
            result: None,
            last_error: None,
            queue_name: Some(queue_name.to_string()),
            dlq_name: Some(dlq_name.to_string()),
            worker_id: None,
            trace_context: None,
            correlation_context: None,
        }
    }

    #[tokio::test]
    async fn queue_commands_cover_branches() -> Result<()> {
        let mut ctx = RedisTestContext::new().await?;
        let config = ctx.write_config().await?;
        let config_path = Some(config.path().to_string_lossy().to_string());
        let queue_name = ctx.settings.default_queue_name.clone();
        let dlq_name = ctx.settings.default_dlq_name.clone();

        queue_list(config_path.clone(), false).await?;

        let pending = build_job(&queue_name, &dlq_name, JobStatus::Pending);
        ctx.store.save_job_definition(&pending).await?;
        ctx.store
            .add_job_to_queue(
                &queue_name,
                &pending.id,
                Utc::now().timestamp_millis() as f64,
            )
            .await?;

        let active = build_job(&queue_name, &dlq_name, JobStatus::Active);
        ctx.store.save_job_definition(&active).await?;
        let completed = build_job(&queue_name, &dlq_name, JobStatus::Completed);
        ctx.store.save_job_definition(&completed).await?;
        let failed = build_job(&queue_name, &dlq_name, JobStatus::Failed);
        ctx.store.save_job_definition(&failed).await?;

        queue_list(config_path.clone(), true).await?;
        queue_stats(config_path.clone(), Vec::new(), 1).await?;

        queue_inspect("missing".to_string(), config_path.clone(), 10, 0).await?;
        queue_inspect(queue_name, config_path, 10, 0).await?;
        Ok(())
    }
}
