use anyhow::Result;

use crate::cli_utils;
use rrq_orchestrator::config::load_toml_settings;
use rrq_orchestrator::constants::QUEUE_KEY_PREFIX;
use rrq_orchestrator::store::JobStore;

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
        let queue_name = key.trim_start_matches(QUEUE_KEY_PREFIX);
        let size = store.queue_size(queue_name).await?;
        if size == 0 && !show_empty {
            continue;
        }
        total += size;
        let oldest = store.queue_range_with_scores(queue_name, 0, 0).await?;
        let newest = store.queue_range_with_scores(queue_name, -1, -1).await?;
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
    let mut queue_names = queues;
    if queue_names.is_empty() {
        let mut cursor = 0u64;
        loop {
            let (next, batch) = store.scan_queue_keys(cursor, 200).await?;
            for key in batch {
                let name = key.trim_start_matches(QUEUE_KEY_PREFIX);
                queue_names.push(name.to_string());
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
    }
    if queue_names.is_empty() {
        println!("No queues found");
        return Ok(());
    }

    println!(
        "{:<25} {:>8} {:>8} {:>8} {:>9} {:>8} {:>10} {:>12} {:>10}",
        "Queue",
        "Total",
        "Pending",
        "Active",
        "Completed",
        "Failed",
        "DLQ",
        "Avg Wait",
        "Throughput"
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
                let now = chrono::Utc::now().timestamp() as f64;
                let mut sum = 0.0;
                for (_, score) in &entries {
                    sum += now - score;
                }
                Some(sum / entries.len() as f64)
            }
        } else {
            None
        };

        let dlq_jobs =
            count_dlq_for_queue(&mut store, &settings.default_dlq_name, &queue_name).await?;

        println!(
            "{:<25} {:>8} {:>8} {:>8} {:>9} {:>8} {:>10} {:>12} {:>10}",
            queue_name,
            total,
            pending,
            active,
            completed,
            failed,
            dlq_jobs,
            cli_utils::format_duration(avg_wait),
            "0.0/min"
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
        "{:<4} {:<20} {:<20} {:<10} {:<20} {:>7}",
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
            "{:<4} {:<20} {:<20} {:<10} {:<20} {:>7}",
            offset + idx + 1,
            format!("{}...", &job_id[..std::cmp::min(8, job_id.len())]),
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
        if let Some(job_map) = store.get_job_data_map(&job_id).await? {
            if let Some(job_queue) = job_map.get("queue_name") {
                if queue_matches(queue_name, job_queue) {
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}
