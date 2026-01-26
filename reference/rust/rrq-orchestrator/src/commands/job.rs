use std::collections::HashMap;

use anyhow::Result;
use serde_json::Value;

use crate::cli_utils;
use rrq_orchestrator::config::load_toml_settings;
use rrq_orchestrator::constants::JOB_KEY_PREFIX;
use rrq_orchestrator::store::JobStore;
use rrq_orchestrator::{EnqueueOptions, JobStatus, RRQClient};

use super::shared::queue_matches;

pub(crate) async fn job_show(job_id: String, config: Option<String>, raw: bool) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let job_map = store.get_job_data_map(&job_id).await?;
    let Some(job_map) = job_map else {
        println!("Job '{job_id}' not found");
        return Ok(());
    };

    if raw {
        let json = serde_json::to_string_pretty(&job_map)?;
        println!("{json}");
        return Ok(());
    }

    let mut fields: Vec<(String, String)> = job_map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    println!("Job {job_id}");
    for (key, value) in fields {
        println!("{:<24} {}", key, value);
    }
    Ok(())
}

pub(crate) async fn job_list(
    config: Option<String>,
    status: Option<String>,
    queue: Option<String>,
    function: Option<String>,
    limit: usize,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let mut job_keys = Vec::new();
    let mut cursor = 0u64;
    loop {
        let (next, keys) = store.scan_job_keys(cursor, 200).await?;
        job_keys.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    if job_keys.is_empty() {
        println!("No jobs found");
        return Ok(());
    }

    let mut jobs: Vec<(String, HashMap<String, String>)> = Vec::new();
    for key in job_keys {
        let job_id = key.trim_start_matches(JOB_KEY_PREFIX).to_string();
        if let Some(job_map) = store.get_job_data_map_by_key(&key).await? {
            if let Some(filter) = status.as_deref() {
                if job_map
                    .get("status")
                    .map(|value| value.eq_ignore_ascii_case(filter))
                    != Some(true)
                {
                    continue;
                }
            }
            if let Some(filter) = queue.as_deref() {
                let job_queue = job_map.get("queue_name").cloned().unwrap_or_default();
                if !queue_matches(filter, &job_queue) {
                    continue;
                }
            }
            if let Some(filter) = function.as_deref() {
                if job_map.get("function_name").map(|value| value.as_str()) != Some(filter) {
                    continue;
                }
            }
            jobs.push((job_id, job_map));
        }
    }

    jobs.sort_by(|a, b| {
        let a_ts =
            a.1.get("enqueue_time")
                .and_then(|value| cli_utils::parse_timestamp(value))
                .unwrap_or(0.0);
        let b_ts =
            b.1.get("enqueue_time")
                .and_then(|value| cli_utils::parse_timestamp(value))
                .unwrap_or(0.0);
        b_ts.partial_cmp(&a_ts).unwrap_or(std::cmp::Ordering::Equal)
    });

    let jobs = jobs.into_iter().take(limit).collect::<Vec<_>>();

    println!(
        "{:<12} {:<30} {:<24} {:<10} {:<18} {:>10}",
        "Job ID", "Function", "Queue", "Status", "Enqueued", "Duration"
    );
    for (job_id, job_map) in jobs {
        let function_name = job_map
            .get("function_name")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let queue_name = job_map
            .get("queue_name")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let status = cli_utils::format_status(job_map.get("status").map(|value| value.as_str()));
        let enqueue =
            cli_utils::format_timestamp(job_map.get("enqueue_time").map(|value| value.as_str()));
        let duration = match (
            job_map.get("start_time"),
            job_map.get("completion_time"),
            job_map.get("status"),
        ) {
            (Some(start), Some(end), _) => {
                let start_ts = cli_utils::parse_timestamp(start).unwrap_or(0.0);
                let end_ts = cli_utils::parse_timestamp(end).unwrap_or(0.0);
                cli_utils::format_duration(Some(end_ts - start_ts))
            }
            (Some(start), None, Some(status)) if status.eq_ignore_ascii_case("active") => {
                let start_ts = cli_utils::parse_timestamp(start).unwrap_or(0.0);
                let now = chrono::Utc::now().timestamp() as f64;
                cli_utils::format_duration(Some(now - start_ts))
            }
            _ => "N/A".to_string(),
        };
        println!(
            "{:<12} {:<30} {:<24} {:<10} {:<18} {:>10}",
            format!("{}...", &job_id[..std::cmp::min(8, job_id.len())]),
            cli_utils::truncate(&function_name, 28),
            cli_utils::truncate(&queue_name, 22),
            status,
            enqueue,
            duration
        );
    }

    Ok(())
}

pub(crate) async fn job_replay(
    job_id: String,
    config: Option<String>,
    queue: Option<String>,
) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let Some(job_map) = store.get_job_data_map(&job_id).await? else {
        println!("Job '{job_id}' not found");
        return Ok(());
    };

    let function_name = job_map
        .get("function_name")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("job missing function_name"))?;
    let args = cli_utils::parse_json(job_map.get("job_args").map(|v| v.as_str()))
        .or_else(|| cli_utils::parse_json(job_map.get("args").map(|v| v.as_str())))
        .unwrap_or(Value::Array(Vec::new()));
    let kwargs = cli_utils::parse_json(job_map.get("job_kwargs").map(|v| v.as_str()))
        .or_else(|| cli_utils::parse_json(job_map.get("kwargs").map(|v| v.as_str())))
        .unwrap_or(Value::Object(serde_json::Map::new()));

    let queue_name = queue
        .or_else(|| job_map.get("queue_name").cloned())
        .unwrap_or_else(|| settings.default_queue_name.clone());

    let mut client = RRQClient::new(settings.clone(), store.clone());
    let args_vec = match args {
        Value::Array(values) => values,
        _ => Vec::new(),
    };
    let kwargs_map = match kwargs {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    let job = client
        .enqueue(
            &function_name,
            args_vec,
            kwargs_map,
            EnqueueOptions {
                queue_name: Some(queue_name.clone()),
                ..Default::default()
            },
        )
        .await?;
    println!("Job replayed with new ID: {}", job.id);
    Ok(())
}

pub(crate) async fn job_cancel(job_id: String, config: Option<String>) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let Some(job_map) = store.get_job_data_map(&job_id).await? else {
        println!("Job '{job_id}' not found");
        return Ok(());
    };
    let status = job_map.get("status").cloned().unwrap_or_default();
    if !status.eq_ignore_ascii_case("pending") {
        println!("Can only cancel pending jobs. Job status: {status}");
        return Ok(());
    }
    let queue_name = job_map.get("queue_name").cloned().unwrap_or_default();
    if queue_name.is_empty() {
        println!("Job has no associated queue");
        return Ok(());
    }
    let removed = store.remove_job_from_queue(&queue_name, &job_id).await?;
    if removed > 0 {
        store
            .update_job_status(&job_id, JobStatus::Cancelled)
            .await?;
        println!("Job '{job_id}' cancelled successfully");
    } else {
        println!("Failed to remove job from queue");
    }
    Ok(())
}

pub(crate) async fn job_trace(job_id: String, config: Option<String>) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings).await?;
    let Some(job_map) = store.get_job_data_map(&job_id).await? else {
        println!("Job '{job_id}' not found");
        return Ok(());
    };
    let mut events: Vec<(String, f64, String)> = Vec::new();

    if let Some(enqueue) = job_map
        .get("enqueue_time")
        .and_then(|v| cli_utils::parse_timestamp(v))
    {
        let function_name = job_map
            .get("function_name")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        events.push((
            "Enqueued".to_string(),
            enqueue,
            format!("Function: {function_name}"),
        ));
    }

    if let Some(start) = job_map
        .get("start_time")
        .and_then(|v| cli_utils::parse_timestamp(v))
    {
        let worker_id = job_map
            .get("worker_id")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        events.push(("Started".to_string(), start, format!("Worker: {worker_id}")));
    }

    let retries = job_map
        .get("current_retries")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    for i in 0..retries {
        let key = format!("retry_{i}_at");
        if let Some(ts) = job_map
            .get(&key)
            .and_then(|v| cli_utils::parse_timestamp(v))
        {
            let max_retries = job_map
                .get("max_retries")
                .cloned()
                .unwrap_or_else(|| "0".to_string());
            events.push((
                format!("Retry {}", i + 1),
                ts,
                format!("Attempt {} of {max_retries}", i + 1),
            ));
        }
    }

    if let Some(end) = job_map
        .get("completion_time")
        .and_then(|v| cli_utils::parse_timestamp(v))
    {
        let status = job_map
            .get("status")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        events.push(("Finished".to_string(), end, format!("Status: {status}")));
    }

    events.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    if events.is_empty() {
        println!("No trace data found for job {job_id}");
        return Ok(());
    }

    println!("Job trace for {job_id}");
    println!("{:<12} {:<20} Details", "Event", "Timestamp");
    let mut prev: Option<f64> = None;
    for (event, ts, details) in &events {
        let time_str = cli_utils::to_utc_rfc3339(*ts);
        let mut gap = String::new();
        if let Some(prev) = prev {
            gap = format!(" (+{})", cli_utils::format_duration(Some(ts - prev)));
        }
        println!("{:<12} {:<20} {}{}", event, time_str, details, gap);
        prev = Some(*ts);
    }
    if let (Some(first), Some(last)) = (events.first(), events.last()) {
        let duration = last.1 - first.1;
        println!(
            "\nTotal Duration: {}",
            cli_utils::format_duration(Some(duration))
        );
    }

    Ok(())
}
