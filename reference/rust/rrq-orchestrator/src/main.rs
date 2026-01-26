use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::time::{Duration as StdDuration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{recommended_watcher, Event, RecursiveMode, Watcher};
use rand::prelude::IndexedRandom;
use rand::Rng;
use serde_json::{json, Value};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::Duration;

use rrq_orchestrator::config::{load_toml_settings, resolve_config_source};
use rrq_orchestrator::constants::{HEALTH_KEY_PREFIX, JOB_KEY_PREFIX, QUEUE_KEY_PREFIX};
use rrq_orchestrator::executor::{build_executors_from_settings, resolve_executor_pool_sizes};
use rrq_orchestrator::worker::RRQWorker;
use rrq_orchestrator::{EnqueueOptions, Job, JobStatus, JobStore, RRQClient};

mod cli_utils;

#[derive(Parser)]
#[command(name = "rrq")]
#[command(about = "RRQ Rust orchestrator", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    #[command(alias = "health")]
    Check {
        #[arg(long)]
        config: Option<String>,
    },
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    Dlq {
        #[command(subcommand)]
        command: DlqCommand,
    },
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
    Executor {
        #[command(subcommand)]
        command: ExecutorCommand,
    },
}

#[derive(Subcommand)]
enum WorkerCommand {
    Run {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        queue: Vec<String>,
        #[arg(long, default_value_t = false)]
        burst: bool,
    },
    Watch {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        queue: Vec<String>,
        #[arg(long, default_value = ".")]
        path: String,
        #[arg(long, action = clap::ArgAction::Append)]
        pattern: Vec<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        ignore_pattern: Vec<String>,
    },
}

#[derive(Subcommand)]
enum QueueCommand {
    List {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = false)]
        show_empty: bool,
    },
    Stats {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        queue: Vec<String>,
        #[arg(long, default_value_t = 1000)]
        max_scan: usize,
    },
    Inspect {
        queue_name: String,
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, default_value_t = 0)]
        offset: usize,
    },
}

#[derive(Subcommand)]
enum JobCommand {
    Show {
        job_id: String,
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = false)]
        raw: bool,
    },
    List {
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        function: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    Replay {
        job_id: String,
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        queue: Option<String>,
    },
    Cancel {
        job_id: String,
        #[arg(long)]
        config: Option<String>,
    },
    Trace {
        job_id: String,
        #[arg(long)]
        config: Option<String>,
    },
}

#[derive(Subcommand)]
enum DlqCommand {
    List {
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        function: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, default_value_t = 0)]
        offset: usize,
        #[arg(long)]
        dlq_name: Option<String>,
        #[arg(long, default_value_t = false)]
        raw: bool,
        #[arg(long, default_value_t = 100)]
        batch_size: usize,
    },
    Stats {
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        dlq_name: Option<String>,
    },
    Inspect {
        job_id: String,
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = false)]
        raw: bool,
    },
    Requeue {
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        dlq_name: Option<String>,
        #[arg(long)]
        target_queue: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        function: Option<String>,
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        all: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum DebugCommand {
    GenerateJobs {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = 100)]
        count: usize,
        #[arg(long, action = clap::ArgAction::Append)]
        queue: Vec<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        status: Vec<String>,
        #[arg(long, default_value_t = 24)]
        age_hours: i64,
        #[arg(long, default_value_t = 10)]
        batch_size: usize,
    },
    GenerateWorkers {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = 5)]
        count: usize,
        #[arg(long, default_value_t = 60)]
        duration: u64,
    },
    Submit {
        function_name: String,
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        args: Option<String>,
        #[arg(long)]
        kwargs: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        delay: Option<i64>,
    },
    Clear {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = false)]
        confirm: bool,
        #[arg(long, default_value = "test_*")]
        pattern: String,
    },
    StressTest {
        #[arg(long)]
        config: Option<String>,
        #[arg(long, default_value_t = 10)]
        jobs_per_second: u64,
        #[arg(long, default_value_t = 60)]
        duration: u64,
        #[arg(long, action = clap::ArgAction::Append)]
        queues: Vec<String>,
    },
}

#[derive(Subcommand)]
enum ExecutorCommand {
    Python {
        #[arg(long)]
        settings: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Worker { command } => match command {
            WorkerCommand::Run {
                config,
                queue,
                burst,
            } => {
                run_worker(config, queue, burst, false).await?;
            }
            WorkerCommand::Watch {
                config,
                queue,
                path,
                pattern,
                ignore_pattern,
            } => {
                run_worker_watch(config, queue, path, pattern, ignore_pattern).await?;
            }
        },
        Commands::Check { config } => {
            check_workers(config).await?;
        }
        Commands::Queue { command } => match command {
            QueueCommand::List { config, show_empty } => {
                queue_list(config, show_empty).await?;
            }
            QueueCommand::Stats {
                config,
                queue,
                max_scan,
            } => {
                queue_stats(config, queue, max_scan).await?;
            }
            QueueCommand::Inspect {
                queue_name,
                config,
                limit,
                offset,
            } => {
                queue_inspect(queue_name, config, limit, offset).await?;
            }
        },
        Commands::Job { command } => match command {
            JobCommand::Show {
                job_id,
                config,
                raw,
            } => {
                job_show(job_id, config, raw).await?;
            }
            JobCommand::List {
                config,
                status,
                queue,
                function,
                limit,
            } => {
                job_list(config, status, queue, function, limit).await?;
            }
            JobCommand::Replay {
                job_id,
                config,
                queue,
            } => {
                job_replay(job_id, config, queue).await?;
            }
            JobCommand::Cancel { job_id, config } => {
                job_cancel(job_id, config).await?;
            }
            JobCommand::Trace { job_id, config } => {
                job_trace(job_id, config).await?;
            }
        },
        Commands::Dlq { command } => match command {
            DlqCommand::List {
                config,
                queue,
                function,
                limit,
                offset,
                dlq_name,
                raw,
                batch_size,
            } => {
                dlq_list(DlqListOptions {
                    config,
                    queue,
                    function,
                    limit,
                    offset,
                    dlq_name,
                    raw,
                    batch_size,
                })
                .await?;
            }
            DlqCommand::Stats { config, dlq_name } => {
                dlq_stats(config, dlq_name).await?;
            }
            DlqCommand::Inspect {
                job_id,
                config,
                raw,
            } => {
                dlq_inspect(job_id, config, raw).await?;
            }
            DlqCommand::Requeue {
                config,
                dlq_name,
                target_queue,
                queue,
                function,
                job_id,
                limit,
                all,
                dry_run,
            } => {
                dlq_requeue(DlqRequeueOptions {
                    config,
                    dlq_name,
                    target_queue,
                    queue,
                    function,
                    job_id,
                    limit,
                    all,
                    dry_run,
                })
                .await?;
            }
        },
        Commands::Debug { command } => match command {
            DebugCommand::GenerateJobs {
                config,
                count,
                queue,
                status,
                age_hours,
                batch_size,
            } => {
                debug_generate_jobs(config, count, queue, status, age_hours, batch_size).await?;
            }
            DebugCommand::GenerateWorkers {
                config,
                count,
                duration,
            } => {
                debug_generate_workers(config, count, duration).await?;
            }
            DebugCommand::Submit {
                function_name,
                config,
                args,
                kwargs,
                queue,
                delay,
            } => {
                debug_submit(function_name, config, args, kwargs, queue, delay).await?;
            }
            DebugCommand::Clear {
                config,
                confirm,
                pattern,
            } => {
                debug_clear(config, confirm, pattern).await?;
            }
            DebugCommand::StressTest {
                config,
                jobs_per_second,
                duration,
                queues,
            } => {
                debug_stress_test(config, jobs_per_second, duration, queues).await?;
            }
        },
        Commands::Executor { command } => match command {
            ExecutorCommand::Python { settings } => {
                executor_python(settings).await?;
            }
        },
    }
    Ok(())
}

async fn run_worker(
    config: Option<String>,
    queues: Vec<String>,
    burst: bool,
    watch_mode: bool,
) -> Result<()> {
    let (resolved, source) = resolve_config_source(config.as_deref());
    if let Some(path) = resolved.as_deref() {
        println!("Loading RRQ settings from {source} ({path}).");
    } else {
        println!("missing RRQ config (provide --config or RRQ_CONFIG).");
    }
    let settings = load_toml_settings(config.as_deref())?;
    let pool_sizes = resolve_executor_pool_sizes(&settings, watch_mode, None)?;
    let effective_concurrency = std::cmp::max(1, pool_sizes.values().sum());
    let mut settings = settings;
    settings.worker_concurrency = effective_concurrency;
    let executors = build_executors_from_settings(&settings, Some(&pool_sizes)).await?;
    let queues = if queues.is_empty() {
        None
    } else {
        Some(queues)
    };
    let worker = RRQWorker::new(settings, queues, None, executors, burst).await?;
    run_worker_loop(worker).await?;
    Ok(())
}

async fn run_worker_loop(mut worker: RRQWorker) -> Result<()> {
    let shutdown = worker.shutdown_handle();
    let mut handle = tokio::spawn(async move {
        let _ = worker.run().await;
        worker
    });
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            shutdown.store(true, Ordering::SeqCst);
        }
        result = &mut handle => {
            if let Err(err) = result {
                eprintln!("worker crashed: {err}");
            }
        }
    }
    let worker = handle.await?;
    worker.close_executors().await;
    Ok(())
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob =
            Glob::new(pattern).with_context(|| format!("invalid glob pattern '{pattern}'"))?;
        builder.add(glob);
    }
    Ok(builder.build()?)
}

fn should_restart_for_event(
    event: &Event,
    base: &Path,
    include_set: &GlobSet,
    ignore_set: &GlobSet,
) -> bool {
    if event.paths.is_empty() {
        return true;
    }
    for path in &event.paths {
        let rel = path.strip_prefix(base).unwrap_or(path);
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        let base_name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default();
        if ignore_set.is_match(&rel_str)
            || (!base_name.is_empty() && ignore_set.is_match(&base_name))
        {
            continue;
        }
        if include_set.is_empty() {
            return true;
        }
        if include_set.is_match(&rel_str)
            || (!base_name.is_empty() && include_set.is_match(&base_name))
        {
            return true;
        }
    }
    false
}

async fn run_worker_watch(
    config: Option<String>,
    queues: Vec<String>,
    path: String,
    include_patterns: Vec<String>,
    ignore_patterns: Vec<String>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(32);
    let mut watcher = recommended_watcher(move |res| {
        let _ = tx.blocking_send(res);
    })?;
    watcher.watch(Path::new(&path), RecursiveMode::Recursive)?;
    println!("Watching {} for changes...", path);

    let include_patterns = if include_patterns.is_empty() {
        vec!["*.py".to_string(), "*.toml".to_string()]
    } else {
        include_patterns
    };
    let include_set = build_globset(&include_patterns)?;
    let ignore_set = build_globset(&ignore_patterns)?;
    let watch_root = PathBuf::from(&path);

    loop {
        let mut restart = false;
        let mut exit_loop = false;
        let (resolved, source) = resolve_config_source(config.as_deref());
        if let Some(path) = resolved.as_deref() {
            println!("Loading RRQ settings from {source} ({path}).");
        } else {
            println!("missing RRQ config (provide --config or RRQ_CONFIG).");
        }
        let settings = load_toml_settings(config.as_deref())?;
        let pool_sizes = resolve_executor_pool_sizes(&settings, true, None)?;
        let effective_concurrency = std::cmp::max(1, pool_sizes.values().sum());
        let mut settings = settings;
        settings.worker_concurrency = effective_concurrency;
        let executors = build_executors_from_settings(&settings, Some(&pool_sizes)).await?;
        let queue_arg = if queues.is_empty() {
            None
        } else {
            Some(queues.clone())
        };
        let mut worker = RRQWorker::new(settings, queue_arg, None, executors, false).await?;
        let shutdown = worker.shutdown_handle();
        let mut handle = tokio::spawn(async move {
            let _ = worker.run().await;
            worker
        });
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                shutdown.store(true, Ordering::SeqCst);
                exit_loop = true;
            }
            result = &mut handle => {
                if let Err(err) = result {
                    eprintln!("worker crashed: {err}");
                }
                exit_loop = true;
            }
            Some(event) = rx.recv() => {
                match event {
                    Ok(event) => {
                        if should_restart_for_event(&event, &watch_root, &include_set, &ignore_set) {
                            shutdown.store(true, Ordering::SeqCst);
                            restart = true;
                        }
                    }
                    Err(err) => {
                        eprintln!("watch error: {err}");
                        shutdown.store(true, Ordering::SeqCst);
                        restart = true;
                    }
                }
            }
        }
        let worker = handle.await?;
        worker.close_executors().await;
        if exit_loop {
            break;
        }
        if !restart {
            break;
        }
        while tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .ok()
            .flatten()
            .is_some()
        {}
        println!("Restarting worker...");
    }

    Ok(())
}

async fn check_workers(config: Option<String>) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let mut cursor = 0u64;
    let mut keys: Vec<String> = Vec::new();
    loop {
        let (next, batch) = store.scan_worker_health_keys(cursor, 100).await?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    if keys.is_empty() {
        println!("Worker Health Check: FAIL (No active workers found)");
        return Ok(());
    }
    println!(
        "Worker Health Check: Found {} active worker(s):",
        keys.len()
    );
    for key in keys {
        let worker_id = key.trim_start_matches(HEALTH_KEY_PREFIX);
        let (health, ttl) = store.get_worker_health(worker_id).await?;
        if let Some(health) = health {
            println!("  - Worker ID: {worker_id}");
            if let Some(status) = health.get("status") {
                println!("    Status: {status}");
            }
            if let Some(active_jobs) = health.get("active_jobs") {
                println!("    Active Jobs: {active_jobs}");
            }
            if let Some(timestamp) = health.get("timestamp") {
                println!("    Last Heartbeat: {timestamp}");
            }
            println!("    TTL: {} seconds", ttl.unwrap_or(0));
        } else {
            println!("  - Worker ID: {worker_id} - Health data missing");
        }
    }
    Ok(())
}

async fn queue_list(config: Option<String>, show_empty: bool) -> Result<()> {
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

async fn queue_stats(config: Option<String>, queues: Vec<String>, max_scan: usize) -> Result<()> {
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

async fn queue_inspect(
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

async fn job_show(job_id: String, config: Option<String>, raw: bool) -> Result<()> {
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

    let status = job_map
        .get("status")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let function_name = job_map
        .get("function_name")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let queue_name = job_map
        .get("queue_name")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    println!("Job ID: {job_id}");
    println!("Status: {}", cli_utils::format_status(Some(&status)));
    println!("Function: {function_name}");
    println!("Queue: {queue_name}");
    if let Some(enqueue_time) = job_map.get("enqueue_time") {
        println!(
            "Enqueued: {}",
            cli_utils::format_timestamp(Some(enqueue_time))
        );
    }
    if let Some(start_time) = job_map.get("start_time") {
        println!("Started: {}", cli_utils::format_timestamp(Some(start_time)));
    }
    if let Some(completion_time) = job_map.get("completion_time") {
        println!(
            "Completed: {}",
            cli_utils::format_timestamp(Some(completion_time))
        );
        let duration = match (job_map.get("start_time"), job_map.get("completion_time")) {
            (Some(start), Some(end)) => {
                let start_ts = cli_utils::parse_timestamp(start).unwrap_or(0.0);
                let end_ts = cli_utils::parse_timestamp(end).unwrap_or(0.0);
                Some(end_ts - start_ts)
            }
            _ => None,
        };
        if duration.is_some() {
            println!("Duration: {}", cli_utils::format_duration(duration));
        }
    }
    let retries = job_map
        .get("current_retries")
        .or_else(|| job_map.get("retries"))
        .cloned()
        .unwrap_or_else(|| "0".to_string());
    let max_retries = job_map
        .get("max_retries")
        .cloned()
        .unwrap_or_else(|| "0".to_string());
    println!("Retries: {retries}/{max_retries}");

    if let Some(args) = job_map.get("job_args").or_else(|| job_map.get("args")) {
        if let Some(parsed) = cli_utils::parse_json(Some(args)) {
            if parsed != Value::Null {
                println!("Args: {}", serde_json::to_string_pretty(&parsed)?);
            }
        }
    }
    if let Some(kwargs) = job_map.get("job_kwargs").or_else(|| job_map.get("kwargs")) {
        if let Some(parsed) = cli_utils::parse_json(Some(kwargs)) {
            if parsed != Value::Null {
                println!("Kwargs: {}", serde_json::to_string_pretty(&parsed)?);
            }
        }
    }

    if status.eq_ignore_ascii_case("completed") {
        if let Some(result) = job_map.get("result") {
            if let Some(parsed) = cli_utils::parse_json(Some(result)) {
                println!("Result: {}", serde_json::to_string_pretty(&parsed)?);
            } else {
                println!("Result: {result}");
            }
        }
    } else if status.eq_ignore_ascii_case("failed") || status.eq_ignore_ascii_case("retrying") {
        if let Some(error) = job_map.get("last_error").or_else(|| job_map.get("error")) {
            println!("Error: {error}");
        }
    }

    Ok(())
}

async fn job_list(
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

async fn job_replay(job_id: String, config: Option<String>, queue: Option<String>) -> Result<()> {
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

async fn job_cancel(job_id: String, config: Option<String>) -> Result<()> {
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

async fn job_trace(job_id: String, config: Option<String>) -> Result<()> {
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

    if let Some(completion) = job_map
        .get("completion_time")
        .and_then(|v| cli_utils::parse_timestamp(v))
    {
        let status = job_map
            .get("status")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        if status.eq_ignore_ascii_case("completed") {
            events.push(("Completed".to_string(), completion, "Success".to_string()));
        } else {
            let error_msg = job_map
                .get("last_error")
                .or_else(|| job_map.get("error"))
                .cloned()
                .unwrap_or_else(|| "Unknown error".to_string());
            events.push((
                "Failed".to_string(),
                completion,
                cli_utils::truncate(&error_msg, 50),
            ));
        }
    }

    events.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    println!("{:<12} {:<20} Details", "Event", "Timestamp");
    let mut prev = None;
    for (event, ts, details) in &events {
        let time_val = cli_utils::to_utc_rfc3339(*ts);
        let mut time_str = cli_utils::format_timestamp(Some(&time_val));
        if let Some(prev_ts) = prev {
            let diff = ts - prev_ts;
            time_str = format!("{} (+{})", time_str, cli_utils::format_duration(Some(diff)));
        }
        println!("{:<12} {:<20} {}", event, time_str, details);
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

struct DlqListOptions {
    config: Option<String>,
    queue: Option<String>,
    function: Option<String>,
    limit: usize,
    offset: usize,
    dlq_name: Option<String>,
    raw: bool,
    batch_size: usize,
}

async fn dlq_list(options: DlqListOptions) -> Result<()> {
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
            if let Some(filter) = options.function.as_deref() {
                if job_map.get("function_name").map(|v| v.as_str()) != Some(filter) {
                    continue;
                }
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

async fn dlq_stats(config: Option<String>, dlq_name: Option<String>) -> Result<()> {
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
        completion_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
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

    println!("\nTop queues:");
    for (queue, count) in top_counts(&by_queue, 10) {
        println!("  {queue}: {count}");
    }
    println!("\nTop functions:");
    for (func, count) in top_counts(&by_function, 10) {
        println!("  {func}: {count}");
    }
    println!("\nTop errors:");
    for (err, count) in top_counts(&error_counts, 10) {
        println!("  {err}: {count}");
    }

    Ok(())
}

async fn dlq_inspect(job_id: String, config: Option<String>, raw: bool) -> Result<()> {
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

struct DlqRequeueOptions {
    config: Option<String>,
    dlq_name: Option<String>,
    target_queue: Option<String>,
    queue: Option<String>,
    function: Option<String>,
    job_id: Option<String>,
    limit: Option<usize>,
    all: bool,
    dry_run: bool,
}

async fn dlq_requeue(options: DlqRequeueOptions) -> Result<()> {
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
            if let Some(filter) = options.job_id.as_deref() {
                if id != filter {
                    continue;
                }
            }
            if let Some(filter) = options.queue.as_deref() {
                let job_queue = job_map.get("queue_name").cloned().unwrap_or_default();
                if !queue_matches(filter, &job_queue) {
                    continue;
                }
            }
            if let Some(filter) = options.function.as_deref() {
                if job_map.get("function_name").map(|v| v.as_str()) != Some(filter) {
                    continue;
                }
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

async fn debug_generate_jobs(
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

async fn debug_generate_workers(config: Option<String>, count: usize, duration: u64) -> Result<()> {
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

async fn debug_submit(
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

async fn debug_clear(config: Option<String>, confirm: bool, pattern: String) -> Result<()> {
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
        std::io::stdin().read_line(&mut input).ok();
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

async fn debug_stress_test(
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

async fn executor_python(settings: Option<String>) -> Result<()> {
    let mut cmd = Command::new("rrq-executor");
    if let Some(settings) = settings {
        cmd.arg("--settings").arg(settings);
    }
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = cmd.status().await?;
    if !status.success() {
        anyhow::bail!("rrq-executor exited with status {status}");
    }
    Ok(())
}

fn queue_matches(filter: &str, job_queue: &str) -> bool {
    if filter == job_queue {
        return true;
    }
    if job_queue == format!("{}{}", QUEUE_KEY_PREFIX, filter) {
        return true;
    }
    false
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

fn top_counts(map: &HashMap<String, usize>, limit: usize) -> Vec<(String, usize)> {
    let mut items = map.iter().map(|(k, v)| (k.clone(), *v)).collect::<Vec<_>>();
    items.sort_by(|a, b| b.1.cmp(&a.1));
    items.into_iter().take(limit).collect()
}
