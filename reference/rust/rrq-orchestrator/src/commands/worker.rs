use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{recommended_watcher, Event, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::time::Duration;

use rrq_orchestrator::config::{load_toml_settings, resolve_config_source};
use rrq_orchestrator::executor::{build_executors_from_settings, resolve_executor_pool_sizes};
use rrq_orchestrator::worker::RRQWorker;

pub(crate) async fn run_worker(
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

pub(crate) async fn run_worker_watch(
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
