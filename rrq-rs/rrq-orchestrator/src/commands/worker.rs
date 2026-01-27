use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use notify::{Event, EventKind, RecursiveMode, Watcher, event::ModifyKind, recommended_watcher};
use tokio::sync::mpsc;
use tokio::time::Duration;

use rrq::config::{load_toml_settings, resolve_config_source};
use rrq::executor::{build_executors_from_settings, resolve_executor_pool_sizes};
use rrq::worker::RRQWorker;

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
    let shutdown_timeout =
        Duration::from_secs_f64(worker.settings().worker_shutdown_grace_period_seconds + 2.0);
    let mut handle = tokio::spawn(async move {
        let _ = worker.run().await;
        worker
    });
    tokio::select! {
        _ = wait_for_shutdown_signal() => {
            shutdown.store(true, Ordering::SeqCst);
        }
        result = &mut handle => {
            if let Err(err) = result {
                eprintln!("worker crashed: {err}");
            }
        }
    }
    let worker = match tokio::time::timeout(shutdown_timeout, &mut handle).await {
        Ok(result) => result?,
        Err(_) => {
            handle.abort();
            return Ok(());
        }
    };
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

fn load_gitignore(root: &Path) -> Result<Option<Gitignore>> {
    let mut builder = GitignoreBuilder::new(root);
    let mut added = false;
    let gitignore_path = root.join(".gitignore");
    if gitignore_path.exists() {
        added = true;
        if let Some(err) = builder.add(&gitignore_path) {
            eprintln!("failed to parse {}: {err}", gitignore_path.display());
            return Ok(None);
        }
    }
    let git_info_exclude = root.join(".git").join("info").join("exclude");
    if git_info_exclude.exists() {
        added = true;
        if let Some(err) = builder.add(&git_info_exclude) {
            eprintln!("failed to parse {}: {err}", git_info_exclude.display());
            return Ok(None);
        }
    }
    if !added {
        return Ok(None);
    }
    match builder.build() {
        Ok(gitignore) => Ok(Some(gitignore)),
        Err(err) => {
            eprintln!("failed to build gitignore matcher: {err}");
            Ok(None)
        }
    }
}

fn should_restart_for_event(
    event: &Event,
    base: &Path,
    include_set: &GlobSet,
    ignore_set: &GlobSet,
    gitignore: Option<&Gitignore>,
) -> bool {
    if matches!(event.kind, EventKind::Access(_) | EventKind::Other) {
        return false;
    }
    if matches!(event.kind, EventKind::Modify(ModifyKind::Metadata(_))) {
        return false;
    }
    if event.paths.is_empty() {
        tracing::debug!(kind = ?event.kind, "watch event had no paths; ignoring");
        return false;
    }
    for path in &event.paths {
        let rel = path.strip_prefix(base).unwrap_or(path);
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        let base_name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default();
        if let Some(gitignore) = gitignore {
            let is_dir = path.is_dir();
            if gitignore.matched(path, is_dir).is_ignore() {
                continue;
            }
        }
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
    path: Option<String>,
    include_patterns: Vec<String>,
    ignore_patterns: Vec<String>,
    no_gitignore: bool,
) -> Result<()> {
    let initial_settings = load_toml_settings(config.as_deref())?;
    let watch_settings = initial_settings.watch.clone();
    let watch_path = path
        .or(watch_settings.path)
        .unwrap_or_else(|| ".".to_string());
    let include_patterns = if include_patterns.is_empty() {
        if watch_settings.include_patterns.is_empty() {
            vec!["*.py".to_string(), "*.toml".to_string()]
        } else {
            watch_settings.include_patterns
        }
    } else {
        include_patterns
    };
    let ignore_patterns = if ignore_patterns.is_empty() {
        if watch_settings.ignore_patterns.is_empty() {
            vec![
                ".git".to_string(),
                ".git/**".to_string(),
                ".venv".to_string(),
                ".venv/**".to_string(),
                "target".to_string(),
                "target/**".to_string(),
                "dist".to_string(),
                "dist/**".to_string(),
                "build".to_string(),
                "build/**".to_string(),
                "__pycache__".to_string(),
                "**/__pycache__".to_string(),
                "**/__pycache__/**".to_string(),
                "*.pyc".to_string(),
                "**/*.pyc".to_string(),
                ".ruff_cache".to_string(),
                ".ruff_cache/**".to_string(),
                ".pytest_cache".to_string(),
                ".pytest_cache/**".to_string(),
            ]
        } else {
            watch_settings.ignore_patterns
        }
    } else {
        ignore_patterns
    };
    let no_gitignore = no_gitignore || watch_settings.no_gitignore.unwrap_or(false);

    let watch_root =
        std::fs::canonicalize(&watch_path).unwrap_or_else(|_| PathBuf::from(&watch_path));
    let (tx, mut rx) = mpsc::channel(32);
    let mut watcher = recommended_watcher(move |res| {
        let _ = tx.blocking_send(res);
    })?;
    watcher.watch(&watch_root, RecursiveMode::Recursive)?;
    println!("Watching {} for changes...", watch_root.display());
    let include_set = build_globset(&include_patterns)?;
    let ignore_set = build_globset(&ignore_patterns)?;
    let gitignore = if no_gitignore {
        None
    } else {
        load_gitignore(&watch_root)?
    };
    let mut cached_settings = Some(initial_settings);

    loop {
        let mut restart = false;
        let mut exit_loop = false;
        let (resolved, source) = resolve_config_source(config.as_deref());
        if let Some(path) = resolved.as_deref() {
            println!("Loading RRQ settings from {source} ({path}).");
        } else {
            println!("missing RRQ config (provide --config or RRQ_CONFIG).");
        }
        let settings = match cached_settings.take() {
            Some(settings) => settings,
            None => load_toml_settings(config.as_deref())?,
        };
        let pool_sizes = resolve_executor_pool_sizes(&settings, true, None)?;
        let effective_concurrency = std::cmp::max(1, pool_sizes.values().sum());
        let mut settings = settings;
        settings.worker_concurrency = effective_concurrency;
        settings.worker_shutdown_grace_period_seconds = 0.0;
        let executors = build_executors_from_settings(&settings, Some(&pool_sizes)).await?;
        let queue_arg = if queues.is_empty() {
            None
        } else {
            Some(queues.clone())
        };
        let mut worker = RRQWorker::new(settings, queue_arg, None, executors, false).await?;
        let shutdown = worker.shutdown_handle();
        let shutdown_timeout = Duration::from_secs(2);
        let mut handle = tokio::spawn(async move {
            let _ = worker.run().await;
            worker
        });
        loop {
            tokio::select! {
                _ = wait_for_shutdown_signal() => {
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
                            if should_restart_for_event(
                                &event,
                                &watch_root,
                                &include_set,
                                &ignore_set,
                                gitignore.as_ref(),
                            ) {
                                println!(
                                    "Change detected (event={:?}, paths={:?}).",
                                    event.kind, event.paths
                                );
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
            if restart || exit_loop {
                break;
            }
        }
        let worker = match tokio::time::timeout(shutdown_timeout, &mut handle).await {
            Ok(result) => result?,
            Err(_) => {
                handle.abort();
                if exit_loop {
                    break;
                }
                while tokio::time::timeout(Duration::from_millis(200), rx.recv())
                    .await
                    .ok()
                    .flatten()
                    .is_some()
                {}
                eprintln!("worker shutdown timed out; restarting...");
                continue;
            }
        };
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

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let sigint = signal(SignalKind::interrupt());
        let sigterm = signal(SignalKind::terminate());
        match (sigint, sigterm) {
            (Ok(mut sigint), Ok(mut sigterm)) => {
                tokio::select! {
                    _ = sigint.recv() => {}
                    _ = sigterm.recv() => {}
                }
            }
            _ => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
