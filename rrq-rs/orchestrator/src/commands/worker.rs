use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use notify::{Event, EventKind, RecursiveMode, Watcher, event::ModifyKind, recommended_watcher};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::{process::Command, time::timeout};

use rrq::runner::{
    build_runners_from_settings_filtered, determine_needed_runners, resolve_runner_max_in_flight,
    resolve_runner_pool_sizes,
};
use rrq::worker::RRQWorker;
use rrq::{load_toml_settings, resolve_config_source};

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

    // Determine which runners are needed based on the queues being listened to
    let queues_slice = if queues.is_empty() {
        None
    } else {
        Some(queues.as_slice())
    };
    let needed_runners = determine_needed_runners(&settings, queues_slice);

    let pool_sizes = resolve_runner_pool_sizes(&settings, watch_mode, None)?;
    let max_in_flight = resolve_runner_max_in_flight(&settings, watch_mode)?;
    let mut effective_concurrency = 0usize;
    for (name, pool_size) in &pool_sizes {
        // Only count concurrency for runners we'll actually spawn
        if !needed_runners.contains(name) {
            continue;
        }
        let in_flight = max_in_flight.get(name).copied().unwrap_or(1);
        effective_concurrency += pool_size.saturating_mul(in_flight);
    }
    let effective_concurrency = std::cmp::max(1, effective_concurrency);
    let runners = build_runners_from_settings_filtered(
        &settings,
        Some(&pool_sizes),
        Some(&max_in_flight),
        Some(&needed_runners),
    )
    .await?;
    let queues = if queues.is_empty() {
        None
    } else {
        Some(queues)
    };
    let worker = RRQWorker::new(
        settings,
        queues,
        None,
        runners,
        burst,
        effective_concurrency,
    )
    .await?;
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
    let mut completed = None;
    tokio::select! {
        _ = wait_for_shutdown_signal() => {
            shutdown.store(true, Ordering::SeqCst);
        }
        result = &mut handle => {
            if let Err(err) = &result {
                eprintln!("worker crashed: {err}");
            }
            completed = Some(result);
        }
    }
    let worker = if let Some(result) = completed {
        result?
    } else {
        match tokio::time::timeout(shutdown_timeout, &mut handle).await {
            Ok(result) => result?,
            Err(_) => {
                handle.abort();
                return Ok(());
            }
        }
    };
    worker.close_runners().await;
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

fn watch_restart_limit() -> Option<usize> {
    if cfg!(test) { Some(1) } else { None }
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

async fn run_pre_restart_cmds(
    cmds: &[Vec<String>],
    cwd: &Path,
    per_cmd_timeout: Option<Duration>,
) -> Result<bool> {
    for (idx, cmd) in cmds.iter().enumerate() {
        let Some((exe, args)) = cmd.split_first() else {
            return Err(anyhow::anyhow!(
                "invalid rrq.watch.pre_restart_cmds[{idx}]: command is empty"
            ));
        };
        println!(
            "Running watch pre-restart command: {}{}",
            exe,
            if args.is_empty() {
                "".to_string()
            } else {
                format!(" {}", args.join(" "))
            }
        );
        let mut child = Command::new(exe)
            .args(args)
            .current_dir(cwd)
            // Inherit IO so build errors are visible in the terminal.
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .with_context(|| format!("failed to spawn rrq.watch.pre_restart_cmds[{idx}]"))?;

        let status = if let Some(limit) = per_cmd_timeout {
            match timeout(limit, child.wait()).await {
                Ok(result) => result?,
                Err(_) => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    eprintln!(
                        "watch pre-restart command timed out after {:.3}s (idx={idx})",
                        limit.as_secs_f64()
                    );
                    return Ok(false);
                }
            }
        } else {
            child.wait().await?
        };

        if !status.success() {
            eprintln!("watch pre-restart command failed (idx={idx}, status={status})");
            return Ok(false);
        }
    }
    Ok(true)
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
        .or(watch_settings.path.clone())
        .unwrap_or_else(|| ".".to_string());
    let include_patterns = if include_patterns.is_empty() {
        if watch_settings.include_patterns.is_empty() {
            vec!["*.py".to_string(), "*.toml".to_string()]
        } else {
            watch_settings.include_patterns.clone()
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
            watch_settings.ignore_patterns.clone()
        }
    } else {
        ignore_patterns
    };
    let no_gitignore = no_gitignore || watch_settings.no_gitignore.unwrap_or(false);
    let pre_restart_cmds = watch_settings.pre_restart_cmds.clone();
    let pre_restart_cwd = watch_settings.pre_restart_cwd.clone();
    let pre_restart_timeout = watch_settings
        .pre_restart_timeout_seconds
        .and_then(|s| {
            if s.is_finite() && s > 0.0 {
                Some(s)
            } else {
                None
            }
        })
        .map(Duration::from_secs_f64);

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
    let mut restarts = 0usize;
    let restart_limit = watch_restart_limit();

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

        // If configured, run build steps (or other commands) before starting/restarting the worker.
        if !pre_restart_cmds.is_empty() {
            let cwd = pre_restart_cwd
                .as_deref()
                .map(PathBuf::from)
                .unwrap_or_else(|| watch_root.clone());
            let ok = run_pre_restart_cmds(&pre_restart_cmds, &cwd, pre_restart_timeout).await?;
            if !ok {
                eprintln!(
                    "watch pre-restart commands failed; worker will remain stopped until next change."
                );
                // Stay stopped and wait for a change event that matches the patterns, then retry.
                loop {
                    tokio::select! {
                        _ = wait_for_shutdown_signal() => {
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
                                            "Change detected while stopped (event={:?}, paths={:?}).",
                                            event.kind, event.paths
                                        );
                                        // Drain a little to coalesce rapid edits before retrying.
                                        while tokio::time::timeout(Duration::from_millis(200), rx.recv())
                                            .await
                                            .ok()
                                            .flatten()
                                            .is_some()
                                        {}
                                        break;
                                    }
                                }
                                Err(err) => {
                                    eprintln!("watch error: {err}");
                                    while tokio::time::timeout(Duration::from_millis(200), rx.recv())
                                        .await
                                        .ok()
                                        .flatten()
                                        .is_some()
                                    {}
                                    break;
                                }
                            }
                        }
                    }
                    if exit_loop {
                        break;
                    }
                }
                if exit_loop {
                    break;
                }
                // Force reload settings on next iteration (config may have changed).
                cached_settings = None;
                continue;
            }
        }

        // Determine which runners are needed based on the queues being listened to
        let queues_slice = if queues.is_empty() {
            None
        } else {
            Some(queues.as_slice())
        };
        let needed_runners = determine_needed_runners(&settings, queues_slice);

        let pool_sizes = resolve_runner_pool_sizes(&settings, true, None)?;
        let max_in_flight = resolve_runner_max_in_flight(&settings, true)?;
        let mut effective_concurrency = 0usize;
        for (name, pool_size) in &pool_sizes {
            // Only count concurrency for runners we'll actually spawn
            if !needed_runners.contains(name) {
                continue;
            }
            let in_flight = max_in_flight.get(name).copied().unwrap_or(1);
            effective_concurrency += pool_size.saturating_mul(in_flight);
        }
        let effective_concurrency = std::cmp::max(1, effective_concurrency);
        let mut settings = settings;
        settings.worker_shutdown_grace_period_seconds = 0.0;
        let runners = build_runners_from_settings_filtered(
            &settings,
            Some(&pool_sizes),
            Some(&max_in_flight),
            Some(&needed_runners),
        )
        .await?;
        let queue_arg = if queues.is_empty() {
            None
        } else {
            Some(queues.clone())
        };
        let mut worker = RRQWorker::new(
            settings,
            queue_arg,
            None,
            runners,
            false,
            effective_concurrency,
        )
        .await?;
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
        worker.close_runners().await;
        if exit_loop {
            break;
        }
        if !restart {
            break;
        }
        restarts += 1;
        if let Some(limit) = restart_limit
            && restarts >= limit
        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::RedisTestContext;
    use notify::EventKind;
    use notify::event::{AccessKind, DataChange, ModifyKind};
    use std::fs;
    use std::net::TcpListener as StdTcpListener;
    use std::os::unix::fs::PermissionsExt;
    use tokio::fs as tokio_fs;
    use uuid::Uuid;

    fn make_event(kind: EventKind, path: &Path) -> Event {
        Event {
            kind,
            paths: vec![path.to_path_buf()],
            attrs: Default::default(),
        }
    }

    #[test]
    fn should_restart_ignores_access_and_metadata() {
        let base = PathBuf::from("/tmp");
        let include = build_globset(&[]).unwrap();
        let ignore = build_globset(&[]).unwrap();
        let access_event = make_event(EventKind::Access(AccessKind::Read), &base.join("a.py"));
        assert!(!should_restart_for_event(
            &access_event,
            &base,
            &include,
            &ignore,
            None
        ));
        let metadata_event = make_event(
            EventKind::Modify(ModifyKind::Metadata(notify::event::MetadataKind::Any)),
            &base.join("a.py"),
        );
        assert!(!should_restart_for_event(
            &metadata_event,
            &base,
            &include,
            &ignore,
            None
        ));
    }

    #[test]
    fn should_restart_respects_include_and_ignore() {
        let base = std::env::temp_dir().join(format!("rrq-watch-{}", Uuid::new_v4()));
        fs::create_dir_all(&base).unwrap();
        let include = build_globset(&["*.py".to_string()]).unwrap();
        let ignore = build_globset(&["ignored.py".to_string()]).unwrap();
        let event = make_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &base.join("main.py"),
        );
        assert!(should_restart_for_event(
            &event, &base, &include, &ignore, None
        ));
        let ignored_event = make_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &base.join("ignored.py"),
        );
        assert!(!should_restart_for_event(
            &ignored_event,
            &base,
            &include,
            &ignore,
            None
        ));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn should_restart_respects_gitignore() {
        let base = std::env::temp_dir().join(format!("rrq-watch-{}", Uuid::new_v4()));
        fs::create_dir_all(&base).unwrap();
        fs::write(base.join(".gitignore"), "ignored.py\n").unwrap();
        let include = build_globset(&[]).unwrap();
        let ignore = build_globset(&[]).unwrap();
        let gitignore = load_gitignore(&base).unwrap();
        let event = make_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &base.join("ignored.py"),
        );
        assert!(!should_restart_for_event(
            &event,
            &base,
            &include,
            &ignore,
            gitignore.as_ref()
        ));
        let event = make_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &base.join("ok.py"),
        );
        assert!(should_restart_for_event(
            &event,
            &base,
            &include,
            &ignore,
            gitignore.as_ref()
        ));
        let _ = fs::remove_dir_all(&base);
    }

    async fn write_runner_script(dir: &Path) -> Result<PathBuf> {
        let script_path = dir.join("rrq-dummy-runner.py");
        let script = r#"#!/usr/bin/env python3
import os
import socket
import time

tcp_socket = os.environ.get("RRQ_RUNNER_TCP_SOCKET")
if not tcp_socket:
    raise SystemExit(1)
host, _, port_str = tcp_socket.rpartition(":")
if not host:
    raise SystemExit(1)
port = int(port_str)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((host, port))
sock.listen(1)
try:
    conn, _ = sock.accept()
    conn.close()
except Exception:
    pass
while True:
    time.sleep(1)
"#;
        tokio_fs::write(&script_path, script).await?;
        let mut perms = tokio_fs::metadata(&script_path).await?.permissions();
        perms.set_mode(0o755);
        tokio_fs::set_permissions(&script_path, perms).await?;
        Ok(script_path)
    }

    async fn write_runner_script_with_marker(dir: &Path, marker_path: &Path) -> Result<PathBuf> {
        let script_path = dir.join("rrq-dummy-runner-marker.py");
        let marker = marker_path.to_string_lossy();
        let script = format!(
            r#"#!/usr/bin/env python3
import os
import socket
import time

marker_path = r"{marker}"
with open(marker_path, "w", encoding="utf-8") as f:
    f.write("started\n")

tcp_socket = os.environ.get("RRQ_RUNNER_TCP_SOCKET")
if not tcp_socket:
    raise SystemExit(1)
host, _, port_str = tcp_socket.rpartition(":")
if not host:
    raise SystemExit(1)
port = int(port_str)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((host, port))
sock.listen(1)
try:
    conn, _ = sock.accept()
    conn.close()
except Exception:
    pass
while True:
    time.sleep(1)
"#
        );
        tokio_fs::write(&script_path, script).await?;
        let mut perms = tokio_fs::metadata(&script_path).await?.permissions();
        perms.set_mode(0o755);
        tokio_fs::set_permissions(&script_path, perms).await?;
        Ok(script_path)
    }

    async fn write_build_gate_script(dir: &Path) -> Result<PathBuf> {
        let script_path = dir.join("rrq-build-gate.py");
        let script = r#"import pathlib
import sys

p = pathlib.Path("rrq-build-counter.txt")
try:
    n = int(p.read_text(encoding="utf-8"))
except Exception:
    n = 0
n += 1
p.write_text(str(n), encoding="utf-8")
sys.exit(1 if n == 1 else 0)
"#;
        tokio_fs::write(&script_path, script).await?;
        Ok(script_path)
    }

    async fn write_worker_config(
        settings: &rrq::RRQSettings,
        script_path: &Path,
    ) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!("rrq-worker-{}.toml", Uuid::new_v4()));
        let port = StdTcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let payload = format!(
            "[rrq]\nredis_dsn = \"{}\"\ndefault_queue_name = \"{}\"\ndefault_dlq_name = \"{}\"\ndefault_runner_name = \"python\"\n\n[rrq.runners.python]\ncmd = [\"python3\", \"{}\"]\ntcp_socket = \"127.0.0.1:{}\"\npool_size = 1\nmax_in_flight = 1\n",
            settings.redis_dsn,
            settings.default_queue_name,
            settings.default_dlq_name,
            script_path.to_string_lossy(),
            port,
        );
        tokio_fs::write(&path, payload).await?;
        Ok(path)
    }

    async fn write_worker_config_with_watch(
        settings: &rrq::RRQSettings,
        script_path: &Path,
        watch_payload: &str,
    ) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!("rrq-worker-{}.toml", Uuid::new_v4()));
        let port = StdTcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let payload = format!(
            "[rrq]\nredis_dsn = \"{}\"\ndefault_queue_name = \"{}\"\ndefault_dlq_name = \"{}\"\ndefault_runner_name = \"python\"\n\n[rrq.runners.python]\ncmd = [\"python3\", \"{}\"]\ntcp_socket = \"127.0.0.1:{}\"\npool_size = 1\nmax_in_flight = 1\n\n{}\n",
            settings.redis_dsn,
            settings.default_queue_name,
            settings.default_dlq_name,
            script_path.to_string_lossy(),
            port,
            watch_payload,
        );
        tokio_fs::write(&path, payload).await?;
        Ok(path)
    }

    #[tokio::test]
    async fn run_worker_burst_exits() -> Result<()> {
        let ctx = RedisTestContext::new().await?;
        let temp_dir = std::env::temp_dir().join(format!("rrq-worker-{}", Uuid::new_v4()));
        tokio_fs::create_dir_all(&temp_dir).await?;
        let script_path = write_runner_script(&temp_dir).await?;
        let config_path = write_worker_config(&ctx.settings, &script_path).await?;

        run_worker(
            Some(config_path.to_string_lossy().to_string()),
            Vec::new(),
            true,
            false,
        )
        .await?;

        let _ = tokio_fs::remove_file(&config_path).await;
        let _ = tokio_fs::remove_file(&script_path).await;
        let _ = tokio_fs::remove_dir_all(&temp_dir).await;
        Ok(())
    }

    #[tokio::test]
    async fn run_worker_watch_restarts_on_change() -> Result<()> {
        let ctx = RedisTestContext::new().await?;
        let temp_dir = std::env::temp_dir().join(format!("rrq-watch-worker-{}", Uuid::new_v4()));
        tokio_fs::create_dir_all(&temp_dir).await?;
        let watch_root = temp_dir.join("watch");
        tokio_fs::create_dir_all(&watch_root).await?;
        let script_path = write_runner_script(&temp_dir).await?;
        let config_path = write_worker_config(&ctx.settings, &script_path).await?;

        let handle = tokio::spawn(run_worker_watch(
            Some(config_path.to_string_lossy().to_string()),
            Vec::new(),
            Some(watch_root.to_string_lossy().to_string()),
            vec!["*.txt".to_string()],
            Vec::new(),
            true,
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;
        let trigger = watch_root.join("trigger.txt");
        for idx in 0..5 {
            tokio_fs::write(&trigger, format!("ping-{idx}")).await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            if handle.is_finished() {
                break;
            }
        }

        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok());
        let join = result.unwrap();
        assert!(join.is_ok());

        let _ = tokio_fs::remove_file(&config_path).await;
        let _ = tokio_fs::remove_file(&script_path).await;
        let _ = tokio_fs::remove_dir_all(&temp_dir).await;
        Ok(())
    }

    #[tokio::test]
    async fn run_worker_watch_pre_restart_cmds_gate_worker_start() -> Result<()> {
        let ctx = RedisTestContext::new().await?;
        let temp_dir = std::env::temp_dir().join(format!("rrq-watch-build-{}", Uuid::new_v4()));
        tokio_fs::create_dir_all(&temp_dir).await?;
        let watch_root = temp_dir.join("watch");
        tokio_fs::create_dir_all(&watch_root).await?;

        let runner_marker = temp_dir.join("runner-started.txt");
        let script_path = write_runner_script_with_marker(&temp_dir, &runner_marker).await?;
        let build_script_path = write_build_gate_script(&temp_dir).await?;
        let watch_payload = format!(
            "[rrq.watch]\npre_restart_cmds = [[\"python3\", \"{}\"]]\npre_restart_cwd = \"{}\"\n",
            build_script_path.file_name().unwrap().to_string_lossy(),
            temp_dir.to_string_lossy()
        );
        let config_path =
            write_worker_config_with_watch(&ctx.settings, &script_path, &watch_payload).await?;

        let handle = tokio::spawn(run_worker_watch(
            Some(config_path.to_string_lossy().to_string()),
            Vec::new(),
            Some(watch_root.to_string_lossy().to_string()),
            vec!["*.txt".to_string()],
            Vec::new(),
            true,
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;
        // First pre-restart command fails, so the worker should remain stopped.
        assert!(!runner_marker.exists());

        let trigger = watch_root.join("trigger.txt");
        tokio_fs::write(&trigger, "ping-1").await?;

        // Next change should let the build succeed and the worker should start.
        let started = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runner_marker.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(started.is_ok());

        // Trigger a restart so the test-only watch restart limit can terminate the loop.
        tokio_fs::write(&trigger, "ping-2").await?;
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok());
        let join = result.unwrap();
        assert!(join.is_ok());

        let counter_path = temp_dir.join("rrq-build-counter.txt");
        let counter = tokio_fs::read_to_string(&counter_path).await?;
        let n: i64 = counter.trim().parse().unwrap_or(0);
        assert!(n >= 2);

        let _ = tokio_fs::remove_file(&config_path).await;
        let _ = tokio_fs::remove_file(&script_path).await;
        let _ = tokio_fs::remove_file(&build_script_path).await;
        let _ = tokio_fs::remove_file(&counter_path).await;
        let _ = tokio_fs::remove_file(&runner_marker).await;
        let _ = tokio_fs::remove_dir_all(&temp_dir).await;
        Ok(())
    }
}
