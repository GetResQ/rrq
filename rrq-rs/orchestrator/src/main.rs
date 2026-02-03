use anyhow::Result;
use clap::{Parser, Subcommand};
mod cli_utils;
mod commands;
mod telemetry;

/// Initialize the rustls crypto provider (ring).
/// This must be called once before any TLS connections are made.
fn init_crypto_provider() {
    // Use the ring crypto provider for rustls.
    // This is required for rustls 0.22+ and embeds CA roots via webpki-roots.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

use commands::{
    DlqListOptions, DlqRequeueOptions, check_workers, debug_clear, debug_generate_jobs,
    debug_generate_workers, debug_stress_test, debug_submit, dlq_inspect, dlq_list, dlq_requeue,
    dlq_stats, job_cancel, job_list, job_replay, job_show, job_trace, queue_inspect, queue_list,
    queue_stats, run_worker, run_worker_watch, runner_python,
};

#[derive(Parser)]
#[command(name = "rrq")]
#[command(version)]
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
    Runner {
        #[command(subcommand)]
        command: RunnerCommand,
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
        #[arg(long)]
        path: Option<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        pattern: Vec<String>,
        #[arg(long, action = clap::ArgAction::Append)]
        ignore_pattern: Vec<String>,
        #[arg(long, default_value_t = false)]
        no_gitignore: bool,
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
enum RunnerCommand {
    Python {
        #[arg(long)]
        settings: Option<String>,
        #[arg(long)]
        tcp_socket: Option<String>,
    },
}

async fn dispatch_command(command: Commands) -> Result<()> {
    match command {
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
                no_gitignore,
            } => {
                run_worker_watch(config, queue, path, pattern, ignore_pattern, no_gitignore)
                    .await?;
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
        Commands::Runner { command } => match command {
            RunnerCommand::Python {
                settings,
                tcp_socket,
            } => {
                runner_python(settings, tcp_socket).await?;
            }
        },
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_crypto_provider();
    telemetry::init_tracing();
    let cli = Cli::parse();
    dispatch_command(cli.command).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::RedisTestContext;
    use chrono::Utc;
    use rrq::EnqueueOptions;
    use std::net::TcpListener as StdTcpListener;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, OnceLock};
    use tokio::fs as tokio_fs;
    use tokio::sync::Mutex as TokioMutex;
    use uuid::Uuid;

    static ENV_LOCK: OnceLock<Arc<TokioMutex<()>>> = OnceLock::new();

    fn env_lock() -> Arc<TokioMutex<()>> {
        ENV_LOCK
            .get_or_init(|| Arc::new(TokioMutex::new(())))
            .clone()
    }

    struct EnvGuard {
        _lock: tokio::sync::OwnedMutexGuard<()>,
        key: &'static str,
        prev: Option<String>,
    }

    #[allow(unsafe_code)] // env var manipulation in tests
    impl EnvGuard {
        async fn set(key: &'static str, value: String) -> Self {
            let lock = env_lock().lock_owned().await;
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self {
                _lock: lock,
                key,
                prev,
            }
        }
    }

    #[allow(unsafe_code)] // env var manipulation in tests
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.prev.take() {
                unsafe {
                    std::env::set_var(self.key, prev);
                }
            } else {
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
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

    async fn write_worker_config(
        settings: &rrq::RRQSettings,
        script_path: &Path,
    ) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!("rrq-main-{}.toml", Uuid::new_v4()));
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

    async fn write_rrq_runner_script(dir: &Path) -> Result<PathBuf> {
        let script_path = dir.join("rrq-runner");
        tokio_fs::write(&script_path, "#!/bin/sh\nexit 0\n").await?;
        let mut perms = tokio_fs::metadata(&script_path).await?.permissions();
        perms.set_mode(0o755);
        tokio_fs::set_permissions(&script_path, perms).await?;
        Ok(script_path)
    }

    #[tokio::test]
    async fn dispatch_command_covers_branches() -> Result<()> {
        let mut ctx = RedisTestContext::new().await?;
        let temp_dir = std::env::temp_dir().join(format!("rrq-main-{}", Uuid::new_v4()));
        tokio_fs::create_dir_all(&temp_dir).await?;
        let script_path = write_runner_script(&temp_dir).await?;
        let config_path = write_worker_config(&ctx.settings, &script_path).await?;
        let rrq_runner = write_rrq_runner_script(&temp_dir).await?;
        let path_guard = EnvGuard::set(
            "PATH",
            format!(
                "{}:{}",
                temp_dir.to_string_lossy(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .await;

        let config = Some(config_path.to_string_lossy().to_string());

        let mut client = rrq::RRQClient::new(ctx.settings.clone(), ctx.store.clone());
        let job = client
            .enqueue(
                "demo_job",
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await?;
        ctx.store
            .move_job_to_dlq(&job.id, &ctx.settings.default_dlq_name, "boom", Utc::now())
            .await?;

        dispatch_command(Commands::Queue {
            command: QueueCommand::List {
                config: config.clone(),
                show_empty: true,
            },
        })
        .await?;
        dispatch_command(Commands::Queue {
            command: QueueCommand::Stats {
                config: config.clone(),
                queue: Vec::new(),
                max_scan: 1,
            },
        })
        .await?;
        dispatch_command(Commands::Queue {
            command: QueueCommand::Inspect {
                queue_name: ctx.settings.default_queue_name.clone(),
                config: config.clone(),
                limit: 5,
                offset: 0,
            },
        })
        .await?;

        dispatch_command(Commands::Job {
            command: JobCommand::Show {
                job_id: job.id.clone(),
                config: config.clone(),
                raw: true,
            },
        })
        .await?;
        dispatch_command(Commands::Job {
            command: JobCommand::List {
                config: config.clone(),
                status: Some("pending".to_string()),
                queue: None,
                function: None,
                limit: 5,
            },
        })
        .await?;
        dispatch_command(Commands::Job {
            command: JobCommand::Replay {
                job_id: job.id.clone(),
                config: config.clone(),
                queue: None,
            },
        })
        .await?;
        let cancel_job = client
            .enqueue(
                "cancel_me",
                serde_json::Map::new(),
                EnqueueOptions::default(),
            )
            .await?;
        dispatch_command(Commands::Job {
            command: JobCommand::Cancel {
                job_id: cancel_job.id,
                config: config.clone(),
            },
        })
        .await?;
        dispatch_command(Commands::Job {
            command: JobCommand::Trace {
                job_id: job.id.clone(),
                config: config.clone(),
            },
        })
        .await?;

        dispatch_command(Commands::Dlq {
            command: DlqCommand::List {
                config: config.clone(),
                queue: None,
                function: None,
                limit: 10,
                offset: 0,
                dlq_name: None,
                raw: false,
                batch_size: 10,
            },
        })
        .await?;
        dispatch_command(Commands::Dlq {
            command: DlqCommand::Stats {
                config: config.clone(),
                dlq_name: None,
            },
        })
        .await?;
        dispatch_command(Commands::Dlq {
            command: DlqCommand::Inspect {
                job_id: job.id.clone(),
                config: config.clone(),
                raw: true,
            },
        })
        .await?;
        dispatch_command(Commands::Dlq {
            command: DlqCommand::Requeue {
                config: config.clone(),
                dlq_name: None,
                target_queue: None,
                queue: None,
                function: None,
                job_id: Some(job.id.clone()),
                limit: Some(1),
                all: false,
                dry_run: true,
            },
        })
        .await?;

        dispatch_command(Commands::Check {
            config: config.clone(),
        })
        .await?;

        dispatch_command(Commands::Debug {
            command: DebugCommand::GenerateJobs {
                config: config.clone(),
                count: 1,
                queue: Vec::new(),
                status: Vec::new(),
                age_hours: 1,
                batch_size: 1,
            },
        })
        .await?;
        dispatch_command(Commands::Debug {
            command: DebugCommand::GenerateWorkers {
                config: config.clone(),
                count: 1,
                duration: 0,
            },
        })
        .await?;
        dispatch_command(Commands::Debug {
            command: DebugCommand::Submit {
                function_name: "debug_fn".to_string(),
                config: config.clone(),
                args: None,
                kwargs: None,
                queue: None,
                delay: None,
            },
        })
        .await?;
        dispatch_command(Commands::Debug {
            command: DebugCommand::Clear {
                config: config.clone(),
                confirm: true,
                pattern: "rrq:job:*".to_string(),
            },
        })
        .await?;
        dispatch_command(Commands::Debug {
            command: DebugCommand::StressTest {
                config: config.clone(),
                jobs_per_second: 1,
                duration: 0,
                queues: Vec::new(),
            },
        })
        .await?;

        dispatch_command(Commands::Runner {
            command: RunnerCommand::Python {
                settings: None,
                tcp_socket: None,
            },
        })
        .await?;

        dispatch_command(Commands::Worker {
            command: WorkerCommand::Run {
                config: config.clone(),
                queue: Vec::new(),
                burst: true,
            },
        })
        .await?;

        drop(path_guard);
        let _ = tokio_fs::remove_file(&rrq_runner).await;
        let _ = tokio_fs::remove_file(&config_path).await;
        let _ = tokio_fs::remove_file(&script_path).await;
        let _ = tokio_fs::remove_dir_all(&temp_dir).await;
        Ok(())
    }
}
