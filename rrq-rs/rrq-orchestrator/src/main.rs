use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod cli_utils;
mod commands;

use commands::{
    check_workers, debug_clear, debug_generate_jobs, debug_generate_workers, debug_stress_test,
    debug_submit, dlq_inspect, dlq_list, dlq_requeue, dlq_stats, executor_python, job_cancel,
    job_list, job_replay, job_show, job_trace, queue_inspect, queue_list, queue_stats, run_worker,
    run_worker_watch, DlqListOptions, DlqRequeueOptions,
};

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
enum ExecutorCommand {
    Python {
        #[arg(long)]
        settings: Option<String>,
        #[arg(long)]
        socket: Option<String>,
    },
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
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
        Commands::Executor { command } => match command {
            ExecutorCommand::Python { settings, socket } => {
                executor_python(settings, socket).await?;
            }
        },
    }
    Ok(())
}
