# RRQ

[![Crates.io](https://img.shields.io/crates/v/rrq.svg)](https://crates.io/crates/rrq)
[![Documentation](https://docs.rs/rrq/badge.svg)](https://docs.rs/rrq)
[![License](https://img.shields.io/crates/l/rrq.svg)](LICENSE)

A high-performance Redis job queue orchestrator written in Rust. RRQ manages job scheduling, worker coordination, and executor process pools for distributed task processing.

## Features

- **Redis-backed job queue** with atomic operations and reliable delivery
- **Socket-based executor pool** for running jobs in isolated processes
- **Auto-reconnecting Redis connections** via ConnectionManager
- **Cron job scheduling** with standard cron syntax
- **Watch mode** for development with automatic executor restarts
- **Dead letter queue (DLQ)** for failed jobs
- **Health checks** and worker heartbeats
- **OpenTelemetry trace context** propagation

## Installation

```bash
cargo install rrq
```

Or add to your `Cargo.toml`:

```toml
[dependencies]
rrq = "0.9"
```

## Quick Start

### 1. Create a configuration file (`rrq.toml`)

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"
default_executor_name = "python"

[rrq.executors.python]
type = "socket"
cmd = ["rrq-executor", "--settings", "my_app.executor_settings"]
pool_size = 2
max_in_flight = 10
```

### 2. Run a worker

```bash
rrq worker run --config rrq.toml
```

### 3. Enqueue jobs

Use the [`rrq-producer`](https://crates.io/crates/rrq-producer) crate to enqueue jobs from Rust:

```rust
use rrq_producer::{Producer, EnqueueOptions};
use serde_json::json;

let producer = Producer::new("redis://localhost:6379/0").await?;
let job_id = producer.enqueue(
    "my_handler",
    vec![json!("arg1"), json!(42)],
    serde_json::Map::new(),
    EnqueueOptions::default(),
).await?;
```

## CLI Commands

### Worker Commands

```bash
# Run worker (production mode)
rrq worker run --config rrq.toml

# Run worker in watch mode (restarts on file changes)
rrq worker watch --config rrq.toml --path ./src

# Run worker in burst mode (exit when queue is empty)
rrq worker run --config rrq.toml --burst
```

### Queue Commands

```bash
# List all queues with job counts
rrq queue list --config rrq.toml

# Inspect a specific queue
rrq queue inspect default --config rrq.toml

# Pause/resume a queue
rrq queue pause default --config rrq.toml
rrq queue resume default --config rrq.toml
```

### Job Commands

```bash
# Get job details
rrq job get <job-id> --config rrq.toml

# Cancel a running job
rrq job cancel <job-id> --config rrq.toml

# Retry a failed job
rrq job retry <job-id> --config rrq.toml
```

### Dead Letter Queue Commands

```bash
# List jobs in DLQ
rrq dlq list --config rrq.toml

# Retry all jobs in DLQ
rrq dlq retry-all --config rrq.toml

# Purge DLQ
rrq dlq purge --config rrq.toml
```

### Health Commands

```bash
# Check worker health
rrq health --config rrq.toml
```

## Configuration Reference

```toml
[rrq]
# Redis connection string (required)
redis_dsn = "redis://localhost:6379/0"

# Default executor for jobs without explicit executor
default_executor_name = "python"

# Worker concurrency is derived from executor pool_size * max_in_flight

# Worker heartbeat interval (seconds)
heartbeat_interval_seconds = 60

# Connection timeout for executors (milliseconds)
executor_connect_timeout_ms = 5000

# Default job timeout (seconds)
default_job_timeout_seconds = 300

# Lock extension beyond job timeout (seconds)
default_lock_timeout_extension_seconds = 60

[rrq.executors.python]
# Executor type: "socket" (only supported type currently)
type = "socket"

# Command to spawn executor process
cmd = ["rrq-executor", "--settings", "my_app.settings"]

# Number of executor processes in pool
pool_size = 2

# Max concurrent jobs per executor process
max_in_flight = 10

# Optional: Use TCP instead of Unix sockets
tcp_socket = "127.0.0.1:9000"

# Optional: Custom socket directory
socket_dir = "/tmp/rrq"

# Optional: Working directory for executor
cwd = "/app"

# Optional: Environment variables
[rrq.executors.python.env]
PYTHONPATH = "/app"

# Optional: Response timeout (seconds)
response_timeout_seconds = 300.0

# Cron jobs
[[rrq.cron]]
name = "daily-cleanup"
schedule = "0 0 * * *"
function_name = "cleanup_old_records"
queue_name = "maintenance"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       RRQ Orchestrator                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Worker    │  │   Worker    │  │   Cron Scheduler    │  │
│  │   Loop      │  │   Loop      │  │                     │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                    │              │
│         └────────────────┼────────────────────┘              │
│                          │                                   │
│                  ┌───────▼───────┐                           │
│                  │  Redis Store  │                           │
│                  └───────┬───────┘                           │
│                          │                                   │
│              ┌───────────┴───────────┐                       │
│              │                       │                       │
│      ┌───────▼───────┐       ┌───────▼───────┐              │
│      │ Executor Pool │       │ Executor Pool │              │
│      │   (Python)    │       │    (Node)     │              │
│      └───────┬───────┘       └───────┬───────┘              │
│              │                       │                       │
│      ┌───────▼───────┐       ┌───────▼───────┐              │
│      │  rrq-executor │       │  rrq-executor │              │
│      │   (process)   │       │   (process)   │              │
│      └───────────────┘       └───────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

## Related Crates

| Crate | Description |
|-------|-------------|
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Client library for enqueuing jobs |
| [`rrq-executor`](https://crates.io/crates/rrq-executor) | Executor runtime for Python/custom handlers |
| [`rrq-protocol`](https://crates.io/crates/rrq-protocol) | Protocol definitions for orchestrator-executor communication |

## Compatibility

RRQ is designed to be compatible with the Python RRQ implementation:
- Redis schema and key prefixes match the Python implementation
- Executor protocol is language-agnostic (see `docs/EXECUTOR_PROTOCOL.md`)
- Python producers (`rrq.client`) work with the Rust orchestrator
- Python executors (`rrq.executor_runtime`) work with the Rust orchestrator

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RRQ_CONFIG` | Path to configuration file |
| `RRQ_REDIS_DSN` | Redis connection string (overrides config) |
| `RRQ_LOG_LEVEL` | Log level: trace, debug, info, warn, error |
| `RRQ_EXECUTOR_SOCKET` | Unix socket path (set by orchestrator for executors) |
| `RRQ_EXECUTOR_TCP_SOCKET` | TCP socket address (set by orchestrator for executors) |

## License

Apache-2.0
