# RRQ

[![Crates.io](https://img.shields.io/crates/v/rrq.svg)](https://crates.io/crates/rrq)
[![Documentation](https://docs.rs/rrq/badge.svg)](https://docs.rs/rrq)
[![License](https://img.shields.io/crates/l/rrq.svg)](LICENSE)

**The orchestrator for RRQ**, a distributed job queue that combines Rust reliability with language-flexible workers.

## What is RRQ?

RRQ (Reliable Redis Queue) separates the hard parts of distributed job processing—scheduling, retries, locking, timeouts—into a single Rust binary. Your job handlers can be written in Python, TypeScript, or Rust, running as isolated processes managed by the orchestrator.

**Why choose RRQ?**

- **Write handlers in any language** - Python, TypeScript, or Rust workers connect via socket protocol
- **Redis-native** - Atomic operations, predictable semantics, no separate database to manage
- **Battle-tested Rust core** - The complex distributed systems logic runs in optimized Rust
- **Production features included** - Retries, DLQ, timeouts, cron scheduling, health checks, distributed tracing

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

### 1. Create configuration (`rrq.toml`)

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"
default_runner_name = "python"

[rrq.runners.python]
type = "socket"
cmd = ["rrq-runner", "--settings", "myapp.runner:settings"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4
max_in_flight = 10
```

### 2. Start the worker

```bash
rrq worker run --config rrq.toml
```

The orchestrator spawns runner processes, polls Redis for jobs, dispatches work, handles retries, and manages the entire lifecycle.

### 3. Enqueue jobs

Use the [`rrq-producer`](https://crates.io/crates/rrq-producer) crate for Rust, or the Python/TypeScript clients:

```rust
use rrq_producer::Producer;

let producer = Producer::new("redis://localhost:6379/0").await?;
let job_id = producer.enqueue("my_handler", json!({"key": "value"})).await?;
```

## CLI Commands

### Worker

```bash
# Production mode
rrq worker run --config rrq.toml

# Watch mode (restarts on file changes)
rrq worker watch --config rrq.toml --path ./src

# Burst mode (exit when queue empty)
rrq worker run --config rrq.toml --burst
```

### Queues

```bash
rrq queue list --config rrq.toml
rrq queue inspect default --config rrq.toml
rrq queue pause default --config rrq.toml
rrq queue resume default --config rrq.toml
```

### Jobs

```bash
rrq job get <job-id> --config rrq.toml
rrq job cancel <job-id> --config rrq.toml
rrq job retry <job-id> --config rrq.toml
```

### Dead Letter Queue

```bash
rrq dlq list --config rrq.toml
rrq dlq retry-all --config rrq.toml
rrq dlq purge --config rrq.toml
```

### Health

```bash
rrq health --config rrq.toml
```

## Configuration Reference

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"    # Required
default_runner_name = "python"
default_job_timeout_seconds = 300
default_max_retries = 3
heartbeat_interval_seconds = 60

[rrq.runners.python]
type = "socket"
cmd = ["rrq-runner", "--settings", "myapp.runner:settings"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4           # Runner processes
max_in_flight = 10      # Concurrent jobs per runner
cwd = "/app"            # Working directory (optional)

[rrq.runners.python.env]
PYTHONPATH = "/app"     # Environment variables (optional)

# Cron jobs
[[rrq.cron_jobs]]
function_name = "daily_cleanup"
schedule = "0 0 9 * * *"   # 6-field cron (with seconds)
queue_name = "maintenance"

# Watch mode settings
[rrq.watch]
path = "."
include_patterns = ["*.py", "*.toml"]
ignore_patterns = [".venv/**", "dist/**"]
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     RRQ Orchestrator                         │
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
│      │  Runner Pool  │       │  Runner Pool  │              │
│      │   (Python)    │       │    (Node)     │              │
│      └───────────────┘       └───────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RRQ_CONFIG` | Path to configuration file |
| `RRQ_REDIS_DSN` | Redis connection (overrides config) |
| `RRQ_LOG_LEVEL` | Log level: trace, debug, info, warn, error |

## Related Crates

| Crate | Purpose |
|-------|---------|
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Enqueue jobs from Rust |
| [`rrq-runner`](https://crates.io/crates/rrq-runner) | Build Rust job handlers |
| [`rrq-protocol`](https://crates.io/crates/rrq-protocol) | Wire protocol types |
| [`rrq-config`](https://crates.io/crates/rrq-config) | Configuration types |

## Cross-Language Compatibility

The orchestrator works seamlessly with:
- Python workers via [rrq](https://pypi.org/project/rrq/)
- TypeScript workers via [rrq-ts](https://www.npmjs.com/package/rrq-ts)
- Rust workers via [rrq-runner](https://crates.io/crates/rrq-runner)

All use the same Redis schema and socket protocol.

## License

Apache-2.0
