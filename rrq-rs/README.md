# RRQ Rust Workspace

This is the Rust implementation of RRQ (Reliable Redis Queue), providing the orchestrator, producer, runner, and protocol crates.

## What is RRQ?

RRQ is a distributed job queue that separates concerns cleanly: a Rust orchestrator handles all the complex distributed systems logic (scheduling, retries, locking, timeouts) while job handlers run in isolated processes using any language.

**Key benefits:**
- **Redis-backed** - Atomic operations, predictable semantics, no separate database
- **Language-agnostic workers** - Write handlers in Python, TypeScript, or Rust
- **Production-ready** - Retries, DLQ, timeouts, cron, distributed tracing out of the box
- **Fast and lightweight** - Thousands of jobs/second with minimal memory

## Crates

| Crate | Description | crates.io |
|-------|-------------|-----------|
| [`rrq`](./orchestrator) | CLI orchestrator - schedules jobs, manages runners, handles retries | [![Crates.io](https://img.shields.io/crates/v/rrq.svg)](https://crates.io/crates/rrq) |
| [`rrq-producer`](./producer) | Client library for enqueuing jobs | [![Crates.io](https://img.shields.io/crates/v/rrq-producer.svg)](https://crates.io/crates/rrq-producer) |
| [`rrq-runner`](./runner) | Runtime for building job handlers in Rust | [![Crates.io](https://img.shields.io/crates/v/rrq-runner.svg)](https://crates.io/crates/rrq-runner) |
| [`rrq-protocol`](./protocol) | Wire protocol types for orchestrator-runner communication | [![Crates.io](https://img.shields.io/crates/v/rrq-protocol.svg)](https://crates.io/crates/rrq-protocol) |
| [`rrq-config`](./config) | Configuration loader and settings types | [![Crates.io](https://img.shields.io/crates/v/rrq-config.svg)](https://crates.io/crates/rrq-config) |

## Quick Start

### Install the orchestrator

```bash
cargo install rrq
```

### Run a worker

```bash
rrq worker run --config rrq.toml
```

### Configuration (`rrq.toml`)

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

## Using as Dependencies

Add crates via git:

```toml
[dependencies]
rrq-producer = { git = "https://github.com/getresq/rrq", package = "rrq-producer" }
rrq-protocol = { git = "https://github.com/getresq/rrq", package = "rrq-protocol" }
```

Or from crates.io:

```toml
[dependencies]
rrq-producer = "0.9"
rrq-runner = "0.9"
```

## Building from Source

```bash
# Build all crates
cargo build --release

# Run tests
cargo test

# Build the orchestrator binary
cargo build -p rrq --release
# Binary at ./target/release/rrq
```

## Example: Rust Runner

```bash
cd runner
RRQ_RUNNER_TCP_SOCKET=127.0.0.1:9000 cargo run --example socket_runner
```

## Related Packages

| Package | Language | Purpose |
|---------|----------|---------|
| [rrq](https://pypi.org/project/rrq/) | Python | Producer client + runner |
| [rrq-ts](https://www.npmjs.com/package/rrq-ts) | TypeScript | Producer client + runner |

## License

Apache-2.0
