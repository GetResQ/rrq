# rrq-config

[![Crates.io](https://img.shields.io/crates/v/rrq-config.svg)](https://crates.io/crates/rrq-config)
[![Documentation](https://docs.rs/rrq-config/badge.svg)](https://docs.rs/rrq-config)
[![License](https://img.shields.io/crates/l/rrq-config.svg)](LICENSE)

**Configuration loader and settings types for RRQ.**

## What is RRQ?

RRQ (Reliable Redis Queue) is a distributed job queue with a Rust orchestrator. This crate provides shared configuration parsing for the orchestrator, producer, and FFI bindings.

## When to Use This Crate

Use `rrq-config` if you're:
- Building custom tooling that reads `rrq.toml`
- Extending the RRQ configuration format
- Embedding RRQ components with programmatic config

For most use cases, the configuration is loaded automatically by [`rrq`](https://crates.io/crates/rrq), [`rrq-producer`](https://crates.io/crates/rrq-producer), etc.

## Installation

```toml
[dependencies]
rrq-config = "0.9"
```

## Usage

### Load from file

```rust
use rrq_config::load_toml_settings;

let settings = load_toml_settings(Some("rrq.toml"))?;
println!("Redis: {}", settings.redis_dsn);
println!("Default runner: {:?}", settings.default_runner_name);
```

### Load producer settings

```rust
use rrq_config::load_producer_settings;

let producer_settings = load_producer_settings(Some("rrq.toml"))?;
println!("Queue: {}", producer_settings.queue_name);
```

### Default config path

```rust
use rrq_config::load_toml_settings;

// Looks for RRQ_CONFIG env var, then ./rrq.toml
let settings = load_toml_settings(None)?;
```

## Configuration Format

```toml
[rrq]
redis_dsn = "redis://localhost:6379/0"
default_runner_name = "python"
default_job_timeout_seconds = 300
default_max_retries = 3

[rrq.runners.python]
type = "socket"
cmd = ["rrq-runner", "--settings", "myapp.runner:settings"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4
max_in_flight = 10

[[rrq.cron_jobs]]
function_name = "cleanup"
schedule = "0 0 * * * *"
```

## Related Crates

| Crate | Purpose |
|-------|---------|
| [`rrq`](https://crates.io/crates/rrq) | Orchestrator |
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Job producer |

## License

Apache-2.0
