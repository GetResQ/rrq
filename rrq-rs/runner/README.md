# rrq-runner

[![Crates.io](https://img.shields.io/crates/v/rrq-runner.svg)](https://crates.io/crates/rrq-runner)
[![Documentation](https://docs.rs/rrq-runner/badge.svg)](https://docs.rs/rrq-runner)
[![License](https://img.shields.io/crates/l/rrq-runner.svg)](LICENSE)

A Rust runtime for building [RRQ](https://crates.io/crates/rrq) job runners. This crate handles the socket protocol, connection management, and job dispatching—you just implement your handlers.

## Features

- **TCP socket support** for orchestrator communication
- **Concurrent job execution** with configurable parallelism
- **Graceful cancellation** of in-flight jobs
- **OpenTelemetry integration** for distributed tracing (optional)
- **Handler registry** for routing jobs to functions
- **Async/await native** with Tokio runtime

## Installation

```toml
[dependencies]
rrq-runner = "0.9"
```

With OpenTelemetry tracing:

```toml
[dependencies]
rrq-runner = { version = "0.9", features = ["otel"] }
```

## Quick Start

Create a simple runner with one handler:

```rust
use rrq_runner::{parse_tcp_socket, run_tcp, ExecutionOutcome, Registry};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create handler registry
    let mut registry = Registry::new();

    // Register a handler
    registry.register("greet", |request| async move {
        let name = request.args.get(0)
            .and_then(|v| v.as_str())
            .unwrap_or("World");

        ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            json!({ "message": format!("Hello, {}!", name) }),
        )
    });

    // Run the runner (reads address from RRQ_RUNNER_TCP_SOCKET)
    let addr = std::env::var("RRQ_RUNNER_TCP_SOCKET")?;
    let socket_addr = parse_tcp_socket(&addr)?;
    run_tcp(&registry, socket_addr)
}
```

## Handler Functions

Handlers receive an `ExecutionRequest` and return an `ExecutionOutcome`:

```rust
use rrq_runner::{ExecutionOutcome, Registry};
use rrq_protocol::ExecutionRequest;

let mut registry = Registry::new();

// Async handler with full request access
registry.register("process_order", |request: ExecutionRequest| async move {
    // Access job metadata
    println!("Job ID: {}", request.job_id);
    println!("Attempt: {}", request.context.attempt);
    println!("Queue: {}", request.context.queue_name);

    // Access arguments
    let order_id = request.args.get(0)
        .and_then(|v| v.as_str())
        .ok_or("missing order_id")?;

    // Access keyword arguments
    let priority = request.kwargs.get("priority")
        .and_then(|v| v.as_str())
        .unwrap_or("normal");

    // Do work...

    ExecutionOutcome::success(
        request.job_id.clone(),
        request.request_id.clone(),
        serde_json::json!({ "processed": order_id }),
    )
});
```

## Outcome Types

Return different outcomes based on execution result:

```rust
use rrq_runner::ExecutionOutcome;

// Success - job completed
ExecutionOutcome::success(job_id, request_id, json!({"result": "ok"}))

// Failure - job failed, may be retried based on retry policy
ExecutionOutcome::failure(job_id, request_id, "Something went wrong".to_string())

// Retry after delay - explicitly request retry after N seconds
ExecutionOutcome::retry_after(job_id, request_id, "Rate limited".to_string(), 60)

// Timeout - job exceeded deadline
ExecutionOutcome::timeout(job_id, request_id)

// Cancelled - job was cancelled
ExecutionOutcome::cancelled(job_id, request_id)
```

## Custom Runtime

For more control, create your own `RunnerRuntime`:

```rust
use rrq_runner::{RunnerRuntime, Registry, ENV_RUNNER_TCP_SOCKET, parse_tcp_socket};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create runtime (initializes Tokio)
    let runtime = RunnerRuntime::new()?;

    let mut registry = Registry::new();
    registry.register("echo", |req| async move {
        ExecutionOutcome::success(
            req.job_id.clone(),
            req.request_id.clone(),
            serde_json::json!(req.args),
        )
    });

    let addr = std::env::var(ENV_RUNNER_TCP_SOCKET)?;
    let socket_addr = parse_tcp_socket(&addr)?;
    runtime.run_tcp(&registry, socket_addr)
}
```

## OpenTelemetry Tracing

Enable the `otel` feature for distributed tracing:

```rust
use rrq_runner::{RunnerRuntime, Registry, ENV_RUNNER_TCP_SOCKET, parse_tcp_socket};
use rrq_runner::telemetry::otel::{init_tracing, OtelTelemetry};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RunnerRuntime::new()?;

    // Initialize OpenTelemetry (requires runtime context)
    {
        let _guard = runtime.enter();
        init_tracing("my-runner")?;
    }

    let telemetry = OtelTelemetry;
    let mut registry = Registry::new();

    registry.register("traced_handler", |request| async move {
        // This handler will be traced with parent context from job
        ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            serde_json::json!({"traced": true}),
        )
    });

    let addr = std::env::var(ENV_RUNNER_TCP_SOCKET)?;
    let socket_addr = parse_tcp_socket(&addr)?;
    runtime.run_tcp_with(&registry, socket_addr, &telemetry)
}
```

Configure via standard OpenTelemetry environment variables:
- `OTEL_EXPORTER_OTLP_ENDPOINT` - OTLP collector endpoint
- `OTEL_SERVICE_NAME` - Service name for traces

## Configuration in rrq.toml

Point the RRQ orchestrator to your runner:

```toml
[rrq.runners.rust]
type = "socket"
cmd = ["./target/release/my-runner"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4
max_in_flight = 10
```

## Environment Variables

Set by the orchestrator when spawning runners:

| Variable | Description |
|----------|-------------|
| `RRQ_RUNNER_TCP_SOCKET` | TCP socket address for communication |

## Example Project Structure

```
my-runner/
├── Cargo.toml
└── src/
    └── main.rs
```

```toml
# Cargo.toml
[package]
name = "my-runner"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "my-runner"

[dependencies]
rrq-runner = "0.9"
serde_json = "1.0"
```

## Related Crates

| Crate | Description |
|-------|-------------|
| [`rrq`](https://crates.io/crates/rrq) | Job queue orchestrator |
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Client for enqueuing jobs |
| [`rrq-protocol`](https://crates.io/crates/rrq-protocol) | Protocol definitions |

## License

Apache-2.0
