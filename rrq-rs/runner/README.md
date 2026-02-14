# rrq-runner

[![Crates.io](https://img.shields.io/crates/v/rrq-runner.svg)](https://crates.io/crates/rrq-runner)
[![Documentation](https://docs.rs/rrq-runner/badge.svg)](https://docs.rs/rrq-runner)
[![License](https://img.shields.io/crates/l/rrq-runner.svg)](LICENSE)

**Rust runtime for building RRQ job handlers.** Write your job handlers in Rust and let this crate handle the socket protocol, connection management, and job dispatching.

## What is RRQ?

RRQ (Reliable Redis Queue) is a distributed job queue with a Rust orchestrator and language-flexible workers. This crate lets you build high-performance job handlers in Rust that connect to the RRQ orchestrator.

**Why Rust runners?**

- **Maximum performance** - Native code for CPU-intensive tasks
- **Memory safety** - No GC pauses, predictable latency
- **Async native** - Built on Tokio for efficient concurrency
- **Same ecosystem** - Use Rust crates in your job handlers

## Installation

```toml
[dependencies]
rrq-runner = "0.9"
```

With OpenTelemetry:

```toml
[dependencies]
rrq-runner = { version = "0.9", features = ["otel"] }
```

## Quick Start

```rust
use rrq_runner::{Registry, run_tcp, parse_tcp_socket, ExecutionOutcome};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut registry = Registry::new();

    registry.register("greet", |request| async move {
        let name = request.params.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("World");

        ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            json!({ "message": format!("Hello, {}!", name) }),
        )
    });

    let args: Vec<String> = std::env::args().skip(1).collect();
    let tcp_socket = args
        .iter()
        .position(|arg| arg == "--tcp-socket")
        .and_then(|idx| args.get(idx + 1))
        .ok_or("Missing --tcp-socket")?;
    let addr = tcp_socket.as_str();
    run_tcp(&registry, parse_tcp_socket(&addr)?)
}
```

## Handler Functions

Handlers receive an `ExecutionRequest` and return an `ExecutionOutcome`:

```rust
registry.register("process_order", |request| async move {
    // Access job metadata
    println!("Job: {}", request.job_id);
    println!("Attempt: {}", request.context.attempt);

    // Access parameters
    let order_id = request.params.get("order_id")
        .and_then(|v| v.as_str())
        .ok_or("missing order_id")?;

    // Do work...

    ExecutionOutcome::success(
        request.job_id.clone(),
        request.request_id.clone(),
        json!({ "processed": order_id }),
    )
});
```

## Outcome Types

```rust
// Success
ExecutionOutcome::success(job_id, request_id, json!({"result": "ok"}))

// Failure (may be retried)
ExecutionOutcome::failure(job_id, request_id, "Something went wrong".to_string())

// Retry after delay
ExecutionOutcome::retry_after(job_id, request_id, "Rate limited".to_string(), 60)

// Timeout
ExecutionOutcome::timeout(job_id, request_id)

// Cancelled
ExecutionOutcome::cancelled(job_id, request_id)
```

## OpenTelemetry

```rust
use rrq_runner::{RunnerRuntime, Registry, parse_tcp_socket};
use rrq_runner::telemetry::otel::{init_tracing, OtelTelemetry};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RunnerRuntime::new()?;

    // Initialize tracing
    { let _guard = runtime.enter(); init_tracing("my-runner")?; }

    let mut registry = Registry::new();
    registry.register("traced_job", |req| async move {
        ExecutionOutcome::success(req.job_id.clone(), req.request_id.clone(), json!({}))
    });

    let args: Vec<String> = std::env::args().skip(1).collect();
    let tcp_socket = args
        .iter()
        .position(|arg| arg == "--tcp-socket")
        .and_then(|idx| args.get(idx + 1))
        .ok_or("Missing --tcp-socket")?;
    runtime.run_tcp_with(&registry, parse_tcp_socket(tcp_socket)?, &OtelTelemetry)
}
```

Configure via:
- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` - Traces endpoint (highest precedence)
- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` - Metrics endpoint (highest precedence)
- `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` - Logs endpoint (highest precedence)
- `OTEL_EXPORTER_OTLP_ENDPOINT` - Fallback endpoint for any signal endpoint that is unset
- `OTEL_SERVICE_NAME` - Service name

Endpoint resolution rules:
- Signal-specific endpoint vars take precedence over `OTEL_EXPORTER_OTLP_ENDPOINT`
- If a signal-specific endpoint is unset, RRQ falls back to `OTEL_EXPORTER_OTLP_ENDPOINT`
- If a signal-specific endpoint is explicitly set to an empty value, that signal is disabled (no fallback)

## Configuration

Add your runner to `rrq.toml`:

```toml
[rrq.runners.rust]
type = "socket"
cmd = ["./target/release/my-runner"]
tcp_socket = "127.0.0.1:9000"
pool_size = 4
max_in_flight = 10
```

## Related Crates

| Crate | Purpose |
|-------|---------|
| [`rrq`](https://crates.io/crates/rrq) | Orchestrator |
| [`rrq-producer`](https://crates.io/crates/rrq-producer) | Enqueue jobs |
| [`rrq-protocol`](https://crates.io/crates/rrq-protocol) | Wire protocol |

## License

Apache-2.0
