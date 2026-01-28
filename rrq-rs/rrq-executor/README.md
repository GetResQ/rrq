# rrq-executor

Reference Rust socket executor for RRQ.

## Git dependency

```toml
[dependencies]
rrq-executor = { git = "https://github.com/getresq/rrq", package = "rrq-executor", rev = "<sha>" }
```

## Example

```bash
cd rrq-rs/rrq-executor
RRQ_EXECUTOR_SOCKET=/tmp/rrq-executor.sock cargo run --example socket_executor
```

## Build your own executor binary

This crate is designed for per-project executors. You implement business logic
handlers and let rrq-executor run the socket protocol loop.

1) Add the dependency:

```toml
[dependencies]
rrq-executor = { git = "https://github.com/getresq/rrq", package = "rrq-executor", rev = "<sha>" }
```

2) Create a small `main.rs`:

```rust
use rrq_executor::{ExecutionOutcome, ExecutorRuntime, Registry, ENV_EXECUTOR_SOCKET};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ExecutorRuntime::new()?;

    let mut registry = Registry::new();
    registry.register("echo", |request| async move {
        ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            json!({
                "job_id": request.job_id,
                "args": request.args,
                "kwargs": request.kwargs,
            }),
        )
    });

    let socket_path = std::env::var(ENV_EXECUTOR_SOCKET)?;
    runtime.run_socket(&registry, socket_path)
}
```

If you do not need tracing, you can also call `rrq_executor::run_socket(&registry, socket_path)`
to create a runtime automatically.

3) Point `rrq.toml` at your binary:

```toml
[rrq.executors.rust]
cmd = ["./target/release/myapp-executor"]
pool_size = 2
max_in_flight = 1
```

The orchestrator sets `RRQ_EXECUTOR_SOCKET` when launching the executor.
Use `max_in_flight` to allow multiple concurrent requests per executor process.

## Tracing (optional)

If you want OpenTelemetry spans, enable the `otel` feature and initialize
tracing after the Tokio runtime is available:

```toml
[dependencies]
rrq-executor = { git = "https://github.com/getresq/rrq", package = "rrq-executor", rev = "<sha>", features = ["otel"] }
```

```rust
use rrq_executor::{ExecutionOutcome, ExecutorRuntime, Registry, ENV_EXECUTOR_SOCKET};
use rrq_executor::telemetry::otel::{init_tracing, OtelTelemetry};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ExecutorRuntime::new()?;
    {
        let _guard = runtime.enter();
        init_tracing("myapp-executor")?;
    }

    let telemetry = OtelTelemetry;
    let mut registry = Registry::new();
    registry.register("echo", |request| async move {
        ExecutionOutcome::success(
            request.job_id.clone(),
            request.request_id.clone(),
            json!({ "job_id": request.job_id }),
        )
    });

    let socket_path = std::env::var(ENV_EXECUTOR_SOCKET)?;
    runtime.run_socket_with(&registry, socket_path, &telemetry)
}
```

To run the reference example with tracing enabled:

```
RRQ_EXECUTOR_SOCKET=/tmp/rrq-executor.sock cargo run --example socket_executor --features otel
```

This will emit executor spans using the trace context passed from RRQ. Configure
your OTLP endpoint via standard OpenTelemetry environment variables (for
example `OTEL_EXPORTER_OTLP_ENDPOINT`).
