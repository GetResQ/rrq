# rrq-executor

Reference Rust stdio executor for RRQ.

## Git dependency

```toml
[dependencies]
rrq-executor = { git = "https://github.com/YOUR_ORG/rrq", package = "rrq-executor", rev = "<sha>" }
```

## Example

```
cd reference/rust/rrq-executor
cargo run --example stdio_executor
```

## Tracing (optional)

Enable OpenTelemetry spans in the executor with the `otel` feature:

```
cargo run --example stdio_executor --features otel
```

This will emit executor spans using the trace context passed from RRQ. Configure
your OTLP endpoint via standard OpenTelemetry environment variables (for
example `OTEL_EXPORTER_OTLP_ENDPOINT`).

If you call `init_tracing` yourself, do it after a Tokio runtime is available
(or inside `runtime.enter()`), since the OTLP pipeline installs async exporters.
