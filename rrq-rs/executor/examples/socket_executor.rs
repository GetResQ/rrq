use rrq_executor::{
    ENV_EXECUTOR_TCP_SOCKET, ExecutionOutcome, ExecutorRuntime, Registry, parse_tcp_socket,
};

#[cfg(not(feature = "otel"))]
use rrq_executor::telemetry::NoopTelemetry;
#[cfg(feature = "otel")]
use rrq_executor::telemetry::otel::{OtelTelemetry, init_tracing};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = ExecutorRuntime::new()?;

    #[cfg(feature = "otel")]
    {
        let _guard = runtime.enter();
        init_tracing("rrq-executor")?;
    }

    #[cfg(feature = "otel")]
    let telemetry = OtelTelemetry;
    #[cfg(not(feature = "otel"))]
    let telemetry = NoopTelemetry;

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

    let tcp_socket = std::env::var(ENV_EXECUTOR_TCP_SOCKET).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RRQ_EXECUTOR_TCP_SOCKET must be set",
        )
    })?;
    let addr = parse_tcp_socket(&tcp_socket)?;
    runtime.run_tcp_with(&registry, addr, &telemetry)
}
