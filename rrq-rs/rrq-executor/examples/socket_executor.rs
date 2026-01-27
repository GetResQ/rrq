use rrq_executor::{ExecutionOutcome, ExecutorRuntime, Registry, ENV_EXECUTOR_SOCKET};

#[cfg(feature = "otel")]
use rrq_executor::telemetry::otel::{init_tracing, OtelTelemetry};
#[cfg(not(feature = "otel"))]
use rrq_executor::telemetry::NoopTelemetry;
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

    let socket_path = std::env::var(ENV_EXECUTOR_SOCKET)?;
    runtime.run_socket_with(&registry, socket_path, &telemetry)
}
