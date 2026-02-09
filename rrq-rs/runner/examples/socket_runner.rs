use rrq_runner::{ExecutionOutcome, Registry, RunnerRuntime, parse_tcp_socket};

#[cfg(not(feature = "otel"))]
use rrq_runner::telemetry::NoopTelemetry;
#[cfg(feature = "otel")]
use rrq_runner::telemetry::otel::{OtelTelemetry, init_tracing};
use serde_json::json;

fn read_arg_value(args: &[String], name: &str) -> Option<String> {
    for (i, arg) in args.iter().enumerate() {
        if arg == name {
            return args.get(i + 1).cloned();
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RunnerRuntime::new()?;

    #[cfg(feature = "otel")]
    {
        let _guard = runtime.enter();
        init_tracing("rrq-runner")?;
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
                "params": request.params,
            }),
        )
    });

    let args: Vec<String> = std::env::args().skip(1).collect();
    let tcp_socket = read_arg_value(&args, "--tcp-socket").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--tcp-socket must be provided",
        )
    })?;
    let addr = parse_tcp_socket(&tcp_socket)?;
    runtime.run_tcp_with(&registry, addr, &telemetry)
}
