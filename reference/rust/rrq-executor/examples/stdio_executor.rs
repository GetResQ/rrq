use rrq_executor::{ExecutionOutcome, ExecutionRequest, Registry};

#[cfg(feature = "otel")]
use rrq_executor::telemetry::otel::{init_tracing, OtelTelemetry};
#[cfg(not(feature = "otel"))]
use rrq_executor::telemetry::NoopTelemetry;
use serde_json::json;
use std::io::{self, BufRead, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;

    #[cfg(feature = "otel")]
    {
        let _guard = runtime.enter();
        init_tracing("rrq-executor")?;
    }

    #[cfg(feature = "otel")]
    let telemetry = OtelTelemetry::default();
    #[cfg(not(feature = "otel"))]
    let telemetry = NoopTelemetry::default();

    let mut registry = Registry::new();
    registry.register("echo", |request| async move {
        ExecutionOutcome::success(request.job_id.clone(), json!({
            "job_id": request.job_id,
            "args": request.args,
            "kwargs": request.kwargs,
        }))
    });

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: ExecutionRequest = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(err) => {
                let outcome =
                    ExecutionOutcome::error("unknown", format!("invalid request: {err}"));
                writeln!(stdout, "{}", serde_json::to_string(&outcome)?)?;
                stdout.flush()?;
                continue;
            }
        };

        let outcome = runtime.block_on(registry.execute_with(request, &telemetry));
        writeln!(stdout, "{}", serde_json::to_string(&outcome)?)?;
        stdout.flush()?;
    }

    Ok(())
}
