use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use std::time::Instant;

use tracing::Instrument;

use crate::telemetry::{NoopTelemetry, Telemetry};
use crate::types::{ExecutionOutcome, ExecutionRequest, OutcomeStatus};

#[async_trait]
pub trait Handler: Send + Sync {
    async fn handle(&self, request: ExecutionRequest) -> ExecutionOutcome;
}

struct FnHandler<F>(F);

#[async_trait]
impl<F, Fut> Handler for FnHandler<F>
where
    F: Fn(ExecutionRequest) -> Fut + Send + Sync,
    Fut: Future<Output = ExecutionOutcome> + Send,
{
    async fn handle(&self, request: ExecutionRequest) -> ExecutionOutcome {
        (self.0)(request).await
    }
}

#[derive(Clone, Default)]
pub struct Registry {
    handlers: HashMap<String, Arc<dyn Handler>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn register<F, Fut>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(ExecutionRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ExecutionOutcome> + Send + 'static,
    {
        let handler = Arc::new(FnHandler(handler)) as Arc<dyn Handler>;
        self.handlers.insert(name.into(), handler);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(name).cloned()
    }

    pub async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        let telemetry = NoopTelemetry;
        self.execute_with(request, &telemetry).await
    }

    pub async fn execute_with<T: Telemetry + ?Sized>(
        &self,
        request: ExecutionRequest,
        telemetry: &T,
    ) -> ExecutionOutcome {
        let span = telemetry.executor_span(&request);
        let start = Instant::now();
        let function_name = request.function_name.clone();
        let outcome = match self.get(&function_name) {
            Some(handler) => handler.handle(request).instrument(span.clone()).await,
            None => ExecutionOutcome::handler_not_found(
                request.job_id.clone(),
                format!("No handler registered for function '{}'", function_name),
            ),
        };
        record_outcome(&span, &outcome, start.elapsed());
        outcome
    }
}

fn record_outcome(span: &tracing::Span, outcome: &ExecutionOutcome, duration: std::time::Duration) {
    let duration_ms = duration.as_secs_f64() * 1000.0;
    span.record("rrq.duration_ms", duration_ms);
    match outcome.status {
        OutcomeStatus::Success => {
            span.record("rrq.outcome", "success");
        }
        OutcomeStatus::Retry => {
            span.record("rrq.outcome", "retry");
            if let Some(delay) = outcome.retry_after_seconds {
                span.record("rrq.retry_delay_ms", delay * 1000.0);
            }
        }
        OutcomeStatus::Timeout => {
            span.record("rrq.outcome", "timeout");
        }
        OutcomeStatus::Error => {
            span.record("rrq.outcome", "error");
        }
    }
    if let Some(message) = outcome.error_message.as_ref() {
        span.record("rrq.error_message", message.as_str());
    }
    if let Some(error_type) = outcome.error_type.as_ref() {
        span.record("rrq.error_type", error_type.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExecutionOutcome, ExecutionRequest, OutcomeStatus};
    use serde_json::json;

    #[tokio::test]
    async fn registry_invokes_handler() {
        let mut registry = Registry::new();
        registry.register("echo", |request| async move {
            ExecutionOutcome::success(request.job_id.clone(), json!({ "job_id": request.job_id }))
        });

        let handler = registry.get("echo").expect("handler not found");
        let payload = json!({
            "job_id": "job-1",
            "function_name": "echo",
            "args": [],
            "kwargs": {},
            "context": {
                "job_id": "job-1",
                "attempt": 1,
                "enqueue_time": "2024-01-01T00:00:00Z",
                "queue_name": "default",
                "deadline": null,
                "trace_context": null,
                "worker_id": null
            }
        });
        let request: ExecutionRequest = serde_json::from_value(payload).unwrap();

        let outcome = handler.handle(request).await;
        assert_eq!(outcome.status, OutcomeStatus::Success);
        assert_eq!(outcome.result, Some(json!({ "job_id": "job-1" })));
    }

    #[tokio::test]
    async fn registry_execute_with_noop_telemetry() {
        let mut registry = Registry::new();
        registry.register("echo", |request| async move {
            ExecutionOutcome::success(request.job_id.clone(), json!({ "job_id": request.job_id }))
        });

        let payload = json!({
            "job_id": "job-2",
            "function_name": "echo",
            "args": [],
            "kwargs": {},
            "context": {
                "job_id": "job-2",
                "attempt": 1,
                "enqueue_time": "2024-01-01T00:00:00Z",
                "queue_name": "default",
                "deadline": null,
                "trace_context": null,
                "worker_id": null
            }
        });
        let request: ExecutionRequest = serde_json::from_value(payload).unwrap();

        let outcome = registry.execute_with(request, &NoopTelemetry).await;
        assert_eq!(outcome.status, OutcomeStatus::Success);
    }

    #[tokio::test]
    async fn registry_execute_handler_not_found() {
        let registry = Registry::new();
        let payload = json!({
            "job_id": "job-3",
            "function_name": "missing",
            "args": [],
            "kwargs": {},
            "context": {
                "job_id": "job-3",
                "attempt": 1,
                "enqueue_time": "2024-01-01T00:00:00Z",
                "queue_name": "default",
                "deadline": null,
                "trace_context": null,
                "worker_id": null
            }
        });
        let request: ExecutionRequest = serde_json::from_value(payload).unwrap();

        let outcome = registry.execute_with(request, &NoopTelemetry).await;
        assert_eq!(outcome.status, OutcomeStatus::Error);
        assert_eq!(outcome.error_type.as_deref(), Some("handler_not_found"));
    }
}
