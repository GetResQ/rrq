use crate::types::ExecutionRequest;
use tracing::Span;

pub trait Telemetry: Send + Sync {
    fn executor_span(&self, request: &ExecutionRequest) -> Span;
}

#[derive(Clone, Default)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn executor_span(&self, _request: &ExecutionRequest) -> Span {
        Span::none()
    }
}

#[cfg(feature = "otel")]
pub mod otel {
    use std::collections::HashMap;

    use opentelemetry::propagation::Extractor;
    use tracing::Span;
    use tracing::field::Empty;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use crate::types::ExecutionRequest;

    use super::Telemetry;

    pub struct OtelTelemetry;

    impl Default for OtelTelemetry {
        fn default() -> Self {
            Self
        }
    }

    impl Telemetry for OtelTelemetry {
        fn executor_span(&self, request: &ExecutionRequest) -> Span {
            let span = tracing::info_span!(
                "rrq.executor",
                "span.kind" = "consumer",
                "messaging.system" = "redis",
                "messaging.destination.name" = %request.context.queue_name,
                "messaging.destination_kind" = "queue",
                "messaging.operation" = "process",
                "rrq.job_id" = %request.job_id,
                "rrq.function" = %request.function_name,
                "rrq.queue" = %request.context.queue_name,
                "rrq.attempt" = request.context.attempt,
                "rrq.worker_id" = Empty,
                "rrq.deadline" = Empty,
                "rrq.outcome" = Empty,
                "rrq.duration_ms" = Empty,
                "rrq.retry_delay_ms" = Empty,
                "rrq.error_message" = Empty,
                "rrq.error_type" = Empty,
            );

            if let Some(worker_id) = &request.context.worker_id {
                span.record("rrq.worker_id", worker_id.as_str());
            }
            if let Some(deadline) = &request.context.deadline {
                let deadline_str = deadline.to_rfc3339();
                span.record("rrq.deadline", deadline_str.as_str());
            }
            if let Some(trace_context) = &request.context.trace_context {
                let parent = opentelemetry::global::get_text_map_propagator(|prop| {
                    prop.extract(&HashMapExtractor(trace_context))
                });
                let _ = span.set_parent(parent);
            }

            span
        }
    }

    struct HashMapExtractor<'a>(&'a HashMap<String, String>);

    impl<'a> Extractor for HashMapExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(|value| value.as_str())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|key| key.as_str()).collect()
        }
    }

    pub fn init_tracing(service_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        use opentelemetry::global;
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::Resource;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()?;
        let resource = Resource::builder()
            .with_service_name(service_name.to_string())
            .build();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();

        let tracer = provider.tracer(service_name.to_string());
        opentelemetry::global::set_tracer_provider(provider);
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(otel_layer)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;

        Ok(())
    }
}
