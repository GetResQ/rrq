use crate::types::ExecutionRequest;
use tracing::Span;

pub trait Telemetry: Send + Sync {
    fn runner_span(&self, request: &ExecutionRequest) -> Span;
    fn clone_box(&self) -> Box<dyn Telemetry>;
}

impl Clone for Box<dyn Telemetry> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Clone, Default)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn runner_span(&self, _request: &ExecutionRequest) -> Span {
        Span::none()
    }

    fn clone_box(&self) -> Box<dyn Telemetry> {
        Box::new(self.clone())
    }
}

#[cfg(not(feature = "otel"))]
pub fn record_runner_inflight_delta(_delta: i64) {}

#[cfg(not(feature = "otel"))]
pub fn record_runner_channel_pressure(_pressure: usize) {}

#[cfg(not(feature = "otel"))]
pub fn record_deadline_expired() {}

#[cfg(not(feature = "otel"))]
pub fn record_cancellation(_scope: &str) {}

#[cfg(feature = "otel")]
pub fn record_runner_inflight_delta(delta: i64) {
    otel::record_runner_inflight_delta(delta);
}

#[cfg(feature = "otel")]
pub fn record_runner_channel_pressure(pressure: usize) {
    otel::record_runner_channel_pressure(pressure);
}

#[cfg(feature = "otel")]
pub fn record_deadline_expired() {
    otel::record_deadline_expired();
}

#[cfg(feature = "otel")]
pub fn record_cancellation(scope: &str) {
    otel::record_cancellation(scope);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ExecutionContext, ExecutionRequest};

    fn build_request() -> ExecutionRequest {
        ExecutionRequest {
            protocol_version: "2".to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: "handler".to_string(),
            params: std::collections::HashMap::new(),
            context: ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                worker_id: None,
            },
        }
    }

    #[test]
    fn noop_telemetry_clone_box_works() {
        let telemetry: Box<dyn Telemetry> = Box::new(NoopTelemetry);
        let cloned = telemetry.clone();
        let request = build_request();
        let span = cloned.runner_span(&request);
        let _guard = span.enter();
    }
}

#[cfg(feature = "otel")]
pub mod otel {
    use std::collections::HashMap;
    use std::sync::OnceLock;

    use chrono::Utc;
    use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
    use opentelemetry::propagation::Extractor;
    use opentelemetry::{KeyValue, global};
    use tracing::Span;
    use tracing::field::Empty;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use crate::types::ExecutionRequest;

    use super::Telemetry;

    static RUNNER_METRICS: OnceLock<RunnerMetrics> = OnceLock::new();
    static METER_PROVIDER: OnceLock<opentelemetry_sdk::metrics::SdkMeterProvider> = OnceLock::new();
    static RUNNER_LABEL: OnceLock<String> = OnceLock::new();

    struct RunnerMetrics {
        runner_inflight: UpDownCounter<i64>,
        runner_channel_pressure: Histogram<f64>,
        deadline_expired_total: Counter<u64>,
        cancellations_total: Counter<u64>,
    }

    impl RunnerMetrics {
        fn new(meter: &Meter) -> Self {
            Self {
                runner_inflight: meter.i64_up_down_counter("rrq_runner_inflight").build(),
                runner_channel_pressure: meter.f64_histogram("rrq_runner_channel_pressure").build(),
                deadline_expired_total: meter.u64_counter("rrq_deadline_expired_total").build(),
                cancellations_total: meter.u64_counter("rrq_cancellations_total").build(),
            }
        }
    }

    fn runner_label() -> &'static str {
        RUNNER_LABEL
            .get_or_init(|| {
                std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "rrq-runner".to_string())
            })
            .as_str()
    }

    pub fn record_runner_inflight_delta(delta: i64) {
        if delta == 0 {
            return;
        }
        let Some(metrics) = RUNNER_METRICS.get() else {
            return;
        };
        metrics.runner_inflight.add(
            delta,
            &[KeyValue::new("runner", runner_label().to_string())],
        );
    }

    pub fn record_runner_channel_pressure(pressure: usize) {
        let Some(metrics) = RUNNER_METRICS.get() else {
            return;
        };
        metrics.runner_channel_pressure.record(
            pressure as f64,
            &[KeyValue::new("runner", runner_label().to_string())],
        );
    }

    pub fn record_deadline_expired() {
        let Some(metrics) = RUNNER_METRICS.get() else {
            return;
        };
        metrics
            .deadline_expired_total
            .add(1, &[KeyValue::new("runner", runner_label().to_string())]);
    }

    pub fn record_cancellation(scope: &str) {
        let Some(metrics) = RUNNER_METRICS.get() else {
            return;
        };
        metrics.cancellations_total.add(
            1,
            &[
                KeyValue::new("runner", runner_label().to_string()),
                KeyValue::new("scope", scope.to_string()),
            ],
        );
    }

    pub struct OtelTelemetry;

    impl Default for OtelTelemetry {
        fn default() -> Self {
            Self
        }
    }

    impl Telemetry for OtelTelemetry {
        fn runner_span(&self, request: &ExecutionRequest) -> Span {
            let queue_wait_ms = Utc::now()
                .signed_duration_since(request.context.enqueue_time)
                .num_milliseconds()
                .max(0) as f64;
            let span = tracing::info_span!(
                "rrq.runner",
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
                "rrq.deadline_remaining_ms" = Empty,
                "rrq.queue_wait_ms" = queue_wait_ms,
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
                let remaining_ms = deadline
                    .signed_duration_since(Utc::now())
                    .num_milliseconds();
                span.record("rrq.deadline_remaining_ms", (remaining_ms.max(0)) as f64);
            }
            if let Some(trace_context) = &request.context.trace_context {
                let parent = opentelemetry::global::get_text_map_propagator(|prop| {
                    prop.extract(&HashMapExtractor(trace_context))
                });
                let _ = span.set_parent(parent);
            }

            span
        }

        fn clone_box(&self) -> Box<dyn Telemetry> {
            Box::new(Self)
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
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::Resource;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        opentelemetry::global::set_text_map_propagator(
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
        let metrics_error = init_metrics_provider(service_name).err();
        tracing_subscriber::registry()
            .with(otel_layer)
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;
        if let Some(error) = metrics_error {
            tracing::warn!(error = %error, "OpenTelemetry metrics exporter failed to initialize");
        }

        Ok(())
    }

    fn init_metrics_provider(service_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()?;
        let resource = opentelemetry_sdk::Resource::builder()
            .with_service_name(service_name.to_string())
            .build();
        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(exporter)
            .build();
        global::set_meter_provider(meter_provider.clone());
        let meter = global::meter("rrq.runner");
        let _ = RUNNER_METRICS.set(RunnerMetrics::new(&meter));
        let _ = RUNNER_LABEL.set(service_name.to_string());
        let _ = METER_PROVIDER.set(meter_provider);
        Ok(())
    }
}
