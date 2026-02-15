use crate::types::ExecutionRequest;
use crate::types::OutcomeStatus;
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

#[cfg(not(feature = "otel"))]
pub fn record_job_outcome(
    _function_name: &str,
    _outcome: OutcomeStatus,
    _duration: std::time::Duration,
) {
}

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

#[cfg(feature = "otel")]
pub fn record_job_outcome(
    function_name: &str,
    outcome: OutcomeStatus,
    duration: std::time::Duration,
) {
    otel::record_job_outcome(function_name, outcome, duration);
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
                correlation_context: None,
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
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::{KeyValue, global};
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader as AsyncPeriodicReader;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
    use rrq_config::{OtlpEnvConfig, OtlpGlobalEndpointStyle, OtlpSignal, resolve_otlp_env};
    use tracing::Span;
    use tracing::field::Empty;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use crate::types::ExecutionRequest;
    use crate::types::OutcomeStatus;

    use super::Telemetry;

    static RUNNER_METRICS: OnceLock<RunnerMetrics> = OnceLock::new();
    static METRICS_ENDPOINT_CONFIGURED: OnceLock<bool> = OnceLock::new();
    static TRACE_PROVIDER: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> = OnceLock::new();
    static LOG_PROVIDER: OnceLock<opentelemetry_sdk::logs::SdkLoggerProvider> = OnceLock::new();
    static METER_PROVIDER: OnceLock<opentelemetry_sdk::metrics::SdkMeterProvider> = OnceLock::new();
    static RUNNER_LABEL: OnceLock<String> = OnceLock::new();

    #[derive(Clone)]
    struct RunnerMetrics {
        runner_inflight: UpDownCounter<i64>,
        runner_channel_pressure: Histogram<f64>,
        deadline_expired_total: Counter<u64>,
        cancellations_total: Counter<u64>,
        runner_jobs_total: Counter<u64>,
        runner_job_duration_ms: Histogram<f64>,
    }

    impl RunnerMetrics {
        fn new(meter: &Meter) -> Self {
            Self {
                runner_inflight: meter.i64_up_down_counter("rrq_runner_inflight").build(),
                runner_channel_pressure: meter.f64_histogram("rrq_runner_channel_pressure").build(),
                deadline_expired_total: meter.u64_counter("rrq_deadline_expired_total").build(),
                cancellations_total: meter.u64_counter("rrq_cancellations_total").build(),
                runner_jobs_total: meter.u64_counter("rrq_runner_jobs_total").build(),
                runner_job_duration_ms: meter.f64_histogram("rrq_runner_job_duration_ms").build(),
            }
        }
    }

    fn runner_label() -> &'static str {
        RUNNER_LABEL
            .get_or_init(|| match std::env::var("OTEL_SERVICE_NAME") {
                Ok(value) if !value.is_empty() => value,
                _ => std::env::var("SERVICE_NAME").unwrap_or_else(|_| "rrq-runner".to_string()),
            })
            .as_str()
    }

    fn metrics_endpoint_configured() -> bool {
        *METRICS_ENDPOINT_CONFIGURED.get_or_init(|| {
            resolve_otlp_env(OtlpGlobalEndpointStyle::AppendHttpSignalPath)
                .signal(OtlpSignal::Metrics)
                .endpoint
                .is_some()
        })
    }

    fn runner_metrics() -> Option<RunnerMetrics> {
        if let Some(metrics) = RUNNER_METRICS.get() {
            return Some(metrics.clone());
        }
        if !metrics_endpoint_configured() {
            return None;
        }

        // Build transient instruments from the current global provider without
        // caching. This avoids permanently binding RUNNER_METRICS to the
        // process-default no-op provider before init_metrics_provider() runs.
        let meter = global::meter("rrq.runner");
        Some(RunnerMetrics::new(&meter))
    }

    pub fn record_runner_inflight_delta(delta: i64) {
        if delta == 0 {
            return;
        }
        let Some(metrics) = runner_metrics() else {
            return;
        };
        metrics.runner_inflight.add(
            delta,
            &[KeyValue::new("runner", runner_label().to_string())],
        );
    }

    pub fn record_runner_channel_pressure(pressure: usize) {
        let Some(metrics) = runner_metrics() else {
            return;
        };
        metrics.runner_channel_pressure.record(
            pressure as f64,
            &[KeyValue::new("runner", runner_label().to_string())],
        );
    }

    pub fn record_deadline_expired() {
        let Some(metrics) = runner_metrics() else {
            return;
        };
        metrics
            .deadline_expired_total
            .add(1, &[KeyValue::new("runner", runner_label().to_string())]);
    }

    pub fn record_cancellation(scope: &str) {
        let Some(metrics) = runner_metrics() else {
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

    pub fn record_job_outcome(
        function_name: &str,
        outcome: OutcomeStatus,
        duration: std::time::Duration,
    ) {
        let Some(metrics) = runner_metrics() else {
            return;
        };
        let outcome = match outcome {
            OutcomeStatus::Success => "success",
            OutcomeStatus::Retry => "retry",
            OutcomeStatus::Timeout => "timeout",
            OutcomeStatus::Error => "error",
        };
        let attrs = [
            KeyValue::new("runner", runner_label().to_string()),
            KeyValue::new("outcome", outcome.to_string()),
            KeyValue::new("function", function_name.to_string()),
        ];
        metrics.runner_jobs_total.add(1, &attrs);
        metrics
            .runner_job_duration_ms
            .record(duration.as_secs_f64() * 1000.0, &attrs);
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
            if let Some(correlation_context) = &request.context.correlation_context {
                let span_context = span.context();
                let otel_span = span_context.span();
                for (key, value) in correlation_context {
                    if key.is_empty() || value.is_empty() {
                        continue;
                    }
                    otel_span.set_attribute(KeyValue::new(key.clone(), value.clone()));
                }
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
        use opentelemetry_sdk::Resource;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let otlp = resolve_otlp_env(OtlpGlobalEndpointStyle::AppendHttpSignalPath);
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let resource = Resource::builder()
            .with_service_name(service_name.to_string())
            .build();
        let (trace_layer, trace_error) = init_trace_layer(service_name, resource.clone(), &otlp);
        let (log_layer, logs_error) = init_logs_layer(resource.clone(), &otlp);
        let metrics_error = init_metrics_provider(service_name, resource, &otlp).err();
        match (trace_layer, log_layer) {
            (Some(trace_layer), Some(log_layer)) => tracing_subscriber::registry()
                .with(trace_layer)
                .with(log_layer)
                .with(tracing_subscriber::fmt::layer())
                .try_init()?,
            (Some(trace_layer), None) => tracing_subscriber::registry()
                .with(trace_layer)
                .with(tracing_subscriber::fmt::layer())
                .try_init()?,
            (None, Some(log_layer)) => tracing_subscriber::registry()
                .with(log_layer)
                .with(tracing_subscriber::fmt::layer())
                .try_init()?,
            (None, None) => tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .try_init()?,
        };
        warn_if_global_otlp_endpoint_only(&otlp);
        if let Some(error) = trace_error {
            tracing::warn!(error = %error, "OpenTelemetry tracing exporter failed to initialize");
        }
        if let Some(error) = metrics_error {
            tracing::warn!(error = %error, "OpenTelemetry metrics exporter failed to initialize");
        }
        if let Some(error) = logs_error {
            tracing::warn!(error = %error, "OpenTelemetry logs exporter failed to initialize");
        }

        Ok(())
    }

    fn init_trace_layer(
        service_name: &str,
        resource: opentelemetry_sdk::Resource,
        otlp: &OtlpEnvConfig,
    ) -> (
        Option<
            tracing_opentelemetry::OpenTelemetryLayer<
                tracing_subscriber::Registry,
                opentelemetry_sdk::trace::Tracer,
            >,
        >,
        Option<String>,
    ) {
        let Some(endpoint) = otlp.signal(OtlpSignal::Traces).endpoint.as_deref() else {
            return (None, None);
        };

        let exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint.to_string())
            .with_headers(otlp.signal(OtlpSignal::Traces).headers.clone())
            .build()
        {
            Ok(exporter) => exporter,
            Err(err) => return (None, Some(err.to_string())),
        };
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(resource)
            .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
            .build();
        let tracer = provider.tracer(service_name.to_string());
        let _ = TRACE_PROVIDER.set(provider.clone());
        opentelemetry::global::set_tracer_provider(provider);
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        (Some(otel_layer), None)
    }

    fn init_metrics_provider(
        service_name: &str,
        resource: opentelemetry_sdk::Resource,
        otlp: &OtlpEnvConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(endpoint) = otlp.signal(OtlpSignal::Metrics).endpoint.as_deref() else {
            return Ok(());
        };
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint.to_string())
            .with_headers(otlp.signal(OtlpSignal::Metrics).headers.clone())
            .build()?;
        let reader = AsyncPeriodicReader::builder(exporter, runtime::Tokio).build();
        let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();
        global::set_meter_provider(meter_provider.clone());
        let meter = global::meter("rrq.runner");
        let _ = RUNNER_METRICS.set(RunnerMetrics::new(&meter));
        let _ = RUNNER_LABEL.set(service_name.to_string());
        let _ = METER_PROVIDER.set(meter_provider);
        Ok(())
    }

    fn init_logs_layer(
        resource: opentelemetry_sdk::Resource,
        otlp: &OtlpEnvConfig,
    ) -> (
        Option<
            OpenTelemetryTracingBridge<
                opentelemetry_sdk::logs::SdkLoggerProvider,
                opentelemetry_sdk::logs::SdkLogger,
            >,
        >,
        Option<String>,
    ) {
        let Some(endpoint) = otlp.signal(OtlpSignal::Logs).endpoint.as_deref() else {
            return (None, None);
        };

        let exporter = match opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(endpoint.to_string())
            .with_headers(otlp.signal(OtlpSignal::Logs).headers.clone())
            .build()
        {
            Ok(exporter) => exporter,
            Err(err) => return (None, Some(err.to_string())),
        };
        let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_resource(resource)
            .with_log_processor(
                opentelemetry_sdk::logs::log_processor_with_async_runtime::BatchLogProcessor::builder(
                    exporter,
                    opentelemetry_sdk::runtime::Tokio,
                )
                .build(),
            )
            .build();
        let _ = LOG_PROVIDER.set(provider);
        let Some(provider_ref) = LOG_PROVIDER.get() else {
            return (
                None,
                Some("failed to initialize OpenTelemetry logger provider".to_string()),
            );
        };
        let layer = OpenTelemetryTracingBridge::new(provider_ref);
        (Some(layer), None)
    }

    fn warn_if_global_otlp_endpoint_only(otlp: &OtlpEnvConfig) {
        if !otlp.has_global_endpoint() {
            return;
        }
        let signal_overrides = otlp.explicit_signal_endpoint_count();
        if signal_overrides < 3 {
            tracing::debug!(
                signal_overrides,
                "using OTEL_EXPORTER_OTLP_ENDPOINT fallback for signals without explicit OTEL_EXPORTER_OTLP_{{TRACES|METRICS|LOGS}}_ENDPOINT"
            );
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;
        use std::sync::{Mutex, OnceLock};

        use opentelemetry_sdk::Resource;
        use rrq_config::OtlpSignalConfig;

        use super::*;

        static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

        fn test_lock() -> std::sync::MutexGuard<'static, ()> {
            TEST_MUTEX
                .get_or_init(|| Mutex::new(()))
                .lock()
                .expect("test mutex poisoned")
        }

        #[test]
        fn early_metric_emission_does_not_prebind_metrics_cache() {
            let _guard = test_lock();
            assert!(
                METER_PROVIDER.get().is_none(),
                "test requires uninitialized meter provider"
            );
            assert!(
                RUNNER_METRICS.get().is_none(),
                "test requires empty runner metrics cache"
            );

            // Emit before metrics provider init to model startup ordering where
            // runtime counters may fire before init_tracing().
            record_deadline_expired();
            assert!(
                RUNNER_METRICS.get().is_none(),
                "early metric emission must not cache no-op instruments"
            );
            assert!(
                METRICS_ENDPOINT_CONFIGURED.get().is_some(),
                "metrics endpoint configuration should be cached after first emission"
            );

            let otlp = OtlpEnvConfig {
                metrics: OtlpSignalConfig {
                    endpoint: Some("http://127.0.0.1:4318/v1/metrics".to_string()),
                    headers: HashMap::new(),
                    has_explicit_endpoint_env: true,
                },
                ..OtlpEnvConfig::default()
            };
            let resource = Resource::builder()
                .with_service_name("rrq-runner-test".to_string())
                .build();

            init_metrics_provider("rrq-runner-test", resource, &otlp)
                .expect("metrics provider should initialize");

            assert!(
                METER_PROVIDER.get().is_some(),
                "metrics provider should be cached after init"
            );
            assert!(
                RUNNER_METRICS.get().is_some(),
                "runner metrics cache should initialize after provider setup"
            );
        }
    }
}
