use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::propagation::{Extractor, TextMapCompositePropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, Value};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::metrics as sdkmetrics;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

static LOG_FORMAT: OnceLock<LogFormat> = OnceLock::new();
static OTEL_TRACE_PROVIDER: OnceLock<sdktrace::SdkTracerProvider> = OnceLock::new();
static OTEL_METRIC_PROVIDER: OnceLock<sdkmetrics::SdkMeterProvider> = OnceLock::new();
static RRQ_METRICS: OnceLock<RrqMetrics> = OnceLock::new();

#[derive(Clone)]
struct ResourceMetadata {
    service_name: String,
    environment: Option<String>,
    version: Option<String>,
}

struct RrqMetrics {
    poll_cycles_total: Counter<u64>,
    jobs_fetched_total: Counter<u64>,
    lock_acquire_total: Counter<u64>,
    jobs_processed_total: Counter<u64>,
    job_duration_ms: Histogram<f64>,
    queue_wait_ms: Histogram<f64>,
    orphan_recovered_total: Counter<u64>,
}

impl RrqMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            poll_cycles_total: meter.u64_counter("rrq_poll_cycles_total").build(),
            jobs_fetched_total: meter.u64_counter("rrq_jobs_fetched_total").build(),
            lock_acquire_total: meter.u64_counter("rrq_lock_acquire_total").build(),
            jobs_processed_total: meter.u64_counter("rrq_jobs_processed_total").build(),
            job_duration_ms: meter.f64_histogram("rrq_job_duration_ms").build(),
            queue_wait_ms: meter.f64_histogram("rrq_queue_wait_ms").build(),
            orphan_recovered_total: meter.u64_counter("rrq_orphan_recovered_total").build(),
        }
    }
}

pub fn log_format() -> LogFormat {
    *LOG_FORMAT.get_or_init(|| {
        let value = env::var("RUST_LOG_FORMAT").unwrap_or_else(|_| "json".to_string());
        parse_log_format(&value)
    })
}

fn parse_log_format(value: &str) -> LogFormat {
    match value.trim().to_lowercase().as_str() {
        "pretty" | "text" | "human" => LogFormat::Pretty,
        _ => LogFormat::Json,
    }
}

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let metadata = resolve_resource_metadata();
    let (otel_layer, trace_error) = init_otel_layer(&metadata);
    let metrics_error = init_metrics_provider(&metadata);
    match log_format() {
        LogFormat::Json => {
            if let Some(otel_layer) = otel_layer {
                let fmt_layer = tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_current_span(true)
                    .with_filter(filter.clone());
                tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(fmt_layer)
                    .init();
            } else {
                let fmt_layer = tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_current_span(true)
                    .with_filter(filter);
                tracing_subscriber::registry().with(fmt_layer).init();
            }
        }
        LogFormat::Pretty => {
            if let Some(otel_layer) = otel_layer {
                let fmt_layer = tracing_subscriber::fmt::layer().with_filter(filter.clone());
                tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(fmt_layer)
                    .init();
            } else {
                let fmt_layer = tracing_subscriber::fmt::layer().with_filter(filter);
                tracing_subscriber::registry().with(fmt_layer).init();
            }
        }
    }

    if let Some(error) = trace_error {
        tracing::warn!(error = %error, "OpenTelemetry tracing exporter failed to initialize");
    }
    if let Some(error) = metrics_error {
        tracing::warn!(error = %error, "OpenTelemetry metrics exporter failed to initialize");
    }
}

pub fn set_parent_from_trace_context(
    span: &tracing::Span,
    trace_context: &HashMap<String, String>,
) {
    if trace_context.is_empty() {
        return;
    }

    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HashMapExtractor(trace_context))
    });
    let _ = span.set_parent(parent);
}

fn resolve_resource_metadata() -> ResourceMetadata {
    let service_name = env_optional_nonempty("OTEL_SERVICE_NAME")
        .or_else(|| otel_resource_attribute("service.name"))
        .unwrap_or_else(|| "rrq".to_string());
    ResourceMetadata {
        service_name,
        environment: otel_resource_attribute("deployment.environment"),
        version: otel_resource_attribute("service.version"),
    }
}

fn build_resource(metadata: &ResourceMetadata) -> Resource {
    let mut resource_builder = Resource::builder().with_service_name(metadata.service_name.clone());
    if let Some(environment) = metadata.environment.as_deref() {
        resource_builder = resource_builder.with_attribute(KeyValue::new(
            "deployment.environment",
            environment.to_string(),
        ));
    }
    if let Some(version) = metadata.version.as_deref() {
        resource_builder =
            resource_builder.with_attribute(KeyValue::new("service.version", version.to_string()));
    }
    resource_builder.build()
}

fn init_otel_layer(
    metadata: &ResourceMetadata,
) -> (
    Option<
        tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::Registry, sdktrace::Tracer>,
    >,
    Option<String>,
) {
    if !otel_trace_enabled() {
        return (None, None);
    }

    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    let exporter = match build_otlp_span_exporter() {
        Ok(exporter) => exporter,
        Err(err) => return (None, Some(err)),
    };

    let resource = build_resource(metadata);

    let provider = sdktrace::SdkTracerProvider::builder()
        .with_resource(resource)
        .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
        .build();
    let tracer = provider.tracer(metadata.service_name.clone());
    let _ = OTEL_TRACE_PROVIDER.set(provider);

    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    (Some(layer), None)
}

fn init_metrics_provider(metadata: &ResourceMetadata) -> Option<String> {
    if !otel_metrics_enabled() {
        return None;
    }

    let exporter = match build_otlp_metric_exporter() {
        Ok(exporter) => exporter,
        Err(err) => return Some(err),
    };

    let meter_provider = sdkmetrics::SdkMeterProvider::builder()
        .with_resource(build_resource(metadata))
        .with_periodic_exporter(exporter)
        .build();
    global::set_meter_provider(meter_provider.clone());

    let meter = global::meter("rrq.orchestrator");
    let _ = RRQ_METRICS.set(RrqMetrics::new(&meter));
    let _ = OTEL_METRIC_PROVIDER.set(meter_provider);

    None
}

fn otel_trace_enabled() -> bool {
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        && !endpoint.is_empty()
    {
        return true;
    }
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !endpoint.is_empty()
    {
        return true;
    }
    false
}

fn otel_metrics_enabled() -> bool {
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        && !endpoint.is_empty()
    {
        return true;
    }
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !endpoint.is_empty()
    {
        return true;
    }
    false
}

fn build_otlp_span_exporter() -> Result<SpanExporter, String> {
    let endpoint = resolve_otlp_trace_endpoint();
    let headers = resolve_otlp_headers("OTEL_EXPORTER_OTLP_TRACES_HEADERS");

    let mut exporter_builder = SpanExporter::builder().with_http().with_endpoint(endpoint);
    if !headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(headers);
    }
    exporter_builder.build().map_err(|err| err.to_string())
}

fn build_otlp_metric_exporter() -> Result<MetricExporter, String> {
    let endpoint = resolve_otlp_metrics_endpoint();
    let headers = resolve_otlp_headers("OTEL_EXPORTER_OTLP_METRICS_HEADERS");

    let mut exporter_builder = MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoint);
    if !headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(headers);
    }
    exporter_builder.build().map_err(|err| err.to_string())
}

fn resolve_otlp_headers(signal_specific_header_env: &str) -> HashMap<String, String> {
    let mut headers = parse_otlp_headers(env::var("OTEL_EXPORTER_OTLP_HEADERS").ok());
    for (key, value) in parse_otlp_headers(env::var(signal_specific_header_env).ok()) {
        headers.insert(key, value);
    }
    headers
}

fn resolve_otlp_trace_endpoint() -> String {
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        && !endpoint.is_empty()
    {
        return endpoint;
    }

    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !endpoint.is_empty()
    {
        return ensure_signal_path(endpoint, "traces");
    }

    "http://127.0.0.1:4318/v1/traces".to_string()
}

fn resolve_otlp_metrics_endpoint() -> String {
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        && !endpoint.is_empty()
    {
        return endpoint;
    }

    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !endpoint.is_empty()
    {
        return ensure_signal_path(endpoint, "metrics");
    }

    "http://127.0.0.1:4318/v1/metrics".to_string()
}

fn ensure_signal_path(endpoint: String, signal: &str) -> String {
    let expected_suffix = format!("/v1/{signal}");
    let (endpoint_path, endpoint_suffix) = split_endpoint_suffix(&endpoint);
    let endpoint_path = endpoint_path.trim_end_matches('/');

    if endpoint_path.ends_with(&expected_suffix) {
        return format!("{endpoint_path}{endpoint_suffix}");
    }

    let base = strip_otlp_signal_suffix(endpoint_path);
    format!("{base}{expected_suffix}{endpoint_suffix}")
}

fn split_endpoint_suffix(endpoint: &str) -> (&str, &str) {
    let split_index = endpoint
        .find('?')
        .into_iter()
        .chain(endpoint.find('#'))
        .min()
        .unwrap_or(endpoint.len());
    endpoint.split_at(split_index)
}

fn strip_otlp_signal_suffix(endpoint: &str) -> &str {
    for suffix in ["/v1/traces", "/v1/metrics", "/v1/logs"] {
        if let Some(base) = endpoint.strip_suffix(suffix) {
            return base;
        }
    }
    endpoint
}

fn env_optional_nonempty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn otel_resource_attribute(key: &str) -> Option<String> {
    let raw = env::var("OTEL_RESOURCE_ATTRIBUTES").ok()?;
    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (attr_key, value) = pair.split_once('=')?;
        if attr_key.trim() != key {
            continue;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if value.is_empty() {
            return None;
        }
        return Some(value.to_string());
    }
    None
}

fn parse_otlp_headers(raw: Option<String>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    let Some(raw) = raw else {
        return headers;
    };

    for pair in raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((key, value)) = pair.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            if !key.is_empty() && !value.is_empty() {
                headers.insert(key.to_string(), value.to_string());
            }
        }
    }
    headers
}

fn add_counter(counter: &Counter<u64>, value: u64, attrs: &[(&str, Value)]) {
    let attrs: Vec<KeyValue> = attrs
        .iter()
        .map(|(key, value)| KeyValue::new((*key).to_string(), value.clone()))
        .collect();
    counter.add(value, &attrs);
}

fn record_histogram(histogram: &Histogram<f64>, value: f64, attrs: &[(&str, Value)]) {
    if !value.is_finite() || value < 0.0 {
        return;
    }
    let attrs: Vec<KeyValue> = attrs
        .iter()
        .map(|(key, value)| KeyValue::new((*key).to_string(), value.clone()))
        .collect();
    histogram.record(value, &attrs);
}

pub fn record_poll_cycle(result: &str) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    add_counter(
        &metrics.poll_cycles_total,
        1,
        &[("result", Value::from(result.to_string()))],
    );
}

pub fn record_jobs_fetched(queue: &str, fetched: u64) {
    if fetched == 0 {
        return;
    }
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    add_counter(
        &metrics.jobs_fetched_total,
        fetched,
        &[("queue", Value::from(queue.to_string()))],
    );
}

pub fn record_lock_acquire(queue: &str, result: &str) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    add_counter(
        &metrics.lock_acquire_total,
        1,
        &[
            ("queue", Value::from(queue.to_string())),
            ("result", Value::from(result.to_string())),
        ],
    );
}

pub fn record_queue_wait_ms(queue: &str, queue_wait_ms: f64) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    record_histogram(
        &metrics.queue_wait_ms,
        queue_wait_ms,
        &[("queue", Value::from(queue.to_string()))],
    );
}

pub fn record_job_processed(queue: &str, runner: &str, outcome: &str, duration_ms: f64) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    let attrs = [
        ("queue", Value::from(queue.to_string())),
        ("runner", Value::from(runner.to_string())),
        ("outcome", Value::from(outcome.to_string())),
    ];
    add_counter(&metrics.jobs_processed_total, 1, &attrs);
    record_histogram(&metrics.job_duration_ms, duration_ms, &attrs);
}

pub fn record_orphan_recovered(count: u64) {
    if count == 0 {
        return;
    }
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    metrics.orphan_recovered_total.add(count, &[]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_log_format_handles_pretty_values() {
        assert_eq!(parse_log_format("pretty"), LogFormat::Pretty);
        assert_eq!(parse_log_format("text"), LogFormat::Pretty);
        assert_eq!(parse_log_format("human"), LogFormat::Pretty);
        assert_eq!(parse_log_format("PRETTY"), LogFormat::Pretty);
    }

    #[test]
    fn parse_log_format_defaults_to_json() {
        assert_eq!(parse_log_format("json"), LogFormat::Json);
        assert_eq!(parse_log_format(""), LogFormat::Json);
        assert_eq!(parse_log_format("nope"), LogFormat::Json);
    }

    #[test]
    fn parse_otlp_headers_parses_pairs() {
        let headers = parse_otlp_headers(Some("a=b, c = d, e=f".to_string()));
        assert_eq!(headers.get("a").map(String::as_str), Some("b"));
        assert_eq!(headers.get("c").map(String::as_str), Some("d"));
        assert_eq!(headers.get("e").map(String::as_str), Some("f"));
    }

    #[test]
    fn parse_otlp_headers_skips_invalid_pairs() {
        let headers = parse_otlp_headers(Some("=,onlykey, k= , =v".to_string()));
        assert!(headers.is_empty());
    }

    #[test]
    fn ensure_signal_path_appends_v1_signal() {
        assert_eq!(
            ensure_signal_path("http://collector:4318".to_string(), "metrics"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            ensure_signal_path("http://collector:4318/v1/traces".to_string(), "traces"),
            "http://collector:4318/v1/traces"
        );
    }

    #[test]
    fn ensure_signal_path_replaces_existing_signal_suffix() {
        assert_eq!(
            ensure_signal_path("http://collector:4318/v1/traces".to_string(), "metrics"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            ensure_signal_path(
                "http://collector:4318/v1/metrics?token=abc".to_string(),
                "traces"
            ),
            "http://collector:4318/v1/traces?token=abc"
        );
    }
}
