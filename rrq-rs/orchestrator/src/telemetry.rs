use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::propagation::{Extractor, Injector, TextMapCompositePropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, Value};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::logs as sdklogs;
use opentelemetry_sdk::metrics as sdkmetrics;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader as AsyncPeriodicReader;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
use serde_json::{Map, Value as JsonValue};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Pretty,
}

static LOG_FORMAT: OnceLock<LogFormat> = OnceLock::new();
static OTEL_TRACE_PROVIDER: OnceLock<sdktrace::SdkTracerProvider> = OnceLock::new();
static OTEL_METRIC_PROVIDER: OnceLock<sdkmetrics::SdkMeterProvider> = OnceLock::new();
static OTEL_LOG_PROVIDER: OnceLock<sdklogs::SdkLoggerProvider> = OnceLock::new();
static RRQ_METRICS: OnceLock<RrqMetrics> = OnceLock::new();

#[derive(Clone)]
struct ResourceMetadata {
    service_name: String,
    environment: Option<String>,
    version: Option<String>,
}

struct RrqMetrics {
    enqueue_total: Counter<u64>,
    enqueue_duration_ms: Histogram<f64>,
    claim_attempt_total: Counter<u64>,
    claim_miss_total: Counter<u64>,
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
            enqueue_total: meter.u64_counter("rrq_enqueue_total").build(),
            enqueue_duration_ms: meter.f64_histogram("rrq_enqueue_duration_ms").build(),
            claim_attempt_total: meter.u64_counter("rrq_claim_attempt_total").build(),
            claim_miss_total: meter.u64_counter("rrq_claim_miss_total").build(),
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
    let metadata = resolve_resource_metadata();
    let (otel_layer, trace_error) = init_otel_layer(&metadata);
    let metrics_error = init_metrics_provider(&metadata);
    let (otel_log_layer, logs_error) = init_otel_log_layer(&metadata);
    match log_format() {
        LogFormat::Json => {
            match (otel_layer, otel_log_layer) {
                (Some(otel_layer), Some(otel_log_layer)) => tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(otel_log_layer)
                    .with(default_env_filter())
                    .with(
                        tracing_subscriber::fmt::layer()
                            .json()
                            .with_ansi(false)
                            .with_current_span(true),
                    )
                    .init(),
                (Some(otel_layer), None) => tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(default_env_filter())
                    .with(
                        tracing_subscriber::fmt::layer()
                            .json()
                            .with_ansi(false)
                            .with_current_span(true),
                    )
                    .init(),
                (None, Some(otel_log_layer)) => tracing_subscriber::registry()
                    .with(otel_log_layer)
                    .with(default_env_filter())
                    .with(
                        tracing_subscriber::fmt::layer()
                            .json()
                            .with_ansi(false)
                            .with_current_span(true),
                    )
                    .init(),
                (None, None) => tracing_subscriber::registry()
                    .with(default_env_filter())
                    .with(
                        tracing_subscriber::fmt::layer()
                            .json()
                            .with_ansi(false)
                            .with_current_span(true),
                    )
                    .init(),
            };
        }
        LogFormat::Pretty => {
            match (otel_layer, otel_log_layer) {
                (Some(otel_layer), Some(otel_log_layer)) => tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(otel_log_layer)
                    .with(default_env_filter())
                    .with(tracing_subscriber::fmt::layer())
                    .init(),
                (Some(otel_layer), None) => tracing_subscriber::registry()
                    .with(otel_layer)
                    .with(default_env_filter())
                    .with(tracing_subscriber::fmt::layer())
                    .init(),
                (None, Some(otel_log_layer)) => tracing_subscriber::registry()
                    .with(otel_log_layer)
                    .with(default_env_filter())
                    .with(tracing_subscriber::fmt::layer())
                    .init(),
                (None, None) => tracing_subscriber::registry()
                    .with(default_env_filter())
                    .with(tracing_subscriber::fmt::layer())
                    .init(),
            };
        }
    }
    warn_if_global_otlp_endpoint_only();

    if let Some(error) = trace_error {
        tracing::warn!(error = %error, "OpenTelemetry tracing exporter failed to initialize");
    }
    if let Some(error) = metrics_error {
        tracing::warn!(error = %error, "OpenTelemetry metrics exporter failed to initialize");
    }
    if let Some(error) = logs_error {
        tracing::warn!(error = %error, "OpenTelemetry logs exporter failed to initialize");
    }
}

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

fn warn_if_global_otlp_endpoint_only() {
    let has_global = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .is_some_and(|value| !value.is_empty());
    if !has_global {
        return;
    }
    let missing_traces = !otel_trace_enabled();
    let missing_metrics = !otel_metrics_enabled();
    let missing_logs = !otel_logs_enabled();
    if missing_traces || missing_metrics || missing_logs {
        tracing::warn!(
            missing_traces,
            missing_metrics,
            missing_logs,
            "OTEL_EXPORTER_OTLP_ENDPOINT is set but RRQ requires per-signal OTLP endpoint variables: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
        );
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

pub fn inject_current_trace_context(
    trace_context: Option<HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    let mut merged = trace_context.unwrap_or_default();
    let current = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&current, &mut HashMapInjector(&mut merged));
    });
    if merged.is_empty() {
        return None;
    }
    Some(merged)
}

pub fn extract_correlation_context(
    params: &Map<String, JsonValue>,
    mappings: &HashMap<String, String>,
    trace_context: Option<&HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    if mappings.is_empty() {
        return None;
    }

    const MAX_CORRELATION_KEYS: usize = 16;
    const MAX_CORRELATION_KEY_LEN: usize = 64;
    const MAX_CORRELATION_VALUE_LEN: usize = 256;
    let mut correlation = HashMap::new();

    for (attribute_name, path) in mappings {
        if correlation.len() >= MAX_CORRELATION_KEYS {
            break;
        }
        let key = attribute_name.trim();
        if key.is_empty() || key.len() > MAX_CORRELATION_KEY_LEN {
            continue;
        }
        if let Some(existing) = trace_context.and_then(|ctx| ctx.get(key))
            && !existing.is_empty()
        {
            correlation.insert(
                key.to_string(),
                truncate_utf8(existing, MAX_CORRELATION_VALUE_LEN),
            );
            continue;
        }
        let Some(raw) = lookup_value_in_params(params, path) else {
            continue;
        };
        let Some(value) = scalar_value_to_string(raw) else {
            continue;
        };
        correlation.insert(
            key.to_string(),
            truncate_utf8(&value, MAX_CORRELATION_VALUE_LEN),
        );
    }

    if correlation.is_empty() {
        return None;
    }
    Some(correlation)
}

pub fn apply_correlation_context_to_span(
    span: &tracing::Span,
    correlation_context: &HashMap<String, String>,
) {
    if correlation_context.is_empty() {
        return;
    }
    let span_context = span.context();
    let otel_span = span_context.span();
    for (key, value) in correlation_context {
        if key.is_empty() || value.is_empty() {
            continue;
        }
        otel_span.set_attribute(KeyValue::new(key.clone(), value.clone()));
    }
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

    let reader = AsyncPeriodicReader::builder(exporter, runtime::Tokio).build();
    let meter_provider = sdkmetrics::SdkMeterProvider::builder()
        .with_resource(build_resource(metadata))
        .with_reader(reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    let meter = global::meter("rrq.orchestrator");
    let _ = RRQ_METRICS.set(RrqMetrics::new(&meter));
    let _ = OTEL_METRIC_PROVIDER.set(meter_provider);

    None
}

fn init_otel_log_layer(
    metadata: &ResourceMetadata,
) -> (
    Option<OpenTelemetryTracingBridge<sdklogs::SdkLoggerProvider, sdklogs::SdkLogger>>,
    Option<String>,
) {
    if !otel_logs_enabled() {
        return (None, None);
    }

    let exporter = match build_otlp_log_exporter() {
        Ok(exporter) => exporter,
        Err(err) => return (None, Some(err)),
    };

    let provider = sdklogs::SdkLoggerProvider::builder()
        .with_resource(build_resource(metadata))
        .with_batch_exporter(exporter)
        .build();
    let _ = OTEL_LOG_PROVIDER.set(provider);
    let Some(provider_ref) = OTEL_LOG_PROVIDER.get() else {
        return (
            None,
            Some("failed to initialize OpenTelemetry logger provider".to_string()),
        );
    };
    let layer = OpenTelemetryTracingBridge::new(provider_ref);
    (Some(layer), None)
}

fn otel_trace_enabled() -> bool {
    env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .ok()
        .is_some_and(|endpoint| !endpoint.is_empty())
}

fn otel_metrics_enabled() -> bool {
    env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .ok()
        .is_some_and(|endpoint| !endpoint.is_empty())
}

fn otel_logs_enabled() -> bool {
    env::var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
        .ok()
        .is_some_and(|endpoint| !endpoint.is_empty())
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

fn build_otlp_log_exporter() -> Result<LogExporter, String> {
    let endpoint = resolve_otlp_logs_endpoint();
    let headers = resolve_otlp_headers("OTEL_EXPORTER_OTLP_LOGS_HEADERS");

    let mut exporter_builder = LogExporter::builder().with_http().with_endpoint(endpoint);
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
    let explicit_endpoint = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").ok();
    select_signal_endpoint(
        explicit_endpoint.as_deref(),
        "http://127.0.0.1:4318",
        "traces",
    )
}

fn resolve_otlp_metrics_endpoint() -> String {
    let explicit_endpoint = env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").ok();
    select_signal_endpoint(
        explicit_endpoint.as_deref(),
        "http://127.0.0.1:4318",
        "metrics",
    )
}

fn resolve_otlp_logs_endpoint() -> String {
    let explicit_endpoint = env::var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").ok();
    select_signal_endpoint(
        explicit_endpoint.as_deref(),
        "http://127.0.0.1:4318",
        "logs",
    )
}

fn select_signal_endpoint(
    explicit_endpoint: Option<&str>,
    default_base_endpoint: &str,
    signal: &str,
) -> String {
    if let Some(endpoint) = explicit_endpoint.filter(|value| !value.is_empty()) {
        return endpoint.to_string();
    }
    ensure_signal_path(default_base_endpoint.to_string(), signal)
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

pub fn record_enqueue(queue: &str, result: &str, duration_ms: f64) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    let attrs = [
        ("queue", Value::from(queue.to_string())),
        ("result", Value::from(result.to_string())),
    ];
    add_counter(&metrics.enqueue_total, 1, &attrs);
    record_histogram(&metrics.enqueue_duration_ms, duration_ms, &attrs);
}

pub fn record_claim_attempt(queue: &str) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    add_counter(
        &metrics.claim_attempt_total,
        1,
        &[("queue", Value::from(queue.to_string()))],
    );
}

pub fn record_claim_miss(queue: &str, reason: &str) {
    let Some(metrics) = RRQ_METRICS.get() else {
        return;
    };
    add_counter(
        &metrics.claim_miss_total,
        1,
        &[
            ("queue", Value::from(queue.to_string())),
            ("reason", Value::from(reason.to_string())),
        ],
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

struct HashMapInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> Injector for HashMapInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

fn lookup_value_in_params<'a>(
    params: &'a Map<String, JsonValue>,
    path: &str,
) -> Option<&'a JsonValue> {
    let cleaned = path.trim().trim_start_matches("params.");
    if cleaned.is_empty() {
        return None;
    }
    let mut parts = cleaned.split('.');
    let first = parts.next()?;
    let mut current = params.get(first)?;
    for part in parts {
        if part.is_empty() {
            return None;
        }
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn scalar_value_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(v) if !v.is_empty() => Some(v.clone()),
        JsonValue::Bool(v) => Some(v.to_string()),
        JsonValue::Number(v) => Some(v.to_string()),
        _ => None,
    }
}

fn truncate_utf8(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_string();
    }
    let mut out = String::with_capacity(max_len);
    for ch in value.chars() {
        if out.len() + ch.len_utf8() > max_len {
            break;
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::trace as sdktrace;
    use serde_json::{Map, Value as JsonValue, json};
    use tracing_subscriber::layer::SubscriberExt;

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
    fn inject_current_trace_context_preserves_existing_entries() {
        let mut existing = HashMap::new();
        existing.insert("session_id".to_string(), "sess-1".to_string());

        let merged =
            inject_current_trace_context(Some(existing)).expect("expected merged trace context");

        assert_eq!(merged.get("session_id").map(String::as_str), Some("sess-1"));
    }

    #[test]
    fn extract_correlation_context_maps_nested_and_scalar_values() {
        let params = json!({
            "session": { "id": "sess-42" },
            "message_id": 123,
            "retry": true
        })
        .as_object()
        .expect("object params")
        .clone();

        let mappings = HashMap::from([
            ("session_id".to_string(), "session.id".to_string()),
            ("message_id".to_string(), "params.message_id".to_string()),
            ("retry_flag".to_string(), "retry".to_string()),
        ]);

        let extracted =
            extract_correlation_context(&params, &mappings, None).expect("expected correlation");

        assert_eq!(
            extracted.get("session_id").map(String::as_str),
            Some("sess-42")
        );
        assert_eq!(extracted.get("message_id").map(String::as_str), Some("123"));
        assert_eq!(
            extracted.get("retry_flag").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn extract_correlation_context_prefers_trace_context_value() {
        let params = json!({
            "session": { "id": "sess-from-params" }
        })
        .as_object()
        .expect("object params")
        .clone();
        let mappings = HashMap::from([("session_id".to_string(), "session.id".to_string())]);
        let trace_context =
            HashMap::from([("session_id".to_string(), "sess-from-trace".to_string())]);

        let extracted = extract_correlation_context(&params, &mappings, Some(&trace_context))
            .expect("expected correlation");

        assert_eq!(
            extracted.get("session_id").map(String::as_str),
            Some("sess-from-trace")
        );
    }

    #[test]
    fn extract_correlation_context_skips_non_scalar_values() {
        let params = json!({
            "object": { "nested": "value" },
            "list": [1, 2, 3]
        })
        .as_object()
        .expect("object params")
        .clone();
        let mappings = HashMap::from([
            ("obj".to_string(), "object".to_string()),
            ("list".to_string(), "list".to_string()),
        ]);

        assert_eq!(extract_correlation_context(&params, &mappings, None), None);
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

    #[test]
    fn select_signal_endpoint_preserves_explicit_path() {
        assert_eq!(
            select_signal_endpoint(
                Some("http://collector:4318/custom/traces"),
                "http://127.0.0.1:4318",
                "traces"
            ),
            "http://collector:4318/custom/traces"
        );
    }

    #[test]
    fn select_signal_endpoint_uses_default_when_missing_or_blank() {
        assert_eq!(
            select_signal_endpoint(None, "http://collector:4318", "metrics"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            select_signal_endpoint(Some(""), "http://collector:4318", "logs"),
            "http://collector:4318/v1/logs"
        );
    }

    #[test]
    fn set_parent_from_trace_context_preserves_upstream_trace_id_when_injecting() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let provided_traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let provided =
            HashMap::from([("traceparent".to_string(), provided_traceparent.to_string())]);

        let tracer_provider = sdktrace::SdkTracerProvider::builder().build();
        let tracer = tracer_provider.tracer("telemetry-tests");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        let merged = tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("rrq.enqueue");
            set_parent_from_trace_context(&span, &provided);
            let _enter = span.enter();
            inject_current_trace_context(Some(provided.clone())).expect("trace context")
        });

        let merged_traceparent = merged
            .get("traceparent")
            .expect("traceparent should be present");
        assert!(
            merged_traceparent.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
            "expected injected traceparent to keep upstream trace id, got {merged_traceparent}"
        );
    }

    #[test]
    fn extract_correlation_context_enforces_key_and_value_limits() {
        let mut params = Map::new();
        let mut mappings = HashMap::new();
        for index in 0..20 {
            let field = format!("field_{index}");
            params.insert(field.clone(), JsonValue::String("x".repeat(400)));
            mappings.insert(format!("attr_{index}"), field);
        }
        mappings.insert(String::new(), "field_0".to_string());
        mappings.insert("k".repeat(65), "field_0".to_string());

        let extracted =
            extract_correlation_context(&params, &mappings, None).expect("correlation context");

        assert_eq!(extracted.len(), 16);
        assert!(
            extracted
                .keys()
                .all(|key| !key.is_empty() && key.len() <= 64)
        );
        assert!(extracted.values().all(|value| value.len() <= 256));
    }

    #[test]
    fn extract_correlation_context_truncates_trace_context_values() {
        let params = json!({
            "session": { "id": "sess-from-params" }
        })
        .as_object()
        .expect("object params")
        .clone();
        let mappings = HashMap::from([("session_id".to_string(), "session.id".to_string())]);
        let trace_context = HashMap::from([("session_id".to_string(), "z".repeat(300))]);

        let extracted = extract_correlation_context(&params, &mappings, Some(&trace_context))
            .expect("expected correlation");

        assert_eq!(
            extracted.get("session_id").map(|value| value.len()),
            Some(256)
        );
    }
}
