use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, TextMapCompositePropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig};
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
static OTEL_PROVIDER: OnceLock<sdktrace::SdkTracerProvider> = OnceLock::new();

pub fn log_format() -> LogFormat {
    *LOG_FORMAT.get_or_init(|| {
        let value = env::var("RRQ_LOG_FORMAT").unwrap_or_else(|_| "json".to_string());
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
    let (otel_layer, otel_error) = init_otel_layer();
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

    if let Some(error) = otel_error {
        tracing::warn!(error = %error, "OpenTelemetry tracing exporter failed to initialize");
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

fn init_otel_layer() -> (
    Option<
        tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::Registry, sdktrace::Tracer>,
    >,
    Option<String>,
) {
    if !otel_enabled() {
        return (None, None);
    }

    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    let service_name = env_optional_nonempty("OTEL_SERVICE_NAME")
        .or_else(|| otel_resource_attribute("service.name"))
        .or_else(|| env_optional_nonempty("RRQ_SERVICE_NAME"))
        .unwrap_or_else(|| "rrq".to_string());
    let environment = otel_resource_attribute("deployment.environment")
        .or_else(|| env_optional_nonempty("ENVIRONMENT"));
    let version = otel_resource_attribute("service.version")
        .or_else(|| env_optional_nonempty("SERVICE_VERSION"));

    let exporter = match build_otlp_exporter() {
        Ok(exporter) => exporter,
        Err(err) => return (None, Some(err)),
    };

    let mut resource_builder = Resource::builder().with_service_name(service_name.clone());
    if let Some(environment) = environment {
        resource_builder =
            resource_builder.with_attribute(KeyValue::new("deployment.environment", environment));
    }
    if let Some(version) = version {
        resource_builder =
            resource_builder.with_attribute(KeyValue::new("service.version", version));
    }
    let resource = resource_builder.build();

    let provider = sdktrace::SdkTracerProvider::builder()
        .with_resource(resource)
        .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
        .build();
    let tracer = provider.tracer(service_name);
    let _ = OTEL_PROVIDER.set(provider);

    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    (Some(layer), None)
}

fn otel_enabled() -> bool {
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

fn build_otlp_exporter() -> Result<SpanExporter, String> {
    let endpoint = resolve_otlp_endpoint();

    let mut headers = parse_otlp_headers(env::var("OTEL_EXPORTER_OTLP_HEADERS").ok());
    if let Ok(api_key) = env::var("DD_API_KEY") {
        headers.entry("DD-API-KEY".to_string()).or_insert(api_key);
    }

    let mut exporter_builder = SpanExporter::builder().with_http().with_endpoint(endpoint);
    if !headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(headers);
    }
    exporter_builder.build().map_err(|err| err.to_string())
}

fn resolve_otlp_endpoint() -> String {
    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        && !endpoint.is_empty()
    {
        return endpoint;
    }

    if let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        && !endpoint.is_empty()
    {
        return ensure_traces_path(endpoint);
    }

    "http://127.0.0.1:4318/v1/traces".to_string()
}

fn ensure_traces_path(endpoint: String) -> String {
    if endpoint.ends_with("/v1/traces") {
        return endpoint;
    }
    let base = endpoint.trim_end_matches('/');
    format!("{base}/v1/traces")
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
}
