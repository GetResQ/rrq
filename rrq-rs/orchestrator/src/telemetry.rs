use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::propagation::{Extractor, TextMapCompositePropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
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
    let fmt_layer = match log_format() {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_current_span(true),
        LogFormat::Pretty => tracing_subscriber::fmt::layer(),
    }
    .with_filter(filter);

    let (otel_layer, otel_error) = init_otel_layer();
    let registry = tracing_subscriber::registry().with(fmt_layer);

    if let Some(otel_layer) = otel_layer {
        registry.with(otel_layer).init();
    } else {
        registry.init();
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
    span.set_parent(parent);
}

fn init_otel_layer(
) -> (
    Option<tracing_opentelemetry::OpenTelemetryLayer<tracing_subscriber::Registry, sdktrace::Tracer>>,
    Option<String>,
) {
    if !otel_enabled() {
        return (None, None);
    }

    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    let service_name = env::var("DD_SERVICE")
        .or_else(|_| env::var("RRQ_SERVICE_NAME"))
        .unwrap_or_else(|_| "rrq".to_string());
    let environment = env::var("DD_ENV").ok();
    let version = env::var("DD_VERSION").ok();

    let exporter = match build_otlp_exporter() {
        Ok(exporter) => exporter,
        Err(err) => return (None, Some(err)),
    };

    let mut resource_builder = Resource::builder().with_service_name(service_name.clone());
    if let Some(environment) = environment {
        resource_builder = resource_builder.with_attribute(KeyValue::new(
            "deployment.environment",
            environment,
        ));
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
    let enabled = env::var("DD_TRACE_ENABLED")
        .ok()
        .map(|value| parse_bool(&value))
        .unwrap_or(false);
    if enabled {
        return true;
    }
    env::var("DD_OTLP_ENDPOINT").is_ok() || env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok()
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn build_otlp_exporter() -> Result<SpanExporter, String> {
    let endpoint = env::var("DD_OTLP_ENDPOINT")
        .or_else(|_| env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .unwrap_or_else(|_| "http://127.0.0.1:4318/v1/traces".to_string());

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
    fn parse_bool_understands_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on", " On "] {
            assert!(parse_bool(value), "expected true for {value}");
        }
        for value in ["0", "false", "no", "off", ""] {
            assert!(!parse_bool(value), "expected false for {value}");
        }
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
