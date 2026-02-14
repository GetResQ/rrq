use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpSignal {
    Traces,
    Metrics,
    Logs,
}

impl OtlpSignal {
    #[must_use]
    pub const fn endpoint_env(self) -> &'static str {
        match self {
            Self::Traces => "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            Self::Metrics => "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            Self::Logs => "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        }
    }

    #[must_use]
    pub const fn headers_env(self) -> &'static str {
        match self {
            Self::Traces => "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            Self::Metrics => "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
            Self::Logs => "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
        }
    }

    #[must_use]
    pub const fn path_suffix(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpGlobalEndpointStyle {
    Raw,
    AppendHttpSignalPath,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OtlpSignalConfig {
    pub endpoint: Option<String>,
    pub headers: HashMap<String, String>,
    pub has_explicit_endpoint_env: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OtlpEnvConfig {
    pub global_endpoint: Option<String>,
    pub traces: OtlpSignalConfig,
    pub metrics: OtlpSignalConfig,
    pub logs: OtlpSignalConfig,
}

impl OtlpEnvConfig {
    #[must_use]
    pub fn signal(&self, signal: OtlpSignal) -> &OtlpSignalConfig {
        match signal {
            OtlpSignal::Traces => &self.traces,
            OtlpSignal::Metrics => &self.metrics,
            OtlpSignal::Logs => &self.logs,
        }
    }

    #[must_use]
    pub fn explicit_signal_endpoint_count(&self) -> usize {
        [self.traces(), self.metrics(), self.logs()]
            .iter()
            .filter(|signal| signal.has_explicit_endpoint_env)
            .count()
    }

    #[must_use]
    pub const fn has_global_endpoint(&self) -> bool {
        self.global_endpoint.is_some()
    }

    #[must_use]
    pub const fn traces(&self) -> &OtlpSignalConfig {
        &self.traces
    }

    #[must_use]
    pub const fn metrics(&self) -> &OtlpSignalConfig {
        &self.metrics
    }

    #[must_use]
    pub const fn logs(&self) -> &OtlpSignalConfig {
        &self.logs
    }
}

#[must_use]
pub fn resolve_otlp_env(style: OtlpGlobalEndpointStyle) -> OtlpEnvConfig {
    resolve_otlp_env_with_lookup(style, |key| std::env::var(key).ok())
}

fn resolve_otlp_env_with_lookup<F>(style: OtlpGlobalEndpointStyle, lookup: F) -> OtlpEnvConfig
where
    F: Fn(&str) -> Option<String>,
{
    let global_endpoint = nonempty(lookup("OTEL_EXPORTER_OTLP_ENDPOINT").as_deref());
    let global_headers = parse_otlp_headers(lookup("OTEL_EXPORTER_OTLP_HEADERS"));

    OtlpEnvConfig {
        global_endpoint: global_endpoint.clone(),
        traces: resolve_signal_config(
            &lookup,
            style,
            global_endpoint.as_deref(),
            &global_headers,
            OtlpSignal::Traces,
        ),
        metrics: resolve_signal_config(
            &lookup,
            style,
            global_endpoint.as_deref(),
            &global_headers,
            OtlpSignal::Metrics,
        ),
        logs: resolve_signal_config(
            &lookup,
            style,
            global_endpoint.as_deref(),
            &global_headers,
            OtlpSignal::Logs,
        ),
    }
}

fn resolve_signal_config<F>(
    lookup: &F,
    style: OtlpGlobalEndpointStyle,
    global_endpoint: Option<&str>,
    global_headers: &HashMap<String, String>,
    signal: OtlpSignal,
) -> OtlpSignalConfig
where
    F: Fn(&str) -> Option<String>,
{
    let explicit_endpoint = lookup(signal.endpoint_env());
    let endpoint = match explicit_endpoint.as_deref() {
        // Explicit signal-specific setting (including empty) takes precedence.
        Some(value) => nonempty(Some(value)),
        None => {
            global_endpoint.map(|endpoint| apply_global_endpoint_style(endpoint, style, signal))
        }
    };

    let mut headers = global_headers.clone();
    for (key, value) in parse_otlp_headers(lookup(signal.headers_env())) {
        headers.insert(key, value);
    }

    OtlpSignalConfig {
        endpoint,
        headers,
        has_explicit_endpoint_env: explicit_endpoint.is_some(),
    }
}

fn apply_global_endpoint_style(
    endpoint: &str,
    style: OtlpGlobalEndpointStyle,
    signal: OtlpSignal,
) -> String {
    match style {
        OtlpGlobalEndpointStyle::Raw => endpoint.to_string(),
        OtlpGlobalEndpointStyle::AppendHttpSignalPath => {
            ensure_signal_path(endpoint.to_string(), signal.path_suffix())
        }
    }
}

fn nonempty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(style: OtlpGlobalEndpointStyle, values: &[(&str, &str)]) -> OtlpEnvConfig {
        let env: HashMap<String, String> = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        resolve_otlp_env_with_lookup(style, |key| env.get(key).cloned())
    }

    #[test]
    fn append_http_signal_path_adds_v1_path_to_global_endpoint() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::AppendHttpSignalPath,
            &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")],
        );

        assert_eq!(
            cfg.traces.endpoint.as_deref(),
            Some("http://collector:4318/v1/traces")
        );
        assert_eq!(
            cfg.metrics.endpoint.as_deref(),
            Some("http://collector:4318/v1/metrics")
        );
        assert_eq!(
            cfg.logs.endpoint.as_deref(),
            Some("http://collector:4318/v1/logs")
        );
    }

    #[test]
    fn append_http_signal_path_replaces_existing_signal_suffix() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::AppendHttpSignalPath,
            &[(
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "http://collector:4318/v1/traces?token=abc",
            )],
        );
        assert_eq!(
            cfg.metrics.endpoint.as_deref(),
            Some("http://collector:4318/v1/metrics?token=abc")
        );
    }

    #[test]
    fn raw_style_keeps_global_endpoint_unmodified() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::Raw,
            &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")],
        );
        assert_eq!(
            cfg.traces.endpoint.as_deref(),
            Some("http://collector:4317")
        );
        assert_eq!(
            cfg.metrics.endpoint.as_deref(),
            Some("http://collector:4317")
        );
        assert_eq!(cfg.logs.endpoint.as_deref(), Some("http://collector:4317"));
    }

    #[test]
    fn explicit_signal_endpoint_takes_precedence_over_global() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::AppendHttpSignalPath,
            &[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://global:4318"),
                (
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                    "http://local:4318/custom",
                ),
            ],
        );
        assert_eq!(
            cfg.traces.endpoint.as_deref(),
            Some("http://local:4318/custom")
        );
        assert_eq!(
            cfg.metrics.endpoint.as_deref(),
            Some("http://global:4318/v1/metrics")
        );
    }

    #[test]
    fn explicit_empty_signal_endpoint_disables_signal() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::AppendHttpSignalPath,
            &[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
                ("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", " "),
            ],
        );
        assert_eq!(cfg.logs.endpoint, None);
        assert!(cfg.logs.has_explicit_endpoint_env);
    }

    #[test]
    fn merges_global_and_signal_headers_with_signal_override() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::AppendHttpSignalPath,
            &[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
                ("OTEL_EXPORTER_OTLP_HEADERS", "a=1, b=2"),
                ("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "b=3, c=4"),
            ],
        );
        assert_eq!(cfg.traces.headers.get("a").map(String::as_str), Some("1"));
        assert_eq!(cfg.traces.headers.get("b").map(String::as_str), Some("3"));
        assert_eq!(cfg.traces.headers.get("c").map(String::as_str), Some("4"));
        assert_eq!(cfg.metrics.headers.get("b").map(String::as_str), Some("2"));
    }

    #[test]
    fn counts_explicit_signal_endpoints_even_when_empty() {
        let cfg = resolve(
            OtlpGlobalEndpointStyle::AppendHttpSignalPath,
            &[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318"),
                ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://trace:4318"),
                ("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", ""),
            ],
        );
        assert_eq!(cfg.explicit_signal_endpoint_count(), 2);
    }
}
