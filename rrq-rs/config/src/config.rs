use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{Map, Value};

use crate::settings::RRQSettings;
use crate::tcp_socket::parse_tcp_socket;

pub const DEFAULT_CONFIG_FILENAME: &str = "rrq.toml";
pub const ENV_CONFIG_KEY: &str = "RRQ_CONFIG";

pub fn resolve_config_source(config_path: Option<&str>) -> (Option<String>, String) {
    if let Some(path) = config_path {
        return (Some(path.to_string()), "--config parameter".to_string());
    }

    if let Ok(env_path) = std::env::var(ENV_CONFIG_KEY)
        && !env_path.is_empty()
    {
        return (Some(env_path), format!("{ENV_CONFIG_KEY} env var"));
    }

    let default_path = Path::new(DEFAULT_CONFIG_FILENAME);
    if default_path.is_file() {
        return (
            Some(default_path.to_string_lossy().to_string()),
            format!("{DEFAULT_CONFIG_FILENAME} in cwd"),
        );
    }

    (None, "not found".to_string())
}

pub fn load_toml_settings(config_path: Option<&str>) -> Result<RRQSettings> {
    dotenvy::dotenv().ok();

    let (path, _) = resolve_config_source(config_path);
    let path = path.ok_or_else(|| {
        anyhow::anyhow!("RRQ config not found. Provide --config, set RRQ_CONFIG, or add rrq.toml.")
    })?;

    let payload = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read config at {path}"))?;
    let toml_value: toml::Value =
        toml::from_str(&payload).with_context(|| format!("failed to parse TOML at {path}"))?;
    let mut json_value =
        serde_json::to_value(toml_value).context("failed to convert TOML to JSON")?;

    json_value = normalize_toml_payload(json_value)?;
    let env_overrides = env_overrides()?;
    let merged = deep_merge(json_value, env_overrides);

    let settings: RRQSettings = serde_json::from_value(merged.clone()).map_err(|err| {
        let hint = diagnose_config_error(&merged, &err);
        anyhow::anyhow!("invalid RRQ config: {err}{hint}")
    })?;
    validate_runner_configs(&settings)?;
    Ok(settings)
}

fn normalize_toml_payload(mut payload: Value) -> Result<Value> {
    if let Value::Object(mut map) = payload {
        if let Some(rrq_value) = map.remove("rrq") {
            payload = rrq_value;
        } else {
            payload = Value::Object(map);
        }
    }

    if let Value::Object(mut map) = payload {
        if let Some(routing) = map.remove("routing") {
            map.insert("runner_routes".to_string(), routing);
        }
        map.remove("worker_concurrency");
        return Ok(Value::Object(map));
    }

    Err(anyhow::anyhow!("RRQ config must be a TOML table"))
}

fn env_overrides() -> Result<Value> {
    let mut payload = Map::new();

    set_env_string(&mut payload, "redis_dsn", "RRQ_REDIS_DSN");
    set_env_string(&mut payload, "default_queue_name", "RRQ_DEFAULT_QUEUE_NAME");
    set_env_string(&mut payload, "default_dlq_name", "RRQ_DEFAULT_DLQ_NAME");
    set_env_int(
        &mut payload,
        "default_max_retries",
        "RRQ_DEFAULT_MAX_RETRIES",
    )?;
    set_env_int(
        &mut payload,
        "default_job_timeout_seconds",
        "RRQ_DEFAULT_JOB_TIMEOUT_SECONDS",
    )?;
    set_env_int(
        &mut payload,
        "default_result_ttl_seconds",
        "RRQ_DEFAULT_RESULT_TTL_SECONDS",
    )?;
    set_env_float(
        &mut payload,
        "default_poll_delay_seconds",
        "RRQ_DEFAULT_POLL_DELAY_SECONDS",
    )?;
    set_env_int(
        &mut payload,
        "runner_connect_timeout_ms",
        "RRQ_RUNNER_CONNECT_TIMEOUT_MS",
    )?;
    set_env_int(
        &mut payload,
        "default_lock_timeout_extension_seconds",
        "RRQ_DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS",
    )?;
    set_env_int(
        &mut payload,
        "default_unique_job_lock_ttl_seconds",
        "RRQ_DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS",
    )?;
    set_env_string(
        &mut payload,
        "default_runner_name",
        "RRQ_DEFAULT_RUNNER_NAME",
    );
    set_env_float(
        &mut payload,
        "worker_health_check_interval_seconds",
        "RRQ_WORKER_HEALTH_CHECK_INTERVAL_SECONDS",
    )?;
    set_env_float(
        &mut payload,
        "worker_health_check_ttl_buffer_seconds",
        "RRQ_WORKER_HEALTH_CHECK_TTL_BUFFER_SECONDS",
    )?;
    set_env_float(
        &mut payload,
        "base_retry_delay_seconds",
        "RRQ_BASE_RETRY_DELAY_SECONDS",
    )?;
    set_env_float(
        &mut payload,
        "max_retry_delay_seconds",
        "RRQ_MAX_RETRY_DELAY_SECONDS",
    )?;
    set_env_float(
        &mut payload,
        "worker_shutdown_grace_period_seconds",
        "RRQ_WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS",
    )?;
    set_env_int(&mut payload, "expected_job_ttl", "RRQ_EXPECTED_JOB_TTL")?;

    Ok(Value::Object(payload))
}

fn set_env_string(map: &mut Map<String, Value>, key: &str, env: &str) {
    if let Ok(value) = std::env::var(env)
        && !value.is_empty()
    {
        map.insert(key.to_string(), Value::String(value));
    }
}

fn set_env_int(map: &mut Map<String, Value>, key: &str, env: &str) -> Result<()> {
    if let Ok(value) = std::env::var(env) {
        if value.is_empty() {
            return Ok(());
        }
        let parsed: i64 = value
            .parse()
            .with_context(|| format!("Invalid {env} value: {value}"))?;
        map.insert(key.to_string(), Value::Number(parsed.into()));
    }
    Ok(())
}

fn set_env_float(map: &mut Map<String, Value>, key: &str, env: &str) -> Result<()> {
    if let Ok(value) = std::env::var(env) {
        if value.is_empty() {
            return Ok(());
        }
        let parsed: f64 = value
            .parse()
            .with_context(|| format!("Invalid {env} value: {value}"))?;
        map.insert(
            key.to_string(),
            Value::Number(
                serde_json::Number::from_f64(parsed)
                    .ok_or_else(|| anyhow::anyhow!("Invalid {env} value: {value}"))?,
            ),
        );
    }
    Ok(())
}

fn deep_merge(base: Value, overlay: Value) -> Value {
    match (base, overlay) {
        (Value::Object(mut base_map), Value::Object(overlay_map)) => {
            for (key, value) in overlay_map {
                let entry = base_map.remove(&key);
                let merged = match entry {
                    Some(existing) => deep_merge(existing, value),
                    None => value,
                };
                base_map.insert(key, merged);
            }
            Value::Object(base_map)
        }
        (_, overlay_value) => overlay_value,
    }
}

fn diagnose_config_error(config: &Value, err: &serde_json::Error) -> String {
    let err_msg = err.to_string().to_lowercase();

    // Check for unknown field errors in runner configs
    if err_msg.contains("unknown field")
        && let Some(runners) = config.get("runners").and_then(|v| v.as_object())
    {
        let valid_fields = [
            "type",
            "cmd",
            "pool_size",
            "max_in_flight",
            "env",
            "cwd",
            "tcp_socket",
            "response_timeout_seconds",
        ];
        for (name, runner) in runners {
            if let Some(obj) = runner.as_object() {
                for key in obj.keys() {
                    if !valid_fields.contains(&key.as_str()) {
                        return format!(
                            "\n\nHint: runner '{name}' has unknown field '{key}'. Valid fields are: {}",
                            valid_fields.join(", ")
                        );
                    }
                }
            }
        }
    }

    // Check for type errors in runner configs
    if err_msg.contains("invalid type")
        && let Some(runners) = config.get("runners").and_then(|v| v.as_object())
    {
        for (name, runner) in runners {
            if let Some(obj) = runner.as_object() {
                if let Some(pool_size) = obj.get("pool_size")
                    && !pool_size.is_u64()
                {
                    return format!(
                        "\n\nHint: runner '{name}' has invalid pool_size - must be a positive integer, got: {pool_size}"
                    );
                }
                if let Some(max_in_flight) = obj.get("max_in_flight")
                    && !max_in_flight.is_u64()
                {
                    return format!(
                        "\n\nHint: runner '{name}' has invalid max_in_flight - must be a positive integer, got: {max_in_flight}"
                    );
                }
                if let Some(cmd) = obj.get("cmd")
                    && !cmd.is_array()
                {
                    return format!(
                        "\n\nHint: runner '{name}' has invalid cmd - must be an array of strings, got: {cmd}"
                    );
                }
            }
        }
    }

    String::new()
}

fn validate_runner_configs(settings: &RRQSettings) -> Result<()> {
    if settings.runners.is_empty() {
        return Ok(()); // No runners configured is valid (will error at worker startup)
    }

    for (name, config) in &settings.runners {
        // Check for missing required fields
        if config.tcp_socket.is_none() {
            return Err(anyhow::anyhow!(
                "runner '{name}' is missing required field 'tcp_socket' (e.g., \"127.0.0.1:9000\")"
            ));
        }

        if config.cmd.is_none() {
            return Err(anyhow::anyhow!(
                "runner '{name}' is missing required field 'cmd' (e.g., [\"rrq-runner\", \"--settings\", \"myapp.settings\"])"
            ));
        }

        // Validate tcp_socket format, host, and port
        if let Some(ref socket) = config.tcp_socket {
            let spec = parse_tcp_socket(socket)
                .with_context(|| format!("runner '{name}' has invalid tcp_socket '{socket}'"))?;

            // Validate port range is sufficient for pool_size
            let pool_size = config.pool_size.unwrap_or(1);
            let max_port = spec.port as u32 + pool_size.saturating_sub(1) as u32;
            if max_port > u16::MAX as u32 {
                return Err(anyhow::anyhow!(
                    "runner '{name}' tcp_socket port range insufficient for pool_size {pool_size} \
                    (port {} + {} would exceed 65535)",
                    spec.port,
                    pool_size - 1
                ));
            }
        }

        // Validate pool_size if specified
        if let Some(pool_size) = config.pool_size
            && pool_size == 0
        {
            return Err(anyhow::anyhow!(
                "runner '{name}' has invalid pool_size: 0 - must be a positive integer"
            ));
        }

        // Validate max_in_flight if specified
        if let Some(max_in_flight) = config.max_in_flight
            && max_in_flight == 0
        {
            return Err(anyhow::anyhow!(
                "runner '{name}' has invalid max_in_flight: 0 - must be a positive integer"
            ));
        }
    }

    // Check that default_runner_name references a configured runner
    if !settings.default_runner_name.is_empty()
        && !settings.runners.contains_key(&settings.default_runner_name)
    {
        let available: Vec<_> = settings.runners.keys().collect();
        if available.is_empty() {
            return Err(anyhow::anyhow!(
                "default_runner_name is '{}' but no runners are configured",
                settings.default_runner_name
            ));
        }
        return Err(anyhow::anyhow!(
            "default_runner_name '{}' does not match any configured runner. Available runners: {:?}",
            settings.default_runner_name,
            available
        ));
    }

    Ok(())
}

#[cfg(test)]
#[allow(unsafe_code)] // env var manipulation in tests
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use uuid::Uuid;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn load_toml_settings_merges_env_and_normalizes_fields() {
        let _lock = env_lock().lock().unwrap();
        unsafe {
            std::env::set_var("RRQ_DEFAULT_QUEUE_NAME", "from_env");
            std::env::set_var("RRQ_WORKER_HEALTH_CHECK_TTL_BUFFER_SECONDS", "12.5");
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        redis_dsn = "redis://localhost:6379/9"
        default_queue_name = "from_toml"
        [rrq.routing]
        alpha = "python"
        "#;
        fs::write(&path, config).unwrap();

        let settings = load_toml_settings(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(settings.default_queue_name, "from_env");
        assert_eq!(
            settings.runner_routes.get("alpha"),
            Some(&"python".to_string())
        );
        assert_eq!(settings.worker_health_check_ttl_buffer_seconds, 12.5);
    }

    #[test]
    fn resolve_config_source_prefers_explicit_path() {
        let (path, source) = resolve_config_source(Some("custom.toml"));
        assert_eq!(path, Some("custom.toml".to_string()));
        assert_eq!(source, "--config parameter");
    }

    #[test]
    fn resolve_config_source_falls_back_to_env() {
        let _lock = env_lock().lock().unwrap();
        let value = format!("rrq-{}.toml", Uuid::new_v4());
        unsafe {
            std::env::set_var(ENV_CONFIG_KEY, &value);
        }
        let (path, source) = resolve_config_source(None);
        assert_eq!(path, Some(value));
        assert!(source.contains(ENV_CONFIG_KEY));
    }

    #[test]
    fn validate_runner_configs_accepts_valid_config() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        default_runner_name = "python"
        [rrq.runners.python]
        cmd = ["rrq-runner", "--settings", "myapp.settings"]
        tcp_socket = "127.0.0.1:9000"
        pool_size = 2
        max_in_flight = 10
        "#;
        fs::write(&path, config).unwrap();
        let settings = load_toml_settings(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(settings.runners.len(), 1);
        assert!(settings.runners.contains_key("python"));
    }

    #[test]
    fn validate_runner_configs_rejects_missing_tcp_socket() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("tcp_socket"));
    }

    #[test]
    fn validate_runner_configs_rejects_missing_cmd() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        tcp_socket = "127.0.0.1:9000"
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("cmd"));
    }

    #[test]
    fn validate_runner_configs_rejects_non_loopback() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        tcp_socket = "10.0.0.1:9000"
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        // Error chain includes "loopback" in the cause
        let full_err = format!("{err:?}");
        assert!(full_err.contains("loopback"), "error was: {full_err}");
    }

    #[test]
    fn validate_runner_configs_rejects_zero_pool_size() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        tcp_socket = "127.0.0.1:9000"
        pool_size = 0
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("pool_size"));
    }

    #[test]
    fn validate_runner_configs_rejects_zero_max_in_flight() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        tcp_socket = "127.0.0.1:9000"
        max_in_flight = 0
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("max_in_flight"));
    }

    #[test]
    fn validate_runner_configs_rejects_mismatched_default_runner() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        default_runner_name = "node"
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        tcp_socket = "127.0.0.1:9000"
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("node"));
        assert!(err.to_string().contains("python"));
    }

    #[test]
    fn validate_runner_configs_rejects_unknown_field() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        tcp_socket = "127.0.0.1:9000"
        pool_siz = 4
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn validate_runner_configs_rejects_port_overflow() {
        let _lock = env_lock().lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rrq.toml");
        let config = r#"
        [rrq]
        [rrq.runners.python]
        cmd = ["rrq-runner"]
        tcp_socket = "127.0.0.1:65535"
        pool_size = 2
        "#;
        fs::write(&path, config).unwrap();
        let err = load_toml_settings(Some(path.to_str().unwrap())).unwrap_err();
        assert!(err.to_string().contains("port range"));
    }
}
