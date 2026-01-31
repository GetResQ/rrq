use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{Map, Value};

use crate::settings::RRQSettings;

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

    let settings: RRQSettings = serde_json::from_value(merged).context("invalid RRQ config")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use uuid::Uuid;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        prev: Vec<(&'static str, Option<String>)>,
    }

    #[allow(unsafe_code)] // env var manipulation in tests
    impl EnvGuard {
        fn set_many(pairs: &[(&'static str, &str)]) -> Self {
            let lock = env_lock().lock().unwrap();
            let mut prev = Vec::with_capacity(pairs.len());
            for (key, value) in pairs {
                prev.push((*key, std::env::var(key).ok()));
                unsafe {
                    std::env::set_var(key, value);
                }
            }
            Self { _lock: lock, prev }
        }
    }

    #[allow(unsafe_code)] // env var manipulation in tests
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, prev) in self.prev.drain(..) {
                if let Some(value) = prev {
                    unsafe {
                        std::env::set_var(key, value);
                    }
                } else {
                    unsafe {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }

    #[test]
    fn resolve_config_source_prefers_explicit_path() {
        let (path, source) = resolve_config_source(Some("custom.toml"));
        assert_eq!(path, Some("custom.toml".to_string()));
        assert!(source.contains("--config"));
    }

    #[test]
    fn load_toml_settings_merges_env_and_normalizes_fields() {
        let tmp_path = std::env::temp_dir().join(format!("rrq-test-{}.toml", Uuid::new_v4()));
        let payload = r#"
[rrq]
default_queue_name = "from_toml"
worker_concurrency = 2

[rrq.routing]
alpha = "beta"
"#;
        fs::write(&tmp_path, payload).unwrap();
        let _guard = EnvGuard::set_many(&[
            ("RRQ_DEFAULT_QUEUE_NAME", "from_env"),
            ("RRQ_WORKER_HEALTH_CHECK_TTL_BUFFER_SECONDS", "12.5"),
        ]);
        let settings = load_toml_settings(Some(tmp_path.to_str().unwrap())).unwrap();
        assert_eq!(settings.default_queue_name, "from_env");
        assert_eq!(
            settings.runner_routes.get("alpha"),
            Some(&"beta".to_string())
        );
        assert_eq!(settings.worker_health_check_ttl_buffer_seconds, 12.5);
        let _ = fs::remove_file(&tmp_path);
    }
}
