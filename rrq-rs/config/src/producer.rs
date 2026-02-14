use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::settings::RRQSettings;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ProducerSettings {
    pub redis_dsn: String,
    pub queue_name: String,
    pub max_retries: i64,
    pub job_timeout_seconds: i64,
    pub result_ttl_seconds: i64,
    pub idempotency_ttl_seconds: i64,
    #[serde(default)]
    pub correlation_mappings: HashMap<String, String>,
}

impl From<&RRQSettings> for ProducerSettings {
    fn from(settings: &RRQSettings) -> Self {
        Self {
            redis_dsn: settings.redis_dsn.clone(),
            queue_name: settings.default_queue_name.clone(),
            max_retries: settings.default_max_retries,
            job_timeout_seconds: settings.default_job_timeout_seconds,
            result_ttl_seconds: settings.default_result_ttl_seconds,
            idempotency_ttl_seconds: settings.default_unique_job_lock_ttl_seconds,
            correlation_mappings: settings.correlation_mappings.clone(),
        }
    }
}

pub fn load_producer_settings(config_path: Option<&str>) -> Result<ProducerSettings> {
    let settings = crate::config::load_toml_settings(config_path)?;
    Ok(ProducerSettings::from(&settings))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_settings_from_rrq_settings_copies_core_and_correlation_fields() {
        let settings = RRQSettings {
            redis_dsn: "redis://localhost:6379/9".to_string(),
            default_queue_name: "rrq:queue:test".to_string(),
            default_max_retries: 11,
            default_job_timeout_seconds: 222,
            default_result_ttl_seconds: 333,
            default_unique_job_lock_ttl_seconds: 444,
            correlation_mappings: HashMap::from([
                ("session_id".to_string(), "params.session.id".to_string()),
                ("message_id".to_string(), "params.message.id".to_string()),
            ]),
            ..Default::default()
        };

        let producer = ProducerSettings::from(&settings);
        assert_eq!(producer.redis_dsn, "redis://localhost:6379/9");
        assert_eq!(producer.queue_name, "rrq:queue:test");
        assert_eq!(producer.max_retries, 11);
        assert_eq!(producer.job_timeout_seconds, 222);
        assert_eq!(producer.result_ttl_seconds, 333);
        assert_eq!(producer.idempotency_ttl_seconds, 444);
        assert_eq!(
            producer
                .correlation_mappings
                .get("session_id")
                .map(String::as_str),
            Some("params.session.id")
        );
        assert_eq!(
            producer
                .correlation_mappings
                .get("message_id")
                .map(String::as_str),
            Some("params.message.id")
        );
    }
}
