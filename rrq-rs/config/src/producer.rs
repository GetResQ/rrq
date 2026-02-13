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
