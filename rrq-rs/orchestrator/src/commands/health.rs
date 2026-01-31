use anyhow::Result;

use rrq::constants::HEALTH_KEY_PREFIX;
use rrq::load_toml_settings;
use rrq::store::JobStore;

pub(crate) async fn check_workers(config: Option<String>) -> Result<()> {
    let settings = load_toml_settings(config.as_deref())?;
    let mut store = JobStore::new(settings.clone()).await?;
    let mut cursor = 0u64;
    let mut keys: Vec<String> = Vec::new();
    loop {
        let (next, batch) = store.scan_worker_health_keys(cursor, 100).await?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    if keys.is_empty() {
        println!("Worker Health Check: FAIL (No active workers found)");
        return Ok(());
    }
    println!(
        "Worker Health Check: Found {} active worker(s):",
        keys.len()
    );
    for key in keys {
        let worker_id = key.trim_start_matches(HEALTH_KEY_PREFIX);
        let (health, ttl) = store.get_worker_health(worker_id).await?;
        if let Some(health) = health {
            println!("  - Worker ID: {worker_id}");
            if let Some(status) = health.get("status") {
                println!("    Status: {status}");
            }
            if let Some(active_jobs) = health.get("active_jobs") {
                println!("    Active Jobs: {active_jobs}");
            }
            if let Some(timestamp) = health.get("timestamp") {
                println!("    Last Heartbeat: {timestamp}");
            }
            println!("    TTL: {} seconds", ttl.unwrap_or(0));
        } else {
            println!("  - Worker ID: {worker_id} - Health data missing");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::RedisTestContext;
    use serde_json::Value;

    #[tokio::test]
    async fn health_check_handles_missing_and_present_workers() -> Result<()> {
        let mut ctx = RedisTestContext::new().await?;
        let config = ctx.write_config().await?;
        let config_path = Some(config.path().to_string_lossy().to_string());

        check_workers(config_path.clone()).await?;

        let mut payload = serde_json::Map::new();
        payload.insert(
            "worker_id".to_string(),
            Value::String("worker-1".to_string()),
        );
        payload.insert("status".to_string(), Value::String("running".to_string()));
        payload.insert("active_jobs".to_string(), Value::from(1));
        payload.insert("timestamp".to_string(), Value::from(1234));
        ctx.store
            .set_worker_health("worker-1", &payload, 60)
            .await?;

        check_workers(config_path).await?;
        Ok(())
    }
}
