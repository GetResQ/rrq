use anyhow::Result;

use rrq::config::load_toml_settings;
use rrq::constants::HEALTH_KEY_PREFIX;
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
