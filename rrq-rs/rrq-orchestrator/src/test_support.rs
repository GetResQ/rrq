use std::path::Path;
use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::Result;
use tokio::fs;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

use crate::settings::RRQSettings;
use crate::store::JobStore;

static REDIS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn redis_lock() -> &'static Mutex<()> {
    REDIS_LOCK.get_or_init(|| Mutex::new(()))
}

pub struct RedisTestContext {
    _guard: MutexGuard<'static, ()>,
    pub settings: RRQSettings,
    pub store: JobStore,
}

#[allow(dead_code)]
pub struct TempConfig {
    path: PathBuf,
}

impl TempConfig {
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl RedisTestContext {
    pub async fn new() -> Result<Self> {
        let guard = redis_lock().lock().await;
        let mut settings = RRQSettings::default();
        let redis_dsn = std::env::var("RRQ_TEST_REDIS_DSN")
            .unwrap_or_else(|_| "redis://localhost:6379/15".to_string());
        settings.redis_dsn = redis_dsn;
        settings.default_queue_name = format!("test-queue-{}", Uuid::new_v4());
        settings.default_dlq_name = format!("test-dlq-{}", Uuid::new_v4());
        let mut store = JobStore::new(settings.clone()).await?;
        store.flushdb().await?;
        Ok(Self {
            _guard: guard,
            settings,
            store,
        })
    }

    #[allow(dead_code)]
    pub async fn write_config(&self) -> Result<TempConfig> {
        let path = std::env::temp_dir().join(format!("rrq-test-{}.toml", Uuid::new_v4()));
        let payload = format!(
            "[rrq]\nredis_dsn = \"{}\"\ndefault_queue_name = \"{}\"\ndefault_dlq_name = \"{}\"\n",
            self.settings.redis_dsn,
            self.settings.default_queue_name,
            self.settings.default_dlq_name
        );
        fs::write(&path, payload).await?;
        Ok(TempConfig { path })
    }
}
