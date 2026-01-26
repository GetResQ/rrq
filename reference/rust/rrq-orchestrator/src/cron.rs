use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{DateTime, Timelike, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CronJob {
    pub function_name: String,
    pub schedule: String,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub kwargs: serde_json::Map<String, Value>,
    #[serde(default)]
    pub queue_name: Option<String>,
    #[serde(default)]
    pub unique: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub next_run_time: Option<DateTime<Utc>>,
}

impl CronJob {
    pub fn schedule_next(&mut self, now: DateTime<Utc>) -> Result<()> {
        let schedule = Schedule::from_str(&self.schedule)
            .with_context(|| format!("invalid cron schedule: {}", self.schedule))?;
        let base = now
            .with_second(0)
            .and_then(|dt| dt.with_nanosecond(0))
            .unwrap_or(now);
        let next = schedule
            .after(&base)
            .next()
            .context("cron schedule produced no next run time")?;
        self.next_run_time = Some(next);
        Ok(())
    }

    pub fn due(&mut self, now: DateTime<Utc>) -> Result<bool> {
        if self.next_run_time.is_none() {
            self.schedule_next(now)?;
        }
        Ok(now >= self.next_run_time.unwrap_or(now))
    }
}
