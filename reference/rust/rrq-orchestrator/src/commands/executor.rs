use std::process::Stdio;

use anyhow::Result;
use tokio::process::Command;

pub(crate) async fn executor_python(settings: Option<String>) -> Result<()> {
    let mut cmd = Command::new("rrq-executor");
    if let Some(settings) = settings {
        cmd.arg("--settings").arg(settings);
    }
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = cmd.status().await?;
    if !status.success() {
        anyhow::bail!("rrq-executor exited with status {status}");
    }
    Ok(())
}
