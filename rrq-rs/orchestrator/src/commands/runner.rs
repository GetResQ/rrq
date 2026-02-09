use std::process::Stdio;

use anyhow::Result;
use tokio::process::Command;

pub(crate) async fn runner_python(settings: String, tcp_socket: String) -> Result<()> {
    let mut cmd = Command::new("rrq-runner");
    cmd.arg("--settings").arg(settings);
    cmd.arg("--tcp-socket").arg(tcp_socket);
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = cmd.status().await?;
    if !status.success() {
        anyhow::bail!("rrq-runner exited with status {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tokio::fs;
    use uuid::Uuid;

    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    #[allow(unsafe_code)] // env var manipulation in tests
    impl EnvGuard {
        fn set(key: &'static str, value: String) -> Self {
            let prev = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, prev }
        }
    }

    #[allow(unsafe_code)] // env var manipulation in tests
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(prev) = self.prev.take() {
                unsafe {
                    std::env::set_var(self.key, prev);
                }
            } else {
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    #[tokio::test]
    async fn runner_python_runs_command() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("rrq-exec-{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).await?;
        let script_path = dir.join("rrq-runner");
        let script = "#!/bin/sh\nexit 0\n";
        fs::write(&script_path, script).await?;
        let mut perms = fs::metadata(&script_path).await?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script_path, perms).await?;

        let original = std::env::var("PATH").unwrap_or_default();
        let new_path = format!("{}:{}", dir.to_string_lossy(), original);
        let _guard = EnvGuard::set("PATH", new_path);

        runner_python("settings.toml".to_string(), "127.0.0.1:1234".to_string()).await?;

        let _ = fs::remove_file(&script_path).await;
        let _ = fs::remove_dir_all(&dir).await;
        Ok(())
    }
}
