use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rrq_protocol::{ExecutionOutcome, ExecutionRequest};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};

#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome>;
    async fn cancel(&self, _job_id: &str) -> Result<()> {
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

struct StdioProcess {
    child: Child,
    stderr_task: Option<JoinHandle<()>>,
}

pub struct StdioExecutorPool {
    cmd: Vec<String>,
    pool_size: usize,
    env: Option<HashMap<String, String>>,
    cwd: Option<String>,
    idle: Mutex<Vec<StdioProcess>>,
    notify: Notify,
    response_timeout: Option<Duration>,
}

impl StdioExecutorPool {
    pub async fn new(
        cmd: Vec<String>,
        pool_size: usize,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        response_timeout: Option<Duration>,
    ) -> Result<Self> {
        if pool_size == 0 {
            return Err(anyhow::anyhow!("pool_size must be positive"));
        }
        if cmd.is_empty() {
            return Err(anyhow::anyhow!("cmd must not be empty"));
        }
        let pool = Self {
            cmd,
            pool_size,
            env,
            cwd,
            idle: Mutex::new(Vec::new()),
            notify: Notify::new(),
            response_timeout,
        };
        pool.ensure_started().await?;
        Ok(pool)
    }

    async fn ensure_started(&self) -> Result<()> {
        let mut idle = self.idle.lock().await;
        if !idle.is_empty() {
            return Ok(());
        }
        for _ in 0..self.pool_size {
            let proc = self.spawn_process().await?;
            idle.push(proc);
        }
        Ok(())
    }

    async fn spawn_process(&self) -> Result<StdioProcess> {
        let mut command = Command::new(&self.cmd[0]);
        if self.cmd.len() > 1 {
            command.args(&self.cmd[1..]);
        }
        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        if let Some(env) = &self.env {
            command.envs(env);
        }
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().context("failed to spawn stdio executor")?;
        let stderr = child.stderr.take();
        let stderr_task = if let Some(stderr) = stderr {
            Some(tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::warn!("[executor] {}", line);
                }
            }))
        } else {
            None
        };
        Ok(StdioProcess { child, stderr_task })
    }

    async fn acquire(&self) -> Result<StdioProcess> {
        loop {
            {
                let mut idle = self.idle.lock().await;
                if let Some(mut proc) = idle.pop() {
                    if let Ok(Some(_)) = proc.child.try_wait() {
                        proc = self.spawn_process().await?;
                    }
                    return Ok(proc);
                }
            }
            self.notify.notified().await;
        }
    }

    async fn release(&self, proc: StdioProcess) -> Result<()> {
        let mut idle = self.idle.lock().await;
        idle.push(proc);
        self.notify.notify_one();
        Ok(())
    }

    async fn invalidate(&self, mut proc: StdioProcess) -> Result<()> {
        if let Some(task) = proc.stderr_task.take() {
            task.abort();
        }
        let _ = proc.child.kill().await;
        let mut idle = self.idle.lock().await;
        let replacement = self.spawn_process().await?;
        idle.push(replacement);
        self.notify.notify_one();
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut idle = self.idle.lock().await;
        while let Some(mut proc) = idle.pop() {
            if let Some(task) = proc.stderr_task.take() {
                task.abort();
            }
            let _ = proc.child.kill().await;
        }
        Ok(())
    }
}

pub struct StdioExecutor {
    pool: Arc<StdioExecutorPool>,
}

impl StdioExecutor {
    pub async fn new(
        cmd: Vec<String>,
        pool_size: usize,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        response_timeout_seconds: Option<f64>,
    ) -> Result<Self> {
        let response_timeout = response_timeout_seconds.map(Duration::from_secs_f64);
        let pool = StdioExecutorPool::new(cmd, pool_size, env, cwd, response_timeout).await?;
        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    async fn execute_with_process(
        &self,
        proc: &mut StdioProcess,
        request: &ExecutionRequest,
    ) -> Result<ExecutionOutcome> {
        let stdin = proc
            .child
            .stdin
            .as_mut()
            .context("executor stdin missing")?;
        let stdout = proc
            .child
            .stdout
            .as_mut()
            .context("executor stdout missing")?;

        let payload = serde_json::to_string(request)?;
        stdin.write_all(payload.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;

        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let read_line = reader.read_line(&mut line);
        if let Some(timeout_duration) = self.pool.response_timeout {
            timeout(timeout_duration, read_line)
                .await
                .context("executor response timeout")??;
        } else {
            read_line.await?;
        }
        if line.is_empty() {
            return Err(anyhow::anyhow!("executor process exited"));
        }
        let outcome: ExecutionOutcome = serde_json::from_str(line.trim())?;
        if outcome.job_id.as_deref() != Some(request.job_id.as_str()) {
            return Err(anyhow::anyhow!(
                "executor outcome job_id mismatch (expected {}, got {:?})",
                request.job_id,
                outcome.job_id
            ));
        }
        Ok(outcome)
    }
}

#[async_trait]
impl Executor for StdioExecutor {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
        let mut proc = self.pool.acquire().await?;
        let result = self.execute_with_process(&mut proc, &request).await;
        match result {
            Ok(outcome) => {
                self.pool.release(proc).await?;
                Ok(outcome)
            }
            Err(err) => {
                self.pool.invalidate(proc).await?;
                Err(err)
            }
        }
    }

    async fn close(&self) -> Result<()> {
        self.pool.close().await
    }
}

pub fn resolve_executor_pool_sizes(
    settings: &crate::settings::RRQSettings,
    watch_mode: bool,
    default_pool_size: Option<usize>,
) -> Result<HashMap<String, usize>> {
    let default_pool_size = default_pool_size.unwrap_or_else(num_cpus::get);
    let mut pool_sizes = HashMap::new();
    for (name, config) in &settings.executors {
        let pool_size = if watch_mode {
            1
        } else {
            config.pool_size.unwrap_or(default_pool_size)
        };
        if pool_size == 0 {
            return Err(anyhow::anyhow!(
                "pool_size must be positive for executor '{}'",
                name
            ));
        }
        pool_sizes.insert(name.clone(), pool_size);
    }
    Ok(pool_sizes)
}

pub async fn build_executors_from_settings(
    settings: &crate::settings::RRQSettings,
    pool_sizes: Option<&HashMap<String, usize>>,
) -> Result<HashMap<String, Arc<dyn Executor>>> {
    let pool_sizes = match pool_sizes {
        Some(map) => map.clone(),
        None => resolve_executor_pool_sizes(settings, false, None)?,
    };
    let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
    for (name, config) in &settings.executors {
        if config.cmd.is_none() {
            return Err(anyhow::anyhow!(
                "executor '{}' requires cmd for stdio mode",
                name
            ));
        }
        let pool_size = pool_sizes
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing pool size for executor '{}'", name))?;
        let cmd = config.cmd.clone().unwrap_or_default();
        let executor = StdioExecutor::new(
            cmd,
            pool_size,
            config.env.clone(),
            config.cwd.clone(),
            config.response_timeout_seconds,
        )
        .await?;
        executors.insert(name.clone(), Arc::new(executor));
    }
    Ok(executors)
}
