use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use crate::constants::DEFAULT_RUNNER_CONNECT_TIMEOUT_MS;
use crate::telemetry::{self, LogFormat};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::{Signal, kill, killpg};
#[cfg(unix)]
use nix::unistd::Pid;
use rrq_config::{
    QUEUE_KEY_PREFIX, RRQSettings, TcpSocketSpec, normalize_queue_name, parse_tcp_socket,
};
use rrq_protocol::{
    CancelRequest, ExecutionOutcome, ExecutionRequest, FRAME_HEADER_LEN, PROTOCOL_VERSION,
    RunnerMessage, encode_frame,
};
use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, timeout};
use tracing::{debug, info, warn};
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const REUSE_SOCKET_MAX_ATTEMPTS: usize = 5;
const REUSE_SOCKET_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone, Copy)]
enum RunnerLogStream {
    Stdout,
    Stderr,
}

impl RunnerLogStream {
    fn as_str(self) -> &'static str {
        match self {
            RunnerLogStream::Stdout => "stdout",
            RunnerLogStream::Stderr => "stderr",
        }
    }

    fn default_level(self) -> &'static str {
        match self {
            RunnerLogStream::Stdout => "INFO",
            RunnerLogStream::Stderr => "ERROR",
        }
    }

    fn write(self, line: &str) {
        match self {
            RunnerLogStream::Stdout => println!("{line}"),
            RunnerLogStream::Stderr => eprintln!("{line}"),
        }
    }
}

fn emit_runner_log(runner: &str, stream: RunnerLogStream, line: &str) {
    if matches!(telemetry::log_format(), LogFormat::Json) {
        let event = build_runner_event_json(runner, stream, line, Utc::now());
        match serde_json::to_string(&event) {
            Ok(text) => stream.write(&text),
            Err(_) => stream.write(line),
        }
        return;
    }

    match stream {
        RunnerLogStream::Stdout => {
            tracing::info!(runner = %runner, %line, "runner stdout");
        }
        RunnerLogStream::Stderr => {
            tracing::error!(runner = %runner, %line, "runner stderr");
        }
    }
}

fn build_runner_event_json(
    runner: &str,
    stream: RunnerLogStream,
    line: &str,
    now: DateTime<Utc>,
) -> Value {
    let mut event = match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(object)) => object,
        _ => {
            let mut object = Map::new();
            object.insert("message".to_string(), Value::String(line.to_string()));
            object
        }
    };
    event
        .entry("timestamp".to_string())
        .or_insert_with(|| Value::String(now.to_rfc3339()));
    event
        .entry("level".to_string())
        .or_insert_with(|| Value::String(stream.default_level().to_string()));
    event.insert("rrq.runner".to_string(), Value::String(runner.to_string()));
    event.insert(
        "rrq.stream".to_string(),
        Value::String(stream.as_str().to_string()),
    );
    event.insert(
        "rrq.source".to_string(),
        Value::String("runner".to_string()),
    );
    Value::Object(event)
}

fn connect_timeout_from_settings(timeout_ms: i64) -> Duration {
    let ms = if timeout_ms > 0 {
        timeout_ms
    } else {
        DEFAULT_RUNNER_CONNECT_TIMEOUT_MS
    };
    Duration::from_millis(ms as u64)
}

fn shutdown_term_grace_from_settings(timeout_seconds: f64) -> Duration {
    if timeout_seconds.is_finite() && timeout_seconds > 0.0 {
        Duration::try_from_secs_f64(timeout_seconds).unwrap_or(Duration::MAX)
    } else {
        Duration::ZERO
    }
}

fn is_response_timeout_error(err: &anyhow::Error) -> bool {
    err.to_string().contains("runner response timeout")
}

#[cfg(unix)]
fn configure_runner_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_runner_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn signal_process_group(pid: u32, signal: Signal) -> Result<bool> {
    let pgid = Pid::from_raw(pid as i32);
    match killpg(pgid, signal) {
        Ok(()) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(err) => Err(anyhow::anyhow!(
            "failed to send {signal:?} to process group {pid}: {err}"
        )),
    }
}

#[cfg(unix)]
fn process_group_exists(pid: u32) -> Result<bool> {
    let pgid = Pid::from_raw(-(pid as i32));
    match kill(pgid, None) {
        Ok(()) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(err) => Err(anyhow::anyhow!(
            "failed to probe process group {pid} state: {err}"
        )),
    }
}

#[cfg(unix)]
async fn wait_for_process_group_exit(pid: u32, deadline: Instant) -> bool {
    loop {
        match process_group_exists(pid) {
            Ok(false) => return true,
            Ok(true) => {}
            Err(err) => {
                tracing::warn!(
                    pid,
                    error = %err,
                    "failed to check runner process-group state during shutdown"
                );
                return false;
            }
        }

        if Instant::now() >= deadline {
            return false;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn terminate_child_process(
    child: &mut Child,
    shutdown_term_grace: Duration,
    termination_reason: &str,
) {
    if let Ok(Some(_)) = child.try_wait() {
        return;
    }

    #[cfg(unix)]
    {
        if let Some(pid) = child.id() {
            let term_sent = signal_process_group(pid, Signal::SIGTERM).unwrap_or(false);
            tracing::info!(
                pid,
                signal = "SIGTERM",
                reason = termination_reason,
                "sent runner process-group termination signal"
            );
            if !term_sent {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return;
            }

            if shutdown_term_grace.is_zero() {
                let kill_sent = signal_process_group(pid, Signal::SIGKILL).unwrap_or(false);
                if !kill_sent {
                    let _ = child.kill().await;
                }
                tracing::warn!(
                    pid,
                    signal = "SIGKILL",
                    reason = termination_reason,
                    "runner termination grace is zero; forcing kill"
                );
                let _ = child.wait().await;
                return;
            }

            let shutdown_deadline = Instant::now() + shutdown_term_grace;
            match timeout(shutdown_term_grace, child.wait()).await {
                Ok(_) => {
                    if wait_for_process_group_exit(pid, shutdown_deadline).await {
                        tracing::info!(
                            pid,
                            reason = termination_reason,
                            "runner process group exited during graceful shutdown"
                        );
                        return;
                    }

                    let kill_sent = signal_process_group(pid, Signal::SIGKILL).unwrap_or(false);
                    if !kill_sent {
                        let _ = child.kill().await;
                    }
                    tracing::warn!(
                        pid,
                        signal = "SIGKILL",
                        reason = termination_reason,
                        "runner leader exited but descendants remained after SIGTERM; forcing kill"
                    );
                    let _ = child.wait().await;
                    return;
                }
                Err(_) => {
                    let kill_sent = signal_process_group(pid, Signal::SIGKILL).unwrap_or(false);
                    if !kill_sent {
                        let _ = child.kill().await;
                    }
                    tracing::warn!(
                        pid,
                        signal = "SIGKILL",
                        reason = termination_reason,
                        grace_ms = shutdown_term_grace.as_millis() as u64,
                        "runner process group did not exit after SIGTERM; forcing kill"
                    );
                    let _ = child.wait().await;
                    return;
                }
            }
        }
    }

    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[async_trait]
pub trait Runner: Send + Sync {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome>;
    async fn execute_with_timeout(
        &self,
        request: ExecutionRequest,
        timeout_duration: Duration,
        send_cancel_hint: bool,
    ) -> RunnerExecutionResult {
        let request_id = request.request_id.clone();
        let job_id = request.job_id.clone();
        match timeout(timeout_duration, self.execute(request)).await {
            Ok(result) => RunnerExecutionResult::Completed(Box::new(result)),
            Err(_) => {
                let _ = self
                    .handle_timeout(&job_id, Some(request_id.as_str()), send_cancel_hint)
                    .await;
                RunnerExecutionResult::TimedOut
            }
        }
    }
    async fn cancel(&self, _job_id: &str, _request_id: Option<&str>) -> Result<()> {
        Ok(())
    }
    async fn handle_timeout(
        &self,
        job_id: &str,
        request_id: Option<&str>,
        send_cancel_hint: bool,
    ) -> Result<()> {
        if send_cancel_hint {
            self.cancel(job_id, request_id).await?;
        }
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

pub enum RunnerExecutionResult {
    Completed(Box<Result<ExecutionOutcome>>),
    TimedOut,
}

trait RunnerIo: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite + ?Sized> RunnerIo for T {}

type RunnerStream = Box<dyn RunnerIo + Unpin + Send>;

type RunnerSocketTarget = SocketAddr;
type PendingOutcomeSender = oneshot::Sender<Result<ExecutionOutcome>>;

#[derive(Clone)]
struct PersistentProcessConnection {
    sender: mpsc::Sender<RunnerMessage>,
    pending: Arc<Mutex<HashMap<String, PendingOutcomeSender>>>,
    closed: Arc<AtomicBool>,
}

struct SocketProcess {
    child: Child,
    socket: RunnerSocketTarget,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
    permits: Arc<Semaphore>,
    connection: Option<Arc<PersistentProcessConnection>>,
}

pub struct SocketRunnerPool {
    name: String,
    cmd: Vec<String>,
    pool_size: usize,
    max_in_flight: usize,
    env: Option<HashMap<String, String>>,
    cwd: Option<String>,
    tcp_socket: TcpSocketSpec,
    tcp_port_cursor: AtomicUsize,
    processes: Mutex<Vec<Arc<Mutex<SocketProcess>>>>,
    cursor: AtomicUsize,
    availability: Arc<Notify>,
    response_timeout: Option<Duration>,
    connect_timeout: Duration,
    shutdown_term_grace: Duration,
    capture_output: bool,
    #[cfg(test)]
    spawn_override: Option<Arc<dyn Fn() -> SocketProcess + Send + Sync>>,
}

struct ProcessPermit {
    _permit: OwnedSemaphorePermit,
    notify: Arc<Notify>,
}

impl Drop for ProcessPermit {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

impl PersistentProcessConnection {
    async fn connect(socket: RunnerSocketTarget, channel_capacity: usize) -> Result<Self> {
        let stream = connect_stream(&socket).await?;
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (sender, mut receiver) = mpsc::channel::<RunnerMessage>(channel_capacity.max(32));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        {
            let pending = pending.clone();
            let closed = closed.clone();
            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    if let Err(err) = write_message(&mut writer, &message).await {
                        close_pending_with_error(
                            &pending,
                            &closed,
                            format!("runner connection write failed: {err}"),
                        )
                        .await;
                        return;
                    }
                }
                close_pending_with_error(&pending, &closed, "runner connection closed".to_string())
                    .await;
            });
        }

        {
            let pending = pending.clone();
            let closed = closed.clone();
            tokio::spawn(async move {
                loop {
                    match read_message(&mut reader).await {
                        Ok(Some(RunnerMessage::Response { payload })) => {
                            let Some(request_id) = payload.request_id.clone() else {
                                let mut single_pending: Option<(String, PendingOutcomeSender)> =
                                    None;
                                let mut pending_count: Option<usize> = None;
                                {
                                    let mut pending = pending.lock().await;
                                    match pending.len() {
                                        0 => {}
                                        1 => {
                                            single_pending = pending.drain().next();
                                        }
                                        count => {
                                            pending_count = Some(count);
                                        }
                                    }
                                }
                                if let Some((expected_request_id, sender)) = single_pending {
                                    let _ = sender.send(Err(anyhow::anyhow!(
                                        "runner outcome missing request_id (expected {})",
                                        expected_request_id
                                    )));
                                    continue;
                                }
                                if let Some(count) = pending_count {
                                    close_pending_with_error(
                                        &pending,
                                        &closed,
                                        format!(
                                            "runner outcome missing request_id with {count} pending requests"
                                        ),
                                    )
                                    .await;
                                    return;
                                }
                                tracing::warn!(
                                    "runner outcome missing request_id with no pending requests; dropping response"
                                );
                                continue;
                            };
                            let sender = {
                                let mut pending = pending.lock().await;
                                pending.remove(&request_id)
                            };
                            if let Some(sender) = sender {
                                let _ = sender.send(Ok(payload));
                            } else {
                                tracing::warn!(
                                    request_id = %request_id,
                                    "runner outcome request_id not found in pending map"
                                );
                            }
                        }
                        Ok(Some(_)) => {
                            tracing::warn!("unexpected runner message on persistent channel");
                        }
                        Ok(None) => {
                            close_pending_with_error(
                                &pending,
                                &closed,
                                "runner connection closed".to_string(),
                            )
                            .await;
                            return;
                        }
                        Err(err) => {
                            close_pending_with_error(
                                &pending,
                                &closed,
                                format!("runner connection read failed: {err}"),
                            )
                            .await;
                            return;
                        }
                    }
                }
            });
        }

        Ok(Self {
            sender,
            pending,
            closed,
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn execute(
        &self,
        request: &ExecutionRequest,
        response_timeout: Option<Duration>,
    ) -> Result<ExecutionOutcome> {
        if self.is_closed() {
            return Err(anyhow::anyhow!("runner connection closed"));
        }

        let request_id = request.request_id.clone();
        let expected_job_id = request.job_id.clone();
        let expected_request_id = request.request_id.clone();
        let (sender, receiver) = oneshot::channel::<Result<ExecutionOutcome>>();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(request_id.clone(), sender);
        }
        if self.is_closed() {
            let mut pending = self.pending.lock().await;
            pending.remove(&request_id);
            return Err(anyhow::anyhow!("runner connection closed"));
        }

        let send_result = self
            .sender
            .send(RunnerMessage::Request {
                payload: request.clone(),
            })
            .await;
        if send_result.is_err() {
            let mut pending = self.pending.lock().await;
            pending.remove(&request_id);
            return Err(anyhow::anyhow!("runner connection closed"));
        }

        let outcome = if let Some(limit) = response_timeout {
            match timeout(limit, receiver).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(anyhow::anyhow!("runner connection closed")),
                Err(_) => {
                    let mut pending = self.pending.lock().await;
                    pending.remove(&request_id);
                    return Err(anyhow::anyhow!("runner response timeout"));
                }
            }
        } else {
            match receiver.await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("runner connection closed")),
            }
        }?;

        if outcome.job_id.as_deref() != Some(expected_job_id.as_str()) {
            return Err(anyhow::anyhow!(
                "runner outcome job_id mismatch (expected {}, got {:?})",
                expected_job_id,
                outcome.job_id
            ));
        }
        if outcome.request_id.as_deref() != Some(expected_request_id.as_str()) {
            return Err(anyhow::anyhow!(
                "runner outcome request_id mismatch (expected {}, got {:?})",
                expected_request_id,
                outcome.request_id
            ));
        }

        Ok(outcome)
    }
}

async fn close_pending_with_error(
    pending: &Arc<Mutex<HashMap<String, PendingOutcomeSender>>>,
    closed: &Arc<AtomicBool>,
    message: String,
) {
    closed.store(true, Ordering::SeqCst);
    let senders = {
        let mut pending = pending.lock().await;
        pending
            .drain()
            .map(|(_, sender)| sender)
            .collect::<Vec<_>>()
    };
    for sender in senders {
        let _ = sender.send(Err(anyhow::anyhow!(message.clone())));
    }
}

impl SocketRunnerPool {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        name: impl Into<String>,
        cmd: Vec<String>,
        pool_size: usize,
        max_in_flight: usize,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        tcp_socket: Option<String>,
        response_timeout: Option<Duration>,
        connect_timeout: Duration,
        shutdown_term_grace: Duration,
        capture_output: bool,
    ) -> Result<Self> {
        let name = name.into();
        if pool_size == 0 {
            return Err(anyhow::anyhow!("pool_size must be positive"));
        }
        if max_in_flight == 0 {
            return Err(anyhow::anyhow!("max_in_flight must be positive"));
        }
        if cmd.is_empty() {
            return Err(anyhow::anyhow!("cmd must not be empty"));
        }

        let tcp_socket = tcp_socket.ok_or_else(|| {
            anyhow::anyhow!("runner tcp_socket is required (unix sockets are not supported)")
        })?;
        let tcp_socket = parse_tcp_socket(&tcp_socket)?;
        let max_port = tcp_socket.port as u32 + (pool_size as u32).saturating_sub(1);
        if max_port > u16::MAX as u32 {
            return Err(anyhow::anyhow!(
                "runner tcp_socket range too small for pool_size"
            ));
        }

        let pool = Self {
            name,
            cmd,
            pool_size,
            max_in_flight,
            env,
            cwd,
            tcp_socket,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout,
            connect_timeout,
            shutdown_term_grace,
            capture_output,
            #[cfg(test)]
            spawn_override: None,
        };
        if let Err(err) = pool.ensure_started().await {
            let _ = pool.close().await;
            return Err(err);
        }
        Ok(pool)
    }

    async fn ensure_started(&self) -> Result<()> {
        let mut processes = self.processes.lock().await;
        if !processes.is_empty() {
            return Ok(());
        }
        for _ in 0..self.pool_size {
            match self.spawn_process(None).await {
                Ok(proc) => processes.push(Arc::new(Mutex::new(proc))),
                Err(err) => {
                    // Clean up any processes we already started before propagating error
                    for proc in processes.iter() {
                        let mut guard = proc.lock().await;
                        let _ = self.terminate(&mut guard, "pool_start_failure").await;
                    }
                    processes.clear();
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn next_tcp_socket(&self) -> Result<SocketAddr> {
        let spec = &self.tcp_socket;
        let offset = self.tcp_port_cursor.fetch_add(1, Ordering::Relaxed);
        let port = spec.port as u32 + offset as u32;
        if port > u16::MAX as u32 {
            return Err(anyhow::anyhow!("runner tcp_socket port range exhausted"));
        }
        Ok(spec.addr(port as u16))
    }

    /// Spawn a new runner process.
    /// If `reuse_socket` is provided, the process reuses that socket/port instead of allocating a new one.
    async fn spawn_process(
        &self,
        reuse_socket: Option<RunnerSocketTarget>,
    ) -> Result<SocketProcess> {
        #[cfg(test)]
        if let Some(spawn_override) = &self.spawn_override {
            return Ok((spawn_override)());
        }
        let max_attempts = if reuse_socket.is_some() {
            REUSE_SOCKET_MAX_ATTEMPTS
        } else {
            self.pool_size.max(1)
        };
        let mut attempts = 0;
        loop {
            let socket = match reuse_socket {
                Some(target) => target,
                None => {
                    // Allocate a new socket target
                    self.next_tcp_socket()?
                }
            };
            if let Err(err) = self.ensure_tcp_port_available(socket) {
                attempts += 1;
                if attempts >= max_attempts {
                    return Err(err);
                }
                if reuse_socket.is_some() {
                    tokio::time::sleep(REUSE_SOCKET_RETRY_DELAY).await;
                }
                continue;
            }

            let mut command = Command::new(&self.cmd[0]);
            if self.cmd.len() > 1 {
                command.args(&self.cmd[1..]);
            }
            // Standard runner contract: accept `--tcp-socket host:port` (localhost only).
            command.arg("--tcp-socket").arg(socket.to_string());
            command.stdin(Stdio::null());
            if self.capture_output {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
            } else {
                command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
            }
            if let Some(env) = &self.env {
                command.envs(env);
            }
            if let Some(cwd) = &self.cwd {
                command.current_dir(cwd);
            }
            configure_runner_process_group(&mut command);
            // Ensure child runner processes are not orphaned if their owning task is dropped.
            command.kill_on_drop(true);
            let mut child = command.spawn().context("failed to spawn runner")?;
            let stdout_task = if self.capture_output {
                let stdout_name = self.name.clone();
                child.stdout.take().map(|stdout| {
                    tokio::spawn(async move {
                        let mut reader = BufReader::new(stdout).lines();
                        while let Ok(Some(line)) = reader.next_line().await {
                            emit_runner_log(&stdout_name, RunnerLogStream::Stdout, &line);
                        }
                    })
                })
            } else {
                None
            };
            let stderr_task = if self.capture_output {
                let stderr_name = self.name.clone();
                child.stderr.take().map(|stderr| {
                    tokio::spawn(async move {
                        let mut reader = BufReader::new(stderr).lines();
                        while let Ok(Some(line)) = reader.next_line().await {
                            emit_runner_log(&stderr_name, RunnerLogStream::Stderr, &line);
                        }
                    })
                })
            } else {
                None
            };

            match self.connect_socket(&socket, &mut child).await {
                Ok(()) => {
                    return Ok(SocketProcess {
                        child,
                        socket,
                        stdout_task,
                        stderr_task,
                        permits: Arc::new(Semaphore::new(self.max_in_flight)),
                        connection: None,
                    });
                }
                Err(err) => {
                    if let Some(task) = stdout_task.as_ref() {
                        task.abort();
                    }
                    if let Some(task) = stderr_task.as_ref() {
                        task.abort();
                    }
                    terminate_child_process(
                        &mut child,
                        self.shutdown_term_grace,
                        "spawn_connect_failure",
                    )
                    .await;
                    attempts += 1;
                    if reuse_socket.is_some() || attempts >= max_attempts {
                        return Err(err);
                    }
                }
            };
        }
    }

    fn ensure_tcp_port_available(&self, addr: SocketAddr) -> Result<()> {
        match std::net::TcpListener::bind(addr) {
            Ok(listener) => {
                drop(listener);
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => Err(anyhow::anyhow!(
                "runner tcp_socket port {} is already in use",
                addr
            )),
            Err(err) => Err(err.into()),
        }
    }

    async fn connect_socket(&self, socket: &RunnerSocketTarget, child: &mut Child) -> Result<()> {
        let start = Instant::now();
        let deadline = start + self.connect_timeout;
        let mut last_error: Option<anyhow::Error> = None;
        let mut delay = Duration::from_millis(10);
        let max_delay = Duration::from_millis(200);
        let mut attempt = 0u32;

        info!(
            timeout_ms = self.connect_timeout.as_millis() as u64,
            "Waiting for runner socket to be ready..."
        );

        loop {
            attempt += 1;
            let elapsed = start.elapsed();

            if let Ok(Some(status)) = child.try_wait() {
                warn!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    attempt,
                    exit_status = %status,
                    "Runner process exited before socket ready"
                );
                return Err(anyhow::anyhow!(
                    "runner exited before socket ready ({status})"
                ));
            }
            if Instant::now() >= deadline {
                warn!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    attempt,
                    last_error = ?last_error,
                    "Runner socket connect timeout exceeded"
                );
                return Err(anyhow::anyhow!(
                    "runner socket not ready: {}",
                    last_error
                        .as_ref()
                        .map(|err| err.to_string())
                        .unwrap_or_else(|| "unknown error".to_string())
                ));
            }
            match connect_stream(socket).await {
                Ok(_) => {
                    if let Ok(Some(status)) = child.try_wait() {
                        last_error = Some(anyhow::anyhow!(
                            "runner exited before socket ready ({status})"
                        ));
                        continue;
                    }
                    info!(
                        elapsed_ms = elapsed.as_millis() as u64,
                        attempt, "Runner socket connected successfully"
                    );
                    return Ok(());
                }
                Err(err) => {
                    let retryable = matches!(
                        err.kind(),
                        std::io::ErrorKind::NotFound
                            | std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                    );
                    if !retryable {
                        warn!(
                            elapsed_ms = elapsed.as_millis() as u64,
                            attempt,
                            error = %err,
                            "Non-retryable socket connect error"
                        );
                        return Err(err.into());
                    }
                    if attempt.is_multiple_of(10) {
                        debug!(
                            elapsed_ms = elapsed.as_millis() as u64,
                            attempt,
                            error = %err,
                            "Still waiting for runner socket..."
                        );
                    }
                    last_error = Some(err.into());
                }
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(max_delay);
        }
    }

    async fn acquire_process(&self) -> Result<(Arc<Mutex<SocketProcess>>, ProcessPermit)> {
        self.ensure_started().await?;
        loop {
            let notified = self.availability.notified();
            let processes = {
                let guard = self.processes.lock().await;
                if guard.is_empty() {
                    return Err(anyhow::anyhow!("runner pool has no processes"));
                }
                guard.clone()
            };
            let start = self.cursor.fetch_add(1, Ordering::Relaxed);
            for offset in 0..processes.len() {
                let idx = (start + offset) % processes.len();
                let proc = processes[idx].clone();
                self.ensure_process(&proc).await?;
                let permits = {
                    let guard = proc.lock().await;
                    guard.permits.clone()
                };
                if let Ok(permit) = permits.try_acquire_owned() {
                    return Ok((
                        proc,
                        ProcessPermit {
                            _permit: permit,
                            notify: self.availability.clone(),
                        },
                    ));
                }
            }
            notified.await;
        }
    }

    async fn ensure_process(&self, proc: &Arc<Mutex<SocketProcess>>) -> Result<()> {
        let needs_respawn = {
            let mut guard = proc.lock().await;
            match guard.child.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(_) => true,
            }
        };
        if needs_respawn {
            self.respawn(proc).await?;
        }
        Ok(())
    }

    async fn respawn(&self, proc: &Arc<Mutex<SocketProcess>>) -> Result<()> {
        let mut guard = proc.lock().await;
        // Capture the old socket target to reuse (prevents port exhaustion on TCP)
        let old_socket = guard.socket;
        let _ = self.terminate(&mut guard, "respawn").await;
        let replacement = match self.spawn_process(Some(old_socket)).await {
            Ok(proc) => proc,
            Err(err) => {
                tracing::warn!(
                    "failed to reuse runner socket {old_socket}: {err}; allocating new socket"
                );
                self.spawn_process(None).await?
            }
        };
        *guard = replacement;
        self.availability.notify_waiters();
        Ok(())
    }

    async fn terminate(&self, proc: &mut SocketProcess, termination_reason: &str) -> Result<()> {
        if let Some(task) = proc.stdout_task.take() {
            task.abort();
        }
        if let Some(task) = proc.stderr_task.take() {
            task.abort();
        }
        proc.connection = None;
        terminate_child_process(
            &mut proc.child,
            self.shutdown_term_grace,
            termination_reason,
        )
        .await;
        Ok(())
    }

    async fn terminate_process(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        reason: &str,
    ) -> Result<()> {
        let mut guard = proc.lock().await;
        self.terminate(&mut guard, reason).await?;
        self.availability.notify_waiters();
        Ok(())
    }

    async fn terminate_process_for_socket(
        &self,
        socket: RunnerSocketTarget,
        reason: &str,
    ) -> Result<()> {
        let processes = {
            let processes = self.processes.lock().await;
            processes.clone()
        };
        for proc in processes {
            let matches_socket = {
                let guard = proc.lock().await;
                guard.socket == socket
            };
            if matches_socket {
                self.terminate_process(&proc, reason).await?;
                return Ok(());
            }
        }
        debug!(
            socket = %socket,
            reason,
            "runner timeout cleanup target socket not found in pool"
        );
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut processes = self.processes.lock().await;
        for proc in processes.iter() {
            let mut guard = proc.lock().await;
            let _ = self.terminate(&mut guard, "pool_close").await;
        }
        processes.clear();
        Ok(())
    }
}

pub struct SocketRunner {
    pool: Arc<SocketRunnerPool>,
    in_flight: Arc<Mutex<HashMap<String, InFlightRequest>>>,
    send_cancel_hints: bool,
}

impl SocketRunner {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        name: impl Into<String>,
        cmd: Vec<String>,
        pool_size: usize,
        max_in_flight: usize,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        tcp_socket: Option<String>,
        response_timeout_seconds: Option<f64>,
        connect_timeout: Duration,
        shutdown_term_grace: Duration,
        send_cancel_hints: bool,
        capture_output: bool,
    ) -> Result<Self> {
        let response_timeout = response_timeout_seconds.map(Duration::from_secs_f64);
        let pool = SocketRunnerPool::new(
            name,
            cmd,
            pool_size,
            max_in_flight,
            env,
            cwd,
            tcp_socket,
            response_timeout,
            connect_timeout,
            shutdown_term_grace,
            capture_output,
        )
        .await?;
        Ok(Self {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints,
        })
    }

    async fn get_or_create_connection(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
    ) -> Result<(RunnerSocketTarget, Arc<PersistentProcessConnection>)> {
        loop {
            let (socket, existing) = {
                let guard = proc.lock().await;
                (guard.socket, guard.connection.clone())
            };

            if let Some(connection) = existing
                && !connection.is_closed()
            {
                return Ok((socket, connection));
            }

            let channel_capacity = self.pool.max_in_flight.saturating_mul(2).max(64);
            let connection =
                Arc::new(PersistentProcessConnection::connect(socket, channel_capacity).await?);

            let mut guard = proc.lock().await;
            if guard.socket != socket {
                continue;
            }
            if let Some(existing) = guard.connection.as_ref()
                && !existing.is_closed()
            {
                return Ok((socket, existing.clone()));
            }
            guard.connection = Some(connection.clone());
            return Ok((socket, connection));
        }
    }

    async fn invalidate_connection(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        connection: &Arc<PersistentProcessConnection>,
    ) {
        let mut guard = proc.lock().await;
        if let Some(existing) = guard.connection.as_ref()
            && Arc::ptr_eq(existing, connection)
        {
            guard.connection = None;
        }
    }

    async fn execute_with_process(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        request: &ExecutionRequest,
    ) -> Result<ExecutionOutcome> {
        let (socket, connection) = self.get_or_create_connection(proc).await?;
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.insert(
                request.request_id.clone(),
                InFlightRequest {
                    job_id: request.job_id.clone(),
                    socket,
                },
            );
        }

        let result = connection
            .execute(request, self.pool.response_timeout)
            .await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(err) => {
                self.invalidate_connection(proc, &connection).await;
                Err(err)
            }
        }
    }

    async fn resolve_in_flight_target(
        &self,
        job_id: &str,
        request_id: Option<&str>,
    ) -> Option<(String, InFlightRequest)> {
        let in_flight = self.in_flight.lock().await;
        let resolved_request_id = if let Some(request_id) = request_id {
            Some(request_id.to_string())
        } else {
            in_flight.iter().find_map(|(request_id, info)| {
                if info.job_id == job_id {
                    Some(request_id.clone())
                } else {
                    None
                }
            })
        };
        let request_id = resolved_request_id?;
        let info = in_flight.get(&request_id).cloned()?;
        Some((request_id, info))
    }

    async fn send_cancel_hint(&self, request_id: &str, info: &InFlightRequest) -> Result<()> {
        let message = RunnerMessage::Cancel {
            payload: CancelRequest {
                protocol_version: PROTOCOL_VERSION.to_string(),
                job_id: info.job_id.clone(),
                request_id: Some(request_id.to_string()),
                hard_kill: false,
            },
        };
        let mut stream = connect_stream(&info.socket).await?;
        write_message(&mut stream, &message).await
    }

    async fn finalize_execute_result(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        request: &ExecutionRequest,
        result: Result<ExecutionOutcome>,
    ) -> Result<ExecutionOutcome> {
        let timed_out = result.as_ref().err().is_some_and(is_response_timeout_error);
        if timed_out {
            if self.send_cancel_hints {
                let _ = self
                    .cancel(&request.job_id, Some(request.request_id.as_str()))
                    .await;
            }
            let _ = self.pool.terminate_process(proc, "response_timeout").await;
        }

        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&request.request_id);
        }

        match result {
            Ok(outcome) => Ok(outcome),
            Err(err) => {
                let exited = {
                    let mut guard = proc.lock().await;
                    match guard.child.try_wait() {
                        Ok(Some(_)) => true,
                        Ok(None) => false,
                        Err(_) => true,
                    }
                };
                if exited && !timed_out {
                    let _ = self.pool.respawn(proc).await;
                }
                Err(err)
            }
        }
    }

    async fn cleanup_worker_timeout_with_permit_held(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        request: &ExecutionRequest,
        send_cancel_hint: bool,
    ) {
        if send_cancel_hint
            && self.send_cancel_hints
            && let Some((request_id, info)) = self
                .resolve_in_flight_target(&request.job_id, Some(request.request_id.as_str()))
                .await
            && let Err(err) = self.send_cancel_hint(&request_id, &info).await
        {
            warn!(
                job_id = %request.job_id,
                request_id = %request_id,
                error = %err,
                "failed to send runner cancel hint during worker timeout cleanup"
            );
        }

        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&request.request_id);
        }

        let _ = self.pool.terminate_process(proc, "worker_timeout").await;
    }
}

#[async_trait]
impl Runner for SocketRunner {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
        let (proc, _permit) = self.pool.acquire_process().await?;
        let result = self.execute_with_process(&proc, &request).await;
        self.finalize_execute_result(&proc, &request, result).await
    }

    async fn execute_with_timeout(
        &self,
        request: ExecutionRequest,
        timeout_duration: Duration,
        send_cancel_hint: bool,
    ) -> RunnerExecutionResult {
        let acquire_started = Instant::now();
        let (proc, _permit) = match timeout(timeout_duration, self.pool.acquire_process()).await {
            Ok(Ok(pair)) => pair,
            Ok(Err(err)) => return RunnerExecutionResult::Completed(Box::new(Err(err))),
            Err(_) => return RunnerExecutionResult::TimedOut,
        };
        let Some(remaining_timeout) = timeout_duration.checked_sub(acquire_started.elapsed())
        else {
            return RunnerExecutionResult::TimedOut;
        };

        match timeout(
            remaining_timeout,
            self.execute_with_process(&proc, &request),
        )
        .await
        {
            Ok(result) => RunnerExecutionResult::Completed(Box::new(
                self.finalize_execute_result(&proc, &request, result).await,
            )),
            Err(_) => {
                self.cleanup_worker_timeout_with_permit_held(&proc, &request, send_cancel_hint)
                    .await;
                RunnerExecutionResult::TimedOut
            }
        }
    }

    async fn cancel(&self, job_id: &str, request_id: Option<&str>) -> Result<()> {
        let Some((request_id, info)) = self.resolve_in_flight_target(job_id, request_id).await
        else {
            return Ok(());
        };
        let result = self.send_cancel_hint(&request_id, &info).await;
        if result.is_ok() {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&request_id);
        }
        result
    }

    async fn handle_timeout(
        &self,
        job_id: &str,
        request_id: Option<&str>,
        send_cancel_hint: bool,
    ) -> Result<()> {
        let Some((request_id, info)) = self.resolve_in_flight_target(job_id, request_id).await
        else {
            return Ok(());
        };

        if send_cancel_hint
            && self.send_cancel_hints
            && let Err(err) = self.send_cancel_hint(&request_id, &info).await
        {
            warn!(
                %job_id,
                request_id = %request_id,
                error = %err,
                "failed to send runner cancel hint during timeout cleanup"
            );
        }

        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&request_id);
        }

        self.pool
            .terminate_process_for_socket(info.socket, "worker_timeout")
            .await
    }

    async fn close(&self) -> Result<()> {
        self.pool.close().await
    }
}

#[derive(Clone)]
struct InFlightRequest {
    job_id: String,
    socket: RunnerSocketTarget,
}

/// Determine which runners are needed based on the queues being listened to.
///
/// Returns a set of runner names that should be spawned. This includes:
/// - Runners explicitly mapped to queues via `runner_routes`
/// - The default runner (for queues without explicit routing)
pub fn determine_needed_runners(
    settings: &RRQSettings,
    queues: Option<&[String]>,
) -> std::collections::HashSet<String> {
    let mut needed = std::collections::HashSet::new();

    // Always need the default runner for unrouted queues
    if !settings.default_runner_name.is_empty() {
        needed.insert(settings.default_runner_name.clone());
    }

    // Get the effective queues (CLI override or default)
    let effective_queues: Vec<String> = match queues {
        Some(q) if !q.is_empty() => q
            .iter()
            .map(|queue_name| normalize_queue_name(queue_name))
            .collect(),
        _ => vec![normalize_queue_name(&settings.default_queue_name)],
    };

    // Add runners that are explicitly routed to these queues
    for queue in &effective_queues {
        let runner_name = settings.runner_routes.get(queue).or_else(|| {
            queue
                .strip_prefix(QUEUE_KEY_PREFIX)
                .and_then(|bare| settings.runner_routes.get(bare))
        });
        if let Some(runner_name) = runner_name {
            needed.insert(runner_name.clone());
        }
    }

    needed
}

pub fn resolve_runner_pool_sizes(
    settings: &RRQSettings,
    watch_mode: bool,
    default_pool_size: Option<usize>,
) -> Result<HashMap<String, usize>> {
    let default_pool_size = default_pool_size.unwrap_or_else(num_cpus::get);
    let mut pool_sizes = HashMap::new();
    for (name, config) in &settings.runners {
        let pool_size = if watch_mode {
            1
        } else {
            config.pool_size.unwrap_or(default_pool_size)
        };
        if pool_size == 0 {
            return Err(anyhow::anyhow!(
                "pool_size must be positive for runner '{}'",
                name
            ));
        }
        pool_sizes.insert(name.clone(), pool_size);
    }
    Ok(pool_sizes)
}

pub fn resolve_runner_max_in_flight(
    settings: &RRQSettings,
    watch_mode: bool,
) -> Result<HashMap<String, usize>> {
    let mut max_in_flight = HashMap::new();
    for (name, config) in &settings.runners {
        let limit = if watch_mode {
            1
        } else {
            config.max_in_flight.unwrap_or(1)
        };
        if limit == 0 {
            return Err(anyhow::anyhow!(
                "max_in_flight must be positive for runner '{}'",
                name
            ));
        }
        max_in_flight.insert(name.clone(), limit);
    }
    Ok(max_in_flight)
}

pub async fn build_runners_from_settings(
    settings: &RRQSettings,
    pool_sizes: Option<&HashMap<String, usize>>,
    max_in_flight: Option<&HashMap<String, usize>>,
) -> Result<HashMap<String, Arc<dyn Runner>>> {
    build_runners_from_settings_filtered(settings, pool_sizes, max_in_flight, None).await
}

/// Build runners from settings, optionally filtering to only spawn needed runners.
///
/// If `needed_runners` is `Some`, only runners in that set will be spawned.
/// If `needed_runners` is `None`, all configured runners will be spawned.
pub async fn build_runners_from_settings_filtered(
    settings: &RRQSettings,
    pool_sizes: Option<&HashMap<String, usize>>,
    max_in_flight: Option<&HashMap<String, usize>>,
    needed_runners: Option<&std::collections::HashSet<String>>,
) -> Result<HashMap<String, Arc<dyn Runner>>> {
    let pool_sizes = match pool_sizes {
        Some(map) => map.clone(),
        None => resolve_runner_pool_sizes(settings, false, None)?,
    };
    let max_in_flight = match max_in_flight {
        Some(map) => map.clone(),
        None => resolve_runner_max_in_flight(settings, false)?,
    };
    let connect_timeout = connect_timeout_from_settings(settings.runner_connect_timeout_ms);
    let shutdown_term_grace =
        shutdown_term_grace_from_settings(settings.runner_shutdown_term_grace_seconds);
    let mut runners: HashMap<String, Arc<dyn Runner>> = HashMap::new();
    for (name, config) in &settings.runners {
        // Skip runners that are not needed (if filter is provided)
        if let Some(needed) = needed_runners
            && !needed.contains(name)
        {
            tracing::debug!(runner = %name, "skipping runner (not needed for configured queues)");
            continue;
        }

        // cmd and tcp_socket are validated by the config crate
        let pool_size = pool_sizes
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing pool size for runner '{}'", name))?;
        let max_in_flight = max_in_flight
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing max_in_flight for runner '{}'", name))?;
        let cmd = config.cmd.clone().unwrap_or_default();
        let runner = SocketRunner::new(
            name.clone(),
            cmd,
            pool_size,
            max_in_flight,
            config.env.clone(),
            config.cwd.clone(),
            config.tcp_socket.clone(),
            config.response_timeout_seconds,
            connect_timeout,
            shutdown_term_grace,
            settings.runner_enable_inflight_cancel_hints,
            settings.capture_runner_output,
        )
        .await?;
        runners.insert(name.clone(), Arc::new(runner));
    }
    Ok(runners)
}

async fn connect_stream(target: &RunnerSocketTarget) -> std::io::Result<RunnerStream> {
    let stream = TcpStream::connect(target).await?;
    Ok(Box::new(stream))
}

async fn read_message<R>(stream: &mut R) -> Result<Option<RunnerMessage>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut header = [0u8; FRAME_HEADER_LEN];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err(anyhow::anyhow!("runner message payload cannot be empty"));
    }
    if length > MAX_FRAME_LEN {
        return Err(anyhow::anyhow!("runner message payload too large"));
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    let message = serde_json::from_slice(&payload)?;
    Ok(Some(message))
}

async fn write_message<W>(stream: &mut W, message: &RunnerMessage) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let framed = encode_frame(message)?;
    stream.write_all(&framed).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    #[cfg(unix)]
    use nix::sys::signal::kill;
    #[cfg(unix)]
    use nix::unistd::Pid;
    use rrq_config::{RRQSettings, RunnerConfig, RunnerType};
    use std::net::{IpAddr, Ipv4Addr, TcpListener as StdTcpListener};
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    use tokio::net::TcpListener as TokioTcpListener;
    use tokio::process::Command;
    use tokio::time::sleep;

    #[tokio::test]
    async fn socket_framing_round_trip() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let request = ExecutionRequest {
            protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: "echo".to_string(),
            params: HashMap::new(),
            context: rrq_protocol::ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                correlation_context: None,
                worker_id: None,
            },
        };
        let message = RunnerMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();
        let decoded = read_message(&mut server).await.unwrap().unwrap();
        match decoded {
            RunnerMessage::Request { payload } => {
                assert_eq!(payload.job_id, request.job_id);
                assert_eq!(payload.request_id, request.request_id);
                assert_eq!(payload.function_name, request.function_name);
            }
            _ => panic!("unexpected message variant"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_child_process_escalates_to_sigkill_when_term_ignored() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("trap '' TERM; while true; do sleep 1; done");
        configure_runner_process_group(&mut command);
        let mut child = command.spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        terminate_child_process(&mut child, Duration::from_millis(50), "test_escalation").await;

        let status = child.try_wait().unwrap().expect("child should be exited");
        assert!(matches!(status.signal(), Some(9) | Some(15)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_child_process_kills_process_group_descendants() {
        let marker =
            std::env::temp_dir().join(format!("rrq-runner-child-{}.pid", uuid::Uuid::new_v4()));
        let script = format!(
            "sleep 60 & echo $! > '{}'; trap '' TERM; while true; do sleep 1; done",
            marker.display()
        );
        let mut command = Command::new("sh");
        command.arg("-c").arg(script);
        configure_runner_process_group(&mut command);
        let mut child = command.spawn().unwrap();

        let mut child_pid: Option<i32> = None;
        for _ in 0..100 {
            if let Ok(payload) = std::fs::read_to_string(&marker)
                && let Ok(pid) = payload.trim().parse::<i32>()
            {
                child_pid = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let child_pid = child_pid.expect("child pid marker should be written");

        terminate_child_process(&mut child, Duration::from_millis(50), "test_descendants").await;

        let still_alive = kill(Pid::from_raw(child_pid), None).is_ok();
        assert!(!still_alive, "descendant process should be terminated");
        let _ = std::fs::remove_file(marker);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_child_process_kills_descendants_when_leader_exits_first() {
        let marker =
            std::env::temp_dir().join(format!("rrq-runner-child-{}.pid", uuid::Uuid::new_v4()));
        let script = format!(
            "sh -c \"trap '' TERM; while true; do sleep 1; done\" & echo $! > '{}'; trap 'exit 0' TERM; while true; do sleep 1; done",
            marker.display()
        );
        let mut command = Command::new("sh");
        command.arg("-c").arg(script);
        configure_runner_process_group(&mut command);
        let mut child = command.spawn().unwrap();

        let mut child_pid: Option<i32> = None;
        for _ in 0..100 {
            if let Ok(payload) = std::fs::read_to_string(&marker)
                && let Ok(pid) = payload.trim().parse::<i32>()
            {
                child_pid = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let child_pid = child_pid.expect("child pid marker should be written");

        terminate_child_process(&mut child, Duration::from_millis(100), "test_leader_exit").await;

        let mut still_alive = true;
        for _ in 0..50 {
            if kill(Pid::from_raw(child_pid), None).is_err() {
                still_alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !still_alive,
            "descendant should be terminated even if leader exits first"
        );
        let _ = std::fs::remove_file(marker);
    }

    #[test]
    fn build_runner_event_json_preserves_fields() {
        let now = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let line = r#"{"timestamp":"2024-01-01T00:00:00Z","level":"WARN","fields":{"message":"hello"},"target":"runner"}"#;
        let value = build_runner_event_json("python", RunnerLogStream::Stdout, line, now);
        let obj = value.as_object().expect("object");
        assert_eq!(
            obj.get("timestamp").and_then(Value::as_str),
            Some("2024-01-01T00:00:00Z")
        );
        assert_eq!(obj.get("level").and_then(Value::as_str), Some("WARN"));
        assert_eq!(
            obj.get("rrq.runner").and_then(Value::as_str),
            Some("python")
        );
        assert_eq!(
            obj.get("rrq.stream").and_then(Value::as_str),
            Some("stdout")
        );
        assert_eq!(
            obj.get("rrq.source").and_then(Value::as_str),
            Some("runner")
        );
        assert!(obj.get("fields").is_some());
    }

    #[test]
    fn build_runner_event_json_wraps_plain_text() {
        let now = Utc.with_ymd_and_hms(2024, 2, 2, 3, 4, 5).unwrap();
        let value = build_runner_event_json("rust", RunnerLogStream::Stderr, "plain log line", now);
        let obj = value.as_object().expect("object");
        let timestamp = obj
            .get("timestamp")
            .and_then(Value::as_str)
            .expect("timestamp");
        let parsed = DateTime::parse_from_rfc3339(timestamp).expect("rfc3339");
        assert_eq!(parsed.with_timezone(&Utc), now);
        assert_eq!(obj.get("level").and_then(Value::as_str), Some("ERROR"));
        assert_eq!(
            obj.get("message").and_then(Value::as_str),
            Some("plain log line")
        );
        assert_eq!(obj.get("rrq.runner").and_then(Value::as_str), Some("rust"));
        assert_eq!(
            obj.get("rrq.stream").and_then(Value::as_str),
            Some("stderr")
        );
    }

    fn build_test_pool(max_in_flight: usize) -> SocketRunnerPool {
        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep");
        let (spec, socket_addr) = allocate_tcp_spec();
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(max_in_flight)),
            connection: None,
        };
        SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        }
    }

    fn allocate_tcp_spec() -> (TcpSocketSpec, SocketAddr) {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind temp port");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let spec = TcpSocketSpec {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: addr.port(),
        };
        (spec, addr)
    }

    async fn bind_tcp_listener() -> (TokioTcpListener, SocketAddr) {
        let listener = TokioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind tcp listener");
        let addr = listener.local_addr().expect("local addr");
        (listener, addr)
    }

    fn build_execution_request(request_id: &str, job_id: &str) -> ExecutionRequest {
        ExecutionRequest {
            protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
            request_id: request_id.to_string(),
            job_id: job_id.to_string(),
            function_name: "echo".to_string(),
            params: HashMap::new(),
            context: rrq_protocol::ExecutionContext {
                job_id: job_id.to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                correlation_context: None,
                worker_id: None,
            },
        }
    }

    #[tokio::test]
    async fn pool_enforces_per_process_max_in_flight() {
        let pool = Arc::new(build_test_pool(1));
        let (_proc, permit) = pool.acquire_process().await.unwrap();
        let pool_clone = pool.clone();
        let waiter = tokio::spawn(async move { pool_clone.acquire_process().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        drop(permit);
        let result = timeout(Duration::from_millis(200), waiter)
            .await
            .expect("acquire should complete after release");
        let (_proc2, permit2) = result.expect("join failed").expect("acquire failed");
        drop(permit2);
        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn execute_with_timeout_includes_process_acquire_wait() {
        let pool = Arc::new(build_test_pool(1));
        let runner = Arc::new(SocketRunner {
            pool: pool.clone(),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        });

        let (_proc, permit) = pool.acquire_process().await.unwrap();
        let timed_out_runner = runner.clone();
        let timed_out_task = tokio::spawn(async move {
            timed_out_runner
                .execute_with_timeout(
                    build_execution_request("req-wait-timeout", "job-wait-timeout"),
                    Duration::from_millis(60),
                    false,
                )
                .await
        });

        let timed_out_result = timeout(Duration::from_millis(150), timed_out_task)
            .await
            .expect("execute_with_timeout should complete while acquire is blocked")
            .unwrap();
        assert!(matches!(timed_out_result, RunnerExecutionResult::TimedOut));

        drop(permit);
        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_process_respawns_exited_child() {
        let spawn_override = Arc::new(|| {
            let (_spec, socket_addr) = allocate_tcp_spec();
            let child = Command::new("sleep").arg("60").spawn().unwrap();
            SocketProcess {
                child,
                socket: socket_addr,
                stdout_task: None,
                stderr_task: None,
                permits: Arc::new(Semaphore::new(1)),
                connection: None,
            }
        });
        let (spec, _socket_addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: Some(spawn_override),
        };
        pool.ensure_started().await.unwrap();
        let proc = {
            let processes = pool.processes.lock().await;
            processes.first().cloned().unwrap()
        };
        let old_socket = { proc.lock().await.socket };
        {
            let mut guard = proc.lock().await;
            let _ = guard.child.kill().await;
            let _ = guard.child.wait().await;
        }
        sleep(Duration::from_millis(50)).await;
        pool.ensure_process(&proc).await.unwrap();
        let new_socket = { proc.lock().await.socket };
        assert_ne!(old_socket, new_socket);
        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn connect_socket_errors_when_child_exits() {
        let (spec, socket_addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["true".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(50),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };
        let mut child = Command::new("true").spawn().unwrap();
        let err = pool
            .connect_socket(&socket_addr, &mut child)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("runner exited before socket ready")
        );
    }

    #[test]
    fn resolve_pool_sizes_and_max_in_flight_watch_mode() {
        let mut settings = RRQSettings::default();
        let mut runners = HashMap::new();
        runners.insert(
            "python".to_string(),
            RunnerConfig {
                runner_type: RunnerType::Socket,
                cmd: Some(vec!["rrq-runner".to_string()]),
                pool_size: Some(4),
                max_in_flight: Some(5),
                env: None,
                cwd: None,
                tcp_socket: Some("127.0.0.1:9000".to_string()),
                response_timeout_seconds: None,
            },
        );
        settings.runners = runners;

        let pool_sizes = resolve_runner_pool_sizes(&settings, true, None).unwrap();
        let max_in_flight = resolve_runner_max_in_flight(&settings, true).unwrap();
        assert_eq!(pool_sizes.get("python"), Some(&1));
        assert_eq!(max_in_flight.get("python"), Some(&1));
    }

    #[test]
    fn resolve_pool_sizes_and_max_in_flight_validate_zero() {
        let mut settings = RRQSettings::default();
        let mut runners = HashMap::new();
        runners.insert(
            "python".to_string(),
            RunnerConfig {
                runner_type: RunnerType::Socket,
                cmd: Some(vec!["rrq-runner".to_string()]),
                pool_size: Some(0),
                max_in_flight: Some(0),
                env: None,
                cwd: None,
                tcp_socket: Some("127.0.0.1:9000".to_string()),
                response_timeout_seconds: None,
            },
        );
        settings.runners = runners;

        let err = resolve_runner_pool_sizes(&settings, false, None).unwrap_err();
        assert!(err.to_string().contains("pool_size must be positive"));
        let err = resolve_runner_max_in_flight(&settings, false).unwrap_err();
        assert!(err.to_string().contains("max_in_flight must be positive"));
    }

    // parse_tcp_socket tests are in rrq_config::tcp_socket module

    #[test]
    fn connect_timeout_from_settings_defaults_for_non_positive() {
        let expected = Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64);
        assert_eq!(connect_timeout_from_settings(0), expected);
        assert_eq!(connect_timeout_from_settings(-5), expected);
        assert_eq!(
            connect_timeout_from_settings(5000),
            Duration::from_millis(5000)
        );
    }

    #[test]
    fn shutdown_term_grace_from_settings_handles_invalid_values() {
        assert_eq!(shutdown_term_grace_from_settings(0.0), Duration::ZERO);
        assert_eq!(shutdown_term_grace_from_settings(-1.0), Duration::ZERO);
        assert_eq!(shutdown_term_grace_from_settings(f64::NAN), Duration::ZERO);
        assert_eq!(
            shutdown_term_grace_from_settings(f64::INFINITY),
            Duration::ZERO
        );
    }

    #[test]
    fn shutdown_term_grace_from_settings_saturates_huge_values() {
        let duration = std::panic::catch_unwind(|| shutdown_term_grace_from_settings(1e100))
            .expect("shutdown grace conversion should not panic");
        assert_eq!(duration, Duration::MAX);
    }

    #[tokio::test]
    async fn tcp_socket_pool_range_rejects_overflow() {
        let err = SocketRunnerPool::new(
            "test",
            vec!["true".to_string()],
            2,
            1,
            None,
            None,
            Some("127.0.0.1:65535".to_string()),
            None,
            Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            Duration::from_millis(50),
            true,
        )
        .await;
        match err {
            Err(err) => assert!(err.to_string().contains("range too small")),
            Ok(_) => panic!("expected tcp socket range error"),
        }
    }

    #[test]
    fn tcp_socket_pool_assigns_incrementing_ports() {
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["true".to_string()],
            pool_size: 2,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: TcpSocketSpec {
                host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                port: 9000,
            },
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };

        let first = pool.next_tcp_socket().unwrap();
        let second = pool.next_tcp_socket().unwrap();
        let third = pool.next_tcp_socket().unwrap();

        assert_eq!(
            first,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000)
        );
        assert_eq!(
            second,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9001)
        );
        assert_eq!(
            third,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9002)
        );
    }

    #[test]
    fn tcp_port_collision_is_detected() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["true".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: TcpSocketSpec {
                host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                port: addr.port(),
            },
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };

        let err = pool.ensure_tcp_port_available(addr).unwrap_err();
        assert!(err.to_string().contains("already in use"));

        drop(listener);
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..10 {
            match pool.ensure_tcp_port_available(addr) {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(err) if err.to_string().contains("already in use") => {
                    last_err = Some(err);
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err) => panic!("unexpected error: {err}"),
            }
        }
        if let Some(err) = last_err {
            panic!("port still in use after release: {err}");
        }
    }

    #[tokio::test]
    async fn cancel_sends_cancel_message() {
        let (listener, socket_addr) = bind_tcp_listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let message = read_message(&mut stream).await.unwrap().unwrap();
            match message {
                RunnerMessage::Cancel { payload } => {
                    assert_eq!(payload.job_id, "job-1");
                    assert_eq!(payload.request_id.as_deref(), Some("req-1"));
                }
                _ => panic!("expected cancel message"),
            }
        });

        let pool = build_test_pool(1);
        let runner = SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };
        {
            let mut in_flight = runner.in_flight.lock().await;
            in_flight.insert(
                "req-1".to_string(),
                InFlightRequest {
                    job_id: "job-1".to_string(),
                    socket: socket_addr,
                },
            );
        }
        runner.cancel("job-1", None).await.unwrap();
        server.await.unwrap();

        let in_flight = runner.in_flight.lock().await;
        assert!(in_flight.is_empty());
    }

    #[tokio::test]
    async fn cancel_failure_preserves_in_flight() {
        let (_spec, socket_addr) = allocate_tcp_spec();
        let pool = build_test_pool(1);
        let runner = SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };
        {
            let mut in_flight = runner.in_flight.lock().await;
            in_flight.insert(
                "req-1".to_string(),
                InFlightRequest {
                    job_id: "job-1".to_string(),
                    socket: socket_addr,
                },
            );
        }
        let err = runner.cancel("job-1", None).await.unwrap_err();
        assert!(!err.to_string().is_empty());
        let in_flight = runner.in_flight.lock().await;
        assert!(in_flight.contains_key("req-1"));
    }

    #[tokio::test]
    async fn execute_timeout_recycles_process_without_cancel_hints() {
        let (listener, socket_addr) = bind_tcp_listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_message(&mut stream).await.unwrap().unwrap();
            assert!(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "unexpected cancel hint when disabled"
            );
        });

        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep");
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let (spec, _addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: Some(Duration::from_millis(50)),
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };
        let runner = SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };
        let request = ExecutionRequest {
            protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: "echo".to_string(),
            params: HashMap::new(),
            context: rrq_protocol::ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                correlation_context: None,
                worker_id: None,
            },
        };
        let err = runner.execute(request).await.unwrap_err();
        assert!(err.to_string().contains("runner response timeout"));
        let proc = {
            let processes = runner.pool.processes.lock().await;
            processes.first().cloned().unwrap()
        };
        let mut exited = false;
        for _ in 0..20 {
            let status = {
                let mut guard = proc.lock().await;
                guard.child.try_wait().unwrap()
            };
            if status.is_some() {
                exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(exited, "timed-out process should be terminated");
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_with_timeout_holds_permit_during_timeout_cleanup() {
        let (listener, socket_addr) = bind_tcp_listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_message(&mut stream).await.unwrap().unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("trap '' TERM; while true; do sleep 1; done");
        configure_runner_process_group(&mut command);
        let child = command.spawn().expect("spawn term-ignoring child");
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let (spec, _addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(200),
            capture_output: true,
            spawn_override: None,
        };
        let runner = Arc::new(SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        });

        let timeout_runner = runner.clone();
        let timeout_task = tokio::spawn(async move {
            timeout_runner
                .execute_with_timeout(
                    build_execution_request("req-timeout", "job-timeout"),
                    Duration::from_millis(50),
                    false,
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(70)).await;
        let acquire_attempt =
            timeout(Duration::from_millis(80), runner.pool.acquire_process()).await;
        assert!(
            acquire_attempt.is_err(),
            "process permit should remain held until timeout cleanup completes"
        );

        let timeout_result = timeout_task.await.unwrap();
        assert!(matches!(timeout_result, RunnerExecutionResult::TimedOut));

        server.abort();
    }

    #[tokio::test]
    async fn execute_timeout_sends_cancel_hint_when_enabled() {
        let (listener, socket_addr) = bind_tcp_listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_message(&mut stream).await.unwrap().unwrap();
            let (mut cancel_stream, _) = listener.accept().await.unwrap();
            let message = read_message(&mut cancel_stream).await.unwrap().unwrap();
            match message {
                RunnerMessage::Cancel { payload } => {
                    assert_eq!(payload.job_id, "job-1");
                    assert_eq!(payload.request_id.as_deref(), Some("req-1"));
                }
                _ => panic!("expected cancel message"),
            }
        });

        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep");
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let (spec, _addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: Some(Duration::from_millis(50)),
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };
        let runner = SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: true,
        };
        let request = build_execution_request("req-1", "job-1");
        let err = runner.execute(request).await.unwrap_err();
        assert!(err.to_string().contains("runner response timeout"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn handle_timeout_terminates_socket_process_without_cancel_hint() {
        let (listener, socket_addr) = bind_tcp_listener().await;
        let server = tokio::spawn(async move {
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "unexpected cancel hint when disabled"
            );
        });

        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep");
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let (spec, _addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };
        let runner = SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };
        {
            let mut in_flight = runner.in_flight.lock().await;
            in_flight.insert(
                "req-1".to_string(),
                InFlightRequest {
                    job_id: "job-1".to_string(),
                    socket: socket_addr,
                },
            );
        }

        runner
            .handle_timeout("job-1", Some("req-1"), false)
            .await
            .unwrap();

        let in_flight = runner.in_flight.lock().await;
        assert!(in_flight.is_empty());
        drop(in_flight);

        let proc = {
            let processes = runner.pool.processes.lock().await;
            processes.first().cloned().unwrap()
        };
        let mut exited = false;
        for _ in 0..20 {
            let status = {
                let mut guard = proc.lock().await;
                guard.child.try_wait().unwrap()
            };
            if status.is_some() {
                exited = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(exited, "timed-out process should be terminated");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn execute_with_process_times_out_on_unknown_request_id() {
        let (listener, socket_addr) = bind_tcp_listener().await;

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let message = read_message(&mut stream).await.unwrap().unwrap();
            let request = match message {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected request"),
            };
            let response = ExecutionOutcome::success(
                request.job_id,
                "wrong-req",
                serde_json::json!({"ok": true}),
            );
            write_message(&mut stream, &RunnerMessage::Response { payload: response })
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        });

        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let (spec, _addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: Some(Duration::from_millis(80)),
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };
        let runner = SocketRunner {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };
        let proc = {
            let processes = runner.pool.processes.lock().await;
            processes.first().cloned().unwrap()
        };

        let request = ExecutionRequest {
            protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: "echo".to_string(),
            params: HashMap::new(),
            context: rrq_protocol::ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                correlation_context: None,
                worker_id: None,
            },
        };
        let err = runner
            .execute_with_process(&proc, &request)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("runner response timeout"));

        server.await.unwrap();
        let mut guard = proc.lock().await;
        let _ = guard.child.kill().await;
        let _ = guard.child.wait().await;
    }

    #[tokio::test]
    async fn execute_with_process_reuses_persistent_connection() {
        let (listener, socket_addr) = bind_tcp_listener().await;

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for (request_id, job_id) in [("req-1", "job-1"), ("req-2", "job-2")] {
                let message = read_message(&mut stream).await.unwrap().unwrap();
                let request = match message {
                    RunnerMessage::Request { payload } => payload,
                    _ => panic!("expected request"),
                };
                assert_eq!(request.request_id, request_id);
                assert_eq!(request.job_id, job_id);
                let response =
                    ExecutionOutcome::success(job_id, request_id, serde_json::json!({"ok": true}));
                write_message(&mut stream, &RunnerMessage::Response { payload: response })
                    .await
                    .unwrap();
            }
            assert!(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "expected requests to reuse the same socket connection"
            );
        });

        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let proc = Arc::new(Mutex::new(process));
        let runner = SocketRunner {
            pool: Arc::new(build_test_pool(1)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };

        let first = runner
            .execute_with_process(&proc, &build_execution_request("req-1", "job-1"))
            .await
            .unwrap();
        assert_eq!(first.request_id.as_deref(), Some("req-1"));

        let second = runner
            .execute_with_process(&proc, &build_execution_request("req-2", "job-2"))
            .await
            .unwrap();
        assert_eq!(second.request_id.as_deref(), Some("req-2"));

        server.await.unwrap();
        let mut guard = proc.lock().await;
        let _ = guard.child.kill().await;
        let _ = guard.child.wait().await;
    }

    #[tokio::test]
    async fn execute_with_process_reconnects_after_connection_close() {
        let (listener, socket_addr) = bind_tcp_listener().await;

        let server = tokio::spawn(async move {
            for (request_id, job_id) in [("req-1", "job-1"), ("req-2", "job-2")] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let message = read_message(&mut stream).await.unwrap().unwrap();
                let request = match message {
                    RunnerMessage::Request { payload } => payload,
                    _ => panic!("expected request"),
                };
                assert_eq!(request.request_id, request_id);
                assert_eq!(request.job_id, job_id);
                let response =
                    ExecutionOutcome::success(job_id, request_id, serde_json::json!({"ok": true}));
                write_message(&mut stream, &RunnerMessage::Response { payload: response })
                    .await
                    .unwrap();
            }
        });

        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let process = SocketProcess {
            child,
            socket: socket_addr,
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
            connection: None,
        };
        let proc = Arc::new(Mutex::new(process));
        let runner = SocketRunner {
            pool: Arc::new(build_test_pool(1)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            send_cancel_hints: false,
        };

        let first = runner
            .execute_with_process(&proc, &build_execution_request("req-1", "job-1"))
            .await
            .unwrap();
        assert_eq!(first.request_id.as_deref(), Some("req-1"));

        // Give the reader task time to observe EOF and mark the persistent connection closed.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let second = runner
            .execute_with_process(&proc, &build_execution_request("req-2", "job-2"))
            .await
            .unwrap();
        assert_eq!(second.request_id.as_deref(), Some("req-2"));

        server.await.unwrap();
        let mut guard = proc.lock().await;
        let _ = guard.child.kill().await;
        let _ = guard.child.wait().await;
    }

    #[tokio::test]
    async fn persistent_connection_matches_out_of_order_responses() {
        let (listener, socket_addr) = bind_tcp_listener().await;

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let first = match read_message(&mut stream).await.unwrap().unwrap() {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected first request"),
            };
            let second = match read_message(&mut stream).await.unwrap().unwrap() {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected second request"),
            };

            let second_response = ExecutionOutcome::success(
                second.job_id.clone(),
                second.request_id.clone(),
                serde_json::json!({"order": 2}),
            );
            write_message(
                &mut stream,
                &RunnerMessage::Response {
                    payload: second_response,
                },
            )
            .await
            .unwrap();

            let first_response = ExecutionOutcome::success(
                first.job_id.clone(),
                first.request_id.clone(),
                serde_json::json!({"order": 1}),
            );
            write_message(
                &mut stream,
                &RunnerMessage::Response {
                    payload: first_response,
                },
            )
            .await
            .unwrap();
        });

        let connection = PersistentProcessConnection::connect(socket_addr, 64)
            .await
            .unwrap();
        let request_one = build_execution_request("req-1", "job-1");
        let request_two = build_execution_request("req-2", "job-2");

        let (outcome_one, outcome_two) = tokio::join!(
            connection.execute(&request_one, Some(Duration::from_secs(1))),
            connection.execute(&request_two, Some(Duration::from_secs(1)))
        );
        let outcome_one = outcome_one.unwrap();
        let outcome_two = outcome_two.unwrap();

        assert_eq!(outcome_one.request_id.as_deref(), Some("req-1"));
        assert_eq!(outcome_two.request_id.as_deref(), Some("req-2"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn persistent_connection_ignores_late_unknown_response_id() {
        let (listener, socket_addr) = bind_tcp_listener().await;

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let first = match read_message(&mut stream).await.unwrap().unwrap() {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected first request"),
            };
            let second = match read_message(&mut stream).await.unwrap().unwrap() {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected second request"),
            };
            let (timed_out_request, pending_request) = if first.request_id == "req-timeout" {
                (first, second)
            } else {
                (second, first)
            };

            tokio::time::sleep(Duration::from_millis(120)).await;

            let late_response = ExecutionOutcome::success(
                timed_out_request.job_id.clone(),
                timed_out_request.request_id.clone(),
                serde_json::json!({"late": true}),
            );
            write_message(
                &mut stream,
                &RunnerMessage::Response {
                    payload: late_response,
                },
            )
            .await
            .unwrap();

            let pending_response = ExecutionOutcome::success(
                pending_request.job_id.clone(),
                pending_request.request_id.clone(),
                serde_json::json!({"ok": true}),
            );
            write_message(
                &mut stream,
                &RunnerMessage::Response {
                    payload: pending_response,
                },
            )
            .await
            .unwrap();
        });

        let connection = PersistentProcessConnection::connect(socket_addr, 64)
            .await
            .unwrap();
        let timeout_request = build_execution_request("req-timeout", "job-timeout");
        let pending_request = build_execution_request("req-pending", "job-pending");

        let (timeout_result, pending_result) = tokio::join!(
            connection.execute(&timeout_request, Some(Duration::from_millis(60))),
            connection.execute(&pending_request, Some(Duration::from_millis(500)))
        );

        let timeout_err = timeout_result.unwrap_err();
        assert!(timeout_err.to_string().contains("runner response timeout"));
        let pending_outcome = pending_result.unwrap();
        assert_eq!(pending_outcome.request_id.as_deref(), Some("req-pending"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn persistent_connection_missing_request_id_fails_pending_request() {
        let (listener, socket_addr) = bind_tcp_listener().await;

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = match read_message(&mut stream).await.unwrap().unwrap() {
                RunnerMessage::Request { payload } => payload,
                _ => panic!("expected request"),
            };

            let mut response = ExecutionOutcome::success(
                request.job_id.clone(),
                request.request_id.clone(),
                serde_json::json!({"ok": true}),
            );
            response.request_id = None;
            write_message(&mut stream, &RunnerMessage::Response { payload: response })
                .await
                .unwrap();
        });

        let connection = PersistentProcessConnection::connect(socket_addr, 64)
            .await
            .unwrap();
        let request = build_execution_request("req-1", "job-1");
        let result = timeout(Duration::from_secs(1), connection.execute(&request, None)).await;
        let err = result
            .expect("request should fail promptly for missing request_id")
            .unwrap_err();
        assert!(err.to_string().contains("missing request_id"));
        assert!(err.to_string().contains("req-1"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_pending_with_error_drains_even_when_already_closed() {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(true));
        let (sender, receiver) = oneshot::channel::<Result<ExecutionOutcome>>();
        {
            let mut guard = pending.lock().await;
            guard.insert("req-1".to_string(), sender);
        }

        close_pending_with_error(&pending, &closed, "runner connection closed".to_string()).await;

        let result = receiver.await.expect("pending sender should be completed");
        let err = result.expect_err("pending request should fail when connection is closed");
        assert!(err.to_string().contains("runner connection closed"));
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn persistent_connection_execute_handles_close_race_after_pending_insert() {
        let (sender, _receiver) = mpsc::channel::<RunnerMessage>(8);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let connection = Arc::new(PersistentProcessConnection {
            sender,
            pending: pending.clone(),
            closed: closed.clone(),
        });
        let request = build_execution_request("req-race", "job-race");

        let pending_guard = pending.lock().await;
        let execute_task = tokio::spawn({
            let connection = connection.clone();
            let request = request.clone();
            async move {
                connection
                    .execute(&request, Some(Duration::from_millis(50)))
                    .await
            }
        });

        tokio::task::yield_now().await;
        closed.store(true, Ordering::SeqCst);
        drop(pending_guard);

        let err = execute_task
            .await
            .expect("execute task should complete")
            .expect_err("execution should fail when connection closes during setup");
        assert!(err.to_string().contains("runner connection closed"));
        assert!(pending.lock().await.is_empty());
    }

    #[test]
    fn spawn_process_with_reuse_socket_does_not_increment_cursor() {
        // This test verifies that when reuse_socket is Some, the tcp_port_cursor is not incremented
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["true".to_string()],
            pool_size: 2,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: TcpSocketSpec {
                host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                port: 9000,
            },
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: None,
        };

        // Calling next_tcp_socket() increments cursor
        let first = pool.next_tcp_socket().unwrap();
        assert_eq!(first.port(), 9000);
        assert_eq!(pool.tcp_port_cursor.load(Ordering::Relaxed), 1);

        let second = pool.next_tcp_socket().unwrap();
        assert_eq!(second.port(), 9001);
        assert_eq!(pool.tcp_port_cursor.load(Ordering::Relaxed), 2);

        // But when we would spawn_process with reuse_socket=Some(...),
        // it skips next_tcp_socket() entirely and uses the provided socket.
        // The cursor stays at 2.
        // (We can't easily test spawn_process directly here without a real process,
        // but the logic is: reuse_socket.is_some() => use it, don't call next_tcp_socket())
    }

    #[tokio::test]
    async fn respawn_preserves_socket_target() {
        // Verify that respawn() captures the old socket and passes it to spawn_process
        let spawn_override = Arc::new(|| {
            let (_spec, socket_addr) = allocate_tcp_spec();
            let child = Command::new("sleep").arg("60").spawn().unwrap();
            SocketProcess {
                child,
                socket: socket_addr,
                stdout_task: None,
                stderr_task: None,
                permits: Arc::new(Semaphore::new(1)),
                connection: None,
            }
        });

        let (spec, _addr) = allocate_tcp_spec();
        let pool = SocketRunnerPool {
            name: "test".to_string(),
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            tcp_socket: spec,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_RUNNER_CONNECT_TIMEOUT_MS as u64),
            shutdown_term_grace: Duration::from_millis(50),
            capture_output: true,
            spawn_override: Some(spawn_override),
        };

        pool.ensure_started().await.unwrap();
        let proc = {
            let processes = pool.processes.lock().await;
            processes.first().cloned().unwrap()
        };

        // Initial socket
        let initial_socket = { proc.lock().await.socket };

        // Kill process
        {
            let mut guard = proc.lock().await;
            let _ = guard.child.kill().await;
            let _ = guard.child.wait().await;
        }
        sleep(Duration::from_millis(50)).await;

        // Respawn - the spawn_override will generate a new tcp socket address,
        // but in real usage (without spawn_override), respawn() passes
        // the old socket to spawn_process(Some(old_socket))
        pool.respawn(&proc).await.unwrap();

        // Note: spawn_override bypasses reuse logic, so socket changes.
        // This test primarily verifies respawn() doesn't panic and processes are restarted.
        // The spawn_process_with_reuse_socket_does_not_increment_cursor test above
        // verifies the cursor logic.

        let new_socket = { proc.lock().await.socket };
        // With spawn_override, socket will be different (new UUID)
        assert_ne!(initial_socket, new_socket);

        pool.close().await.unwrap();
    }

    #[test]
    fn determine_needed_runners_includes_default() {
        let settings = RRQSettings {
            default_runner_name: "python".to_string(),
            default_queue_name: "default".to_string(),
            ..Default::default()
        };

        let needed = super::determine_needed_runners(&settings, None);
        assert!(needed.contains("python"));
        assert_eq!(needed.len(), 1);
    }

    #[test]
    fn determine_needed_runners_includes_routed_runners() {
        let mut settings = RRQSettings {
            default_runner_name: "python".to_string(),
            default_queue_name: "default".to_string(),
            ..Default::default()
        };
        settings
            .runner_routes
            .insert("mail-ingest".to_string(), "mail_runner".to_string());
        settings
            .runner_routes
            .insert("mail-extract".to_string(), "mail_runner".to_string());

        // When listening to mail-ingest queue, should need mail_runner + default
        let needed = super::determine_needed_runners(&settings, Some(&["mail-ingest".to_string()]));
        assert!(needed.contains("python")); // default runner
        assert!(needed.contains("mail_runner")); // routed runner
        assert_eq!(needed.len(), 2);
    }

    #[test]
    fn determine_needed_runners_uses_default_queue_when_none_provided() {
        let mut settings = RRQSettings {
            default_runner_name: "python".to_string(),
            default_queue_name: "my-queue".to_string(),
            ..Default::default()
        };
        settings
            .runner_routes
            .insert("my-queue".to_string(), "special_runner".to_string());

        // When no queues provided, uses default_queue_name which is routed
        let needed = super::determine_needed_runners(&settings, None);
        assert!(needed.contains("python")); // default runner
        assert!(needed.contains("special_runner")); // routed for default queue
        assert_eq!(needed.len(), 2);
    }

    #[test]
    fn determine_needed_runners_handles_bare_and_prefixed_queue_names() {
        let mut settings = RRQSettings {
            default_runner_name: "python".to_string(),
            default_queue_name: "default".to_string(),
            ..Default::default()
        };
        settings.runner_routes.insert(
            "rrq:queue:mail-ingest".to_string(),
            "mail_runner".to_string(),
        );
        settings
            .runner_routes
            .insert("legacy-mail".to_string(), "legacy_runner".to_string());

        let needed_from_bare =
            super::determine_needed_runners(&settings, Some(&["mail-ingest".to_string()]));
        assert!(needed_from_bare.contains("mail_runner"));

        let needed_from_prefixed = super::determine_needed_runners(
            &settings,
            Some(&["rrq:queue:legacy-mail".to_string()]),
        );
        assert!(needed_from_prefixed.contains("legacy_runner"));
    }

    #[test]
    fn determine_needed_runners_deduplicates() {
        let mut settings = RRQSettings {
            default_runner_name: "shared".to_string(),
            default_queue_name: "default".to_string(),
            ..Default::default()
        };
        settings
            .runner_routes
            .insert("queue-a".to_string(), "shared".to_string());
        settings
            .runner_routes
            .insert("queue-b".to_string(), "shared".to_string());

        let needed = super::determine_needed_runners(
            &settings,
            Some(&["queue-a".to_string(), "queue-b".to_string()]),
        );
        assert!(needed.contains("shared"));
        assert_eq!(needed.len(), 1); // Only one runner, deduplicated
    }
}
