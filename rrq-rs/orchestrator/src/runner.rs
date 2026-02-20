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
// Built-in RRQ runner runtimes cap in-flight requests per TCP connection at 64.
const MAX_IN_FLIGHT_PER_CONNECTION_LIMIT: usize = 64;
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

fn validate_max_in_flight_limit(max_in_flight: usize, runner_name: &str) -> Result<()> {
    if max_in_flight == 0 {
        return Err(anyhow::anyhow!(
            "max_in_flight must be positive for runner '{}'",
            runner_name
        ));
    }
    if max_in_flight > MAX_IN_FLIGHT_PER_CONNECTION_LIMIT {
        return Err(anyhow::anyhow!(
            "max_in_flight must be <= {} for runner '{}'",
            MAX_IN_FLIGHT_PER_CONNECTION_LIMIT,
            runner_name
        ));
    }
    Ok(())
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

    async fn remove_pending_request(&self, request_id: &str) -> bool {
        let mut pending = self.pending.lock().await;
        pending.remove(request_id).is_some()
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
        validate_max_in_flight_limit(max_in_flight, &name)?;
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

    async fn remove_pending_request_for_socket(
        &self,
        socket: RunnerSocketTarget,
        request_id: &str,
    ) -> bool {
        let processes = {
            let processes = self.processes.lock().await;
            processes.clone()
        };
        for proc in processes {
            let connection = {
                let guard = proc.lock().await;
                if guard.socket == socket {
                    guard.connection.clone()
                } else {
                    None
                }
            };
            if let Some(connection) = connection {
                return connection.remove_pending_request(request_id).await;
            }
        }
        false
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

    async fn clear_pending_request_for_timeout(
        &self,
        request_id: &str,
        socket: RunnerSocketTarget,
    ) {
        let removed = self
            .pool
            .remove_pending_request_for_socket(socket, request_id)
            .await;
        if removed {
            debug!(
                request_id = %request_id,
                socket = %socket,
                "removed timed-out request from persistent pending map"
            );
        }
    }

    async fn finalize_execute_result(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        request: &ExecutionRequest,
        result: Result<ExecutionOutcome>,
    ) -> Result<ExecutionOutcome> {
        let timed_out = result.as_ref().err().is_some_and(is_response_timeout_error);
        let timeout_target = if timed_out {
            let in_flight = self.in_flight.lock().await;
            let timed_out_info = in_flight.get(&request.request_id).cloned();
            let has_other_on_socket = timed_out_info.as_ref().is_some_and(|info| {
                in_flight.iter().any(|(other_request_id, other)| {
                    other_request_id != &request.request_id && other.socket == info.socket
                })
            });
            timed_out_info.map(|info| (info, has_other_on_socket))
        } else {
            None
        };
        let mut timeout_cancel_sent = false;
        if timed_out && let Some((info, has_other_on_socket)) = timeout_target.as_ref() {
            let should_try_cancel = *has_other_on_socket || self.send_cancel_hints;
            if should_try_cancel {
                match self
                    .send_cancel_hint(request.request_id.as_str(), info)
                    .await
                {
                    Ok(()) => timeout_cancel_sent = true,
                    Err(err) => {
                        warn!(
                            job_id = %request.job_id,
                            request_id = %request.request_id,
                            socket = %info.socket,
                            error = %err,
                            "failed to send runner cancel hint during response-timeout cleanup"
                        );
                    }
                }
            }
        }

        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&request.request_id);
        }
        if timed_out {
            if let Some((info, has_other_on_socket)) = timeout_target {
                self.clear_pending_request_for_timeout(request.request_id.as_str(), info.socket)
                    .await;
                if has_other_on_socket && timeout_cancel_sent {
                    debug!(
                        job_id = %request.job_id,
                        request_id = %request.request_id,
                        socket = %info.socket,
                        "skipping runner process termination on response timeout because targeted cancel succeeded for shared socket"
                    );
                } else {
                    if has_other_on_socket {
                        warn!(
                            job_id = %request.job_id,
                            request_id = %request.request_id,
                            socket = %info.socket,
                            "terminating shared runner process on response timeout because targeted cancel was unavailable"
                        );
                    }
                    let _ = self
                        .pool
                        .terminate_process_for_socket(info.socket, "response_timeout")
                        .await;
                }
            } else {
                debug!(
                    job_id = %request.job_id,
                    request_id = %request.request_id,
                    "skipping runner process termination on response timeout because request is not registered in-flight"
                );
            }
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
        request: &ExecutionRequest,
        send_cancel_hint: bool,
    ) {
        let (timed_out, has_other_on_socket) = {
            let mut in_flight = self.in_flight.lock().await;
            let timed_out = in_flight.remove(&request.request_id);
            let has_other_on_socket = timed_out
                .as_ref()
                .is_some_and(|info| in_flight.values().any(|other| other.socket == info.socket));
            (timed_out, has_other_on_socket)
        };

        let mut timeout_cancel_sent = false;
        if let Some(info) = timed_out.as_ref() {
            // Shared sockets must cancel the specific request to avoid orphaned
            // execution after RRQ marks the job timed out.
            let should_try_cancel =
                has_other_on_socket || (send_cancel_hint && self.send_cancel_hints);
            if should_try_cancel {
                match self
                    .send_cancel_hint(request.request_id.as_str(), info)
                    .await
                {
                    Ok(()) => timeout_cancel_sent = true,
                    Err(err) => {
                        warn!(
                            job_id = %request.job_id,
                            request_id = %request.request_id,
                            socket = %info.socket,
                            error = %err,
                            "failed to send runner cancel hint during worker timeout cleanup"
                        );
                    }
                }
            }
        }

        if let Some(info) = timed_out {
            self.clear_pending_request_for_timeout(request.request_id.as_str(), info.socket)
                .await;
            if has_other_on_socket && timeout_cancel_sent {
                debug!(
                    job_id = %request.job_id,
                    request_id = %request.request_id,
                    socket = %info.socket,
                    "skipping runner process termination during worker timeout cleanup because targeted cancel succeeded for shared socket"
                );
            } else {
                if has_other_on_socket {
                    warn!(
                        job_id = %request.job_id,
                        request_id = %request.request_id,
                        socket = %info.socket,
                        "terminating shared runner process during worker timeout cleanup because targeted cancel was unavailable"
                    );
                }
                let _ = self
                    .pool
                    .terminate_process_for_socket(info.socket, "worker_timeout")
                    .await;
            }
        } else {
            debug!(
                job_id = %request.job_id,
                request_id = %request.request_id,
                "skipping runner process termination during worker timeout cleanup because request is not registered in-flight"
            );
        }
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
                self.cleanup_worker_timeout_with_permit_held(&request, send_cancel_hint)
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
        self.clear_pending_request_for_timeout(&request_id, info.socket)
            .await;

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
/// - The default runner only when at least one selected queue is not explicitly routed
pub fn determine_needed_runners(
    settings: &RRQSettings,
    queues: Option<&[String]>,
) -> std::collections::HashSet<String> {
    let mut needed = std::collections::HashSet::new();
    let mut needs_default_runner = false;

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
        } else {
            needs_default_runner = true;
        }
    }

    if needs_default_runner && !settings.default_runner_name.is_empty() {
        needed.insert(settings.default_runner_name.clone());
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
        validate_max_in_flight_limit(limit, name)?;
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
mod tests;
