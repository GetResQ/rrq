use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::constants::DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS;
use anyhow::{Context, Result};
use async_trait::async_trait;
use rrq_protocol::{
    CancelRequest, ExecutionOutcome, ExecutionRequest, ExecutorMessage, FRAME_HEADER_LEN,
    PROTOCOL_VERSION, encode_frame,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, timeout};
use uuid::Uuid;

const ENV_EXECUTOR_SOCKET: &str = "RRQ_EXECUTOR_SOCKET";
const ENV_EXECUTOR_TCP_SOCKET: &str = "RRQ_EXECUTOR_TCP_SOCKET";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct TcpSocketSpec {
    host: IpAddr,
    port: u16,
}

impl TcpSocketSpec {
    fn addr(&self, port: u16) -> SocketAddr {
        SocketAddr::new(self.host, port)
    }
}

fn parse_tcp_socket(raw: &str) -> Result<TcpSocketSpec> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow::anyhow!("executor tcp_socket cannot be empty"));
    }

    let (host, port_str) = if let Some(rest) = raw.strip_prefix('[') {
        let (host, port_str) = rest
            .split_once("]:")
            .ok_or_else(|| anyhow::anyhow!("executor tcp_socket must be in [host]:port format"))?;
        (host, port_str)
    } else {
        let (host, port_str) = raw
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("executor tcp_socket must be in host:port format"))?;
        if host.is_empty() {
            return Err(anyhow::anyhow!("executor tcp_socket host cannot be empty"));
        }
        (host, port_str)
    };

    let port: u16 = port_str
        .parse()
        .with_context(|| format!("Invalid executor tcp_socket port: {port_str}"))?;
    if port == 0 {
        return Err(anyhow::anyhow!("executor tcp_socket port must be > 0"));
    }

    let ip = if host == "localhost" {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    } else {
        let parsed: IpAddr = host
            .parse()
            .with_context(|| format!("Invalid executor tcp_socket host: {host}"))?;
        if !parsed.is_loopback() {
            return Err(anyhow::anyhow!(
                "executor tcp_socket host must be localhost"
            ));
        }
        parsed
    };

    Ok(TcpSocketSpec { host: ip, port })
}

fn connect_timeout_from_settings(timeout_ms: i64) -> Duration {
    let ms = if timeout_ms > 0 {
        timeout_ms
    } else {
        DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS
    };
    Duration::from_millis(ms as u64)
}

fn socket_path_max_len() -> Option<usize> {
    if cfg!(target_os = "macos") {
        Some(104)
    } else if cfg!(target_os = "linux") {
        Some(108)
    } else {
        None
    }
}

fn default_socket_base_dir() -> PathBuf {
    let tmp = PathBuf::from("/tmp");
    if tmp.is_dir() {
        return tmp;
    }
    std::env::temp_dir()
}

fn resolve_base_dir(cwd: &Option<String>) -> Result<PathBuf> {
    if let Some(cwd) = cwd {
        let path = PathBuf::from(cwd);
        if path.is_absolute() {
            return Ok(path);
        }
        return Ok(std::env::current_dir()?.join(path));
    }
    Ok(std::env::current_dir()?)
}

async fn resolve_socket_dir(
    socket_dir: Option<String>,
    cwd: &Option<String>,
) -> Result<(PathBuf, Option<PathBuf>)> {
    match socket_dir {
        Some(path) => {
            let mut path = PathBuf::from(path);
            if !path.is_absolute() {
                let base = resolve_base_dir(cwd)?;
                path = base.join(path);
            }
            tokio::fs::create_dir_all(&path).await?;
            Ok((path, None))
        }
        None => {
            let base = default_socket_base_dir();
            let path = base.join(format!("rrq-executor-{}", Uuid::new_v4()));
            tokio::fs::create_dir_all(&path).await?;
            Ok((path.clone(), Some(path)))
        }
    }
}

#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome>;
    async fn cancel(&self, _job_id: &str, _request_id: Option<&str>) -> Result<()> {
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

trait ExecutorIo: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite + ?Sized> ExecutorIo for T {}

type ExecutorStream = Box<dyn ExecutorIo + Unpin + Send>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExecutorSocketTarget {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

impl ExecutorSocketTarget {
    fn env(&self) -> (&'static str, String) {
        match self {
            ExecutorSocketTarget::Unix(path) => {
                (ENV_EXECUTOR_SOCKET, path.to_string_lossy().to_string())
            }
            ExecutorSocketTarget::Tcp(addr) => (ENV_EXECUTOR_TCP_SOCKET, addr.to_string()),
        }
    }
}

struct SocketProcess {
    child: Child,
    socket: ExecutorSocketTarget,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
    permits: Arc<Semaphore>,
}

pub struct SocketExecutorPool {
    cmd: Vec<String>,
    pool_size: usize,
    max_in_flight: usize,
    env: Option<HashMap<String, String>>,
    cwd: Option<String>,
    socket_dir: Option<PathBuf>,
    owned_socket_dir: Option<PathBuf>,
    tcp_socket: Option<TcpSocketSpec>,
    tcp_port_cursor: AtomicUsize,
    processes: Mutex<Vec<Arc<Mutex<SocketProcess>>>>,
    cursor: AtomicUsize,
    availability: Arc<Notify>,
    response_timeout: Option<Duration>,
    connect_timeout: Duration,
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

impl SocketExecutorPool {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        cmd: Vec<String>,
        pool_size: usize,
        max_in_flight: usize,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        socket_dir: Option<String>,
        tcp_socket: Option<String>,
        response_timeout: Option<Duration>,
        connect_timeout: Duration,
    ) -> Result<Self> {
        if pool_size == 0 {
            return Err(anyhow::anyhow!("pool_size must be positive"));
        }
        if max_in_flight == 0 {
            return Err(anyhow::anyhow!("max_in_flight must be positive"));
        }
        if cmd.is_empty() {
            return Err(anyhow::anyhow!("cmd must not be empty"));
        }

        if socket_dir.is_some() && tcp_socket.is_some() {
            return Err(anyhow::anyhow!("tcp_socket cannot be used with socket_dir"));
        }
        let tcp_socket = tcp_socket.map(|raw| parse_tcp_socket(&raw)).transpose()?;
        if let Some(spec) = &tcp_socket {
            let max_port = spec.port as u32 + (pool_size as u32).saturating_sub(1);
            if max_port > u16::MAX as u32 {
                return Err(anyhow::anyhow!(
                    "executor tcp_socket range too small for pool_size"
                ));
            }
        }

        let (socket_dir, owned_socket_dir) = if tcp_socket.is_some() {
            (None, None)
        } else {
            let (socket_dir, owned_socket_dir) = resolve_socket_dir(socket_dir, &cwd).await?;
            (Some(socket_dir), owned_socket_dir)
        };

        let pool = Self {
            cmd,
            pool_size,
            max_in_flight,
            env,
            cwd,
            socket_dir,
            owned_socket_dir,
            tcp_socket,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout,
            connect_timeout,
            #[cfg(test)]
            spawn_override: None,
        };
        pool.ensure_started().await?;
        Ok(pool)
    }

    async fn ensure_started(&self) -> Result<()> {
        let mut processes = self.processes.lock().await;
        if !processes.is_empty() {
            return Ok(());
        }
        for _ in 0..self.pool_size {
            let proc = self.spawn_process().await?;
            processes.push(Arc::new(Mutex::new(proc)));
        }
        Ok(())
    }

    fn next_socket_path(&self) -> Result<PathBuf> {
        let socket_dir = self
            .socket_dir
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("socket_dir is required for unix sockets"))?;
        Ok(socket_dir.join(format!("exec-{}.sock", Uuid::new_v4())))
    }

    fn next_tcp_socket(&self) -> Result<SocketAddr> {
        let spec = self
            .tcp_socket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("tcp_socket is required for TCP sockets"))?;
        let offset = self.tcp_port_cursor.fetch_add(1, Ordering::Relaxed);
        let port = spec.port as u32 + offset as u32;
        if port > u16::MAX as u32 {
            return Err(anyhow::anyhow!("executor tcp_socket port range exhausted"));
        }
        Ok(spec.addr(port as u16))
    }

    async fn spawn_process(&self) -> Result<SocketProcess> {
        #[cfg(test)]
        if let Some(spawn_override) = &self.spawn_override {
            return Ok((spawn_override)());
        }
        let socket = if self.tcp_socket.is_some() {
            let addr = self.next_tcp_socket()?;
            ExecutorSocketTarget::Tcp(addr)
        } else {
            let socket_path = self.next_socket_path()?;
            self.validate_socket_path(&socket_path)?;
            if socket_path.exists() {
                let _ = tokio::fs::remove_file(&socket_path).await;
            }
            ExecutorSocketTarget::Unix(socket_path)
        };

        let mut command = Command::new(&self.cmd[0]);
        if self.cmd.len() > 1 {
            command.args(&self.cmd[1..]);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(env) = &self.env {
            command.envs(env);
        }
        let (env_key, env_value) = socket.env();
        command.env(env_key, env_value);
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().context("failed to spawn socket executor")?;
        let stdout_task = child.stdout.take().map(|stdout| {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::warn!("[executor:stdout] {}", line);
                }
            })
        });
        let stderr_task = child.stderr.take().map(|stderr| {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::warn!("[executor:stderr] {}", line);
                }
            })
        });

        match self.connect_socket(&socket, &mut child).await {
            Ok(()) => {}
            Err(err) => {
                if let Some(task) = stdout_task.as_ref() {
                    task.abort();
                }
                if let Some(task) = stderr_task.as_ref() {
                    task.abort();
                }
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(err);
            }
        };

        Ok(SocketProcess {
            child,
            socket,
            stdout_task,
            stderr_task,
            permits: Arc::new(Semaphore::new(self.max_in_flight)),
        })
    }

    fn validate_socket_path(&self, socket_path: &Path) -> Result<()> {
        if let Some(max_len) = socket_path_max_len() {
            let len = socket_path.to_string_lossy().len();
            if len > max_len {
                return Err(anyhow::anyhow!(
                    "executor socket path is too long ({} > {} bytes). \
set socket_dir to a shorter absolute path (e.g. /tmp/rrq).",
                    len,
                    max_len
                ));
            }
        }
        Ok(())
    }

    async fn connect_socket(&self, socket: &ExecutorSocketTarget, child: &mut Child) -> Result<()> {
        let deadline = Instant::now() + self.connect_timeout;
        let mut last_error: Option<anyhow::Error> = None;
        loop {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(anyhow::anyhow!(
                    "executor exited before socket ready ({status})"
                ));
            }
            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "executor socket not ready: {}",
                    last_error
                        .as_ref()
                        .map(|err| err.to_string())
                        .unwrap_or_else(|| "unknown error".to_string())
                ));
            }
            match connect_stream(socket).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    let retryable = matches!(
                        err.kind(),
                        std::io::ErrorKind::NotFound
                            | std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                    );
                    if !retryable {
                        return Err(err.into());
                    }
                    last_error = Some(err.into());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn acquire_process(&self) -> Result<(Arc<Mutex<SocketProcess>>, ProcessPermit)> {
        self.ensure_started().await?;
        loop {
            let notified = self.availability.notified();
            let processes = {
                let guard = self.processes.lock().await;
                if guard.is_empty() {
                    return Err(anyhow::anyhow!("executor pool has no processes"));
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
        let _ = self.terminate(&mut guard).await;
        *guard = self.spawn_process().await?;
        self.availability.notify_waiters();
        Ok(())
    }

    async fn terminate(&self, proc: &mut SocketProcess) -> Result<()> {
        if let Some(task) = proc.stdout_task.take() {
            task.abort();
        }
        if let Some(task) = proc.stderr_task.take() {
            task.abort();
        }
        let _ = proc.child.kill().await;
        let _ = proc.child.wait().await;
        if let ExecutorSocketTarget::Unix(path) = &proc.socket {
            let _ = tokio::fs::remove_file(path).await;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut processes = self.processes.lock().await;
        for proc in processes.iter() {
            let mut guard = proc.lock().await;
            let _ = self.terminate(&mut guard).await;
        }
        processes.clear();
        if let Some(path) = &self.owned_socket_dir {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
        Ok(())
    }
}

pub struct SocketExecutor {
    pool: Arc<SocketExecutorPool>,
    in_flight: Arc<Mutex<HashMap<String, InFlightRequest>>>,
}

impl SocketExecutor {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        cmd: Vec<String>,
        pool_size: usize,
        max_in_flight: usize,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        socket_dir: Option<String>,
        tcp_socket: Option<String>,
        response_timeout_seconds: Option<f64>,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let response_timeout = response_timeout_seconds.map(Duration::from_secs_f64);
        let pool = SocketExecutorPool::new(
            cmd,
            pool_size,
            max_in_flight,
            env,
            cwd,
            socket_dir,
            tcp_socket,
            response_timeout,
            connect_timeout,
        )
        .await?;
        Ok(Self {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn execute_with_process(
        &self,
        proc: &Arc<Mutex<SocketProcess>>,
        request: &ExecutionRequest,
    ) -> Result<ExecutionOutcome> {
        let mut socket = {
            let guard = proc.lock().await;
            guard.socket.clone()
        };
        let mut stream = match connect_stream(&socket).await {
            Ok(stream) => stream,
            Err(err) => {
                let refreshed = {
                    let guard = proc.lock().await;
                    guard.socket.clone()
                };
                if refreshed != socket {
                    socket = refreshed;
                    connect_stream(&socket).await?
                } else {
                    return Err(err.into());
                }
            }
        };
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.insert(
                request.request_id.clone(),
                InFlightRequest {
                    job_id: request.job_id.clone(),
                    socket: socket.clone(),
                },
            );
        }
        let request_message = ExecutorMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut stream, &request_message).await?;
        let deadline = self.pool.response_timeout.map(|d| Instant::now() + d);
        let read = read_message(&mut stream);
        let message = if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(anyhow::anyhow!("executor response timeout"));
            }
            timeout(remaining, read)
                .await
                .context("executor response timeout")??
        } else {
            read.await?
        };
        let message = message.context("executor process exited")?;
        match message {
            ExecutorMessage::Response { payload } => {
                if payload.job_id.as_deref() != Some(request.job_id.as_str()) {
                    return Err(anyhow::anyhow!(
                        "executor outcome job_id mismatch (expected {}, got {:?})",
                        request.job_id,
                        payload.job_id
                    ));
                }
                if payload.request_id.as_deref() != Some(request.request_id.as_str()) {
                    return Err(anyhow::anyhow!(
                        "executor outcome request_id mismatch (expected {}, got {:?})",
                        request.request_id,
                        payload.request_id
                    ));
                }
                Ok(payload)
            }
            ExecutorMessage::Request { .. } | ExecutorMessage::Cancel { .. } => {
                Err(anyhow::anyhow!("unexpected executor message"))
            }
        }
    }
}

#[async_trait]
impl Executor for SocketExecutor {
    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutcome> {
        let (proc, _permit) = self.pool.acquire_process().await?;
        let result = self.execute_with_process(&proc, &request).await;
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
                if exited {
                    let _ = self.pool.respawn(&proc).await;
                }
                Err(err)
            }
        }
    }

    async fn cancel(&self, job_id: &str, request_id: Option<&str>) -> Result<()> {
        let target = {
            let mut in_flight = self.in_flight.lock().await;
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
            let Some(request_id) = resolved_request_id else {
                return Ok(());
            };
            let info = in_flight.remove(&request_id);
            info.map(|info| (request_id, info))
        };
        let Some((request_id, info)) = target else {
            return Ok(());
        };
        let mut stream = match connect_stream(&info.socket).await {
            Ok(stream) => stream,
            Err(_) => return Ok(()),
        };
        let message = ExecutorMessage::Cancel {
            payload: CancelRequest {
                protocol_version: PROTOCOL_VERSION.to_string(),
                job_id: info.job_id,
                request_id: Some(request_id),
                hard_kill: false,
            },
        };
        let _ = write_message(&mut stream, &message).await;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.pool.close().await
    }
}

struct InFlightRequest {
    job_id: String,
    socket: ExecutorSocketTarget,
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

pub fn resolve_executor_max_in_flight(
    settings: &crate::settings::RRQSettings,
    watch_mode: bool,
) -> Result<HashMap<String, usize>> {
    let mut max_in_flight = HashMap::new();
    for (name, config) in &settings.executors {
        let limit = if watch_mode {
            1
        } else {
            config.max_in_flight.unwrap_or(1)
        };
        if limit == 0 {
            return Err(anyhow::anyhow!(
                "max_in_flight must be positive for executor '{}'",
                name
            ));
        }
        max_in_flight.insert(name.clone(), limit);
    }
    Ok(max_in_flight)
}

pub async fn build_executors_from_settings(
    settings: &crate::settings::RRQSettings,
    pool_sizes: Option<&HashMap<String, usize>>,
    max_in_flight: Option<&HashMap<String, usize>>,
) -> Result<HashMap<String, Arc<dyn Executor>>> {
    let pool_sizes = match pool_sizes {
        Some(map) => map.clone(),
        None => resolve_executor_pool_sizes(settings, false, None)?,
    };
    let max_in_flight = match max_in_flight {
        Some(map) => map.clone(),
        None => resolve_executor_max_in_flight(settings, false)?,
    };
    let connect_timeout = connect_timeout_from_settings(settings.executor_connect_timeout_ms);
    let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
    for (name, config) in &settings.executors {
        if config.cmd.is_none() {
            return Err(anyhow::anyhow!(
                "executor '{}' requires cmd for socket mode",
                name
            ));
        }
        let pool_size = pool_sizes
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing pool size for executor '{}'", name))?;
        let max_in_flight = max_in_flight
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing max_in_flight for executor '{}'", name))?;
        let cmd = config.cmd.clone().unwrap_or_default();
        let executor = SocketExecutor::new(
            cmd,
            pool_size,
            max_in_flight,
            config.env.clone(),
            config.cwd.clone(),
            config.socket_dir.clone(),
            config.tcp_socket.clone(),
            config.response_timeout_seconds,
            connect_timeout,
        )
        .await?;
        executors.insert(name.clone(), Arc::new(executor));
    }
    Ok(executors)
}

async fn connect_stream(target: &ExecutorSocketTarget) -> std::io::Result<ExecutorStream> {
    match target {
        ExecutorSocketTarget::Unix(path) => {
            let stream = UnixStream::connect(path).await?;
            Ok(Box::new(stream))
        }
        ExecutorSocketTarget::Tcp(addr) => {
            let stream = TcpStream::connect(addr).await?;
            Ok(Box::new(stream))
        }
    }
}

async fn read_message<R>(stream: &mut R) -> Result<Option<ExecutorMessage>>
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
        return Err(anyhow::anyhow!("executor message payload cannot be empty"));
    }
    if length > MAX_FRAME_LEN {
        return Err(anyhow::anyhow!("executor message payload too large"));
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    let message = serde_json::from_slice(&payload)?;
    Ok(Some(message))
}

async fn write_message<W>(stream: &mut W, message: &ExecutorMessage) -> Result<()>
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
    use crate::settings::{ExecutorConfig, ExecutorType, RRQSettings};
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
            args: Vec::new(),
            kwargs: HashMap::new(),
            context: rrq_protocol::ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                worker_id: None,
            },
        };
        let message = ExecutorMessage::Request {
            payload: request.clone(),
        };
        write_message(&mut client, &message).await.unwrap();
        let decoded = read_message(&mut server).await.unwrap().unwrap();
        match decoded {
            ExecutorMessage::Request { payload } => {
                assert_eq!(payload.job_id, request.job_id);
                assert_eq!(payload.request_id, request.request_id);
                assert_eq!(payload.function_name, request.function_name);
            }
            _ => panic!("unexpected message variant"),
        }
    }

    fn build_test_pool(max_in_flight: usize) -> SocketExecutorPool {
        let child = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn sleep");
        let socket_path = std::env::temp_dir().join(format!("rrq-test-{}.sock", Uuid::new_v4()));
        let process = SocketProcess {
            child,
            socket: ExecutorSocketTarget::Unix(socket_path),
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(max_in_flight)),
        };
        SocketExecutorPool {
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight,
            env: None,
            cwd: None,
            socket_dir: Some(std::env::temp_dir()),
            owned_socket_dir: None,
            tcp_socket: None,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(vec![Arc::new(Mutex::new(process))]),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS as u64),
            spawn_override: None,
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
    async fn ensure_process_respawns_exited_child() {
        let spawn_override = Arc::new(|| {
            let socket_path =
                std::env::temp_dir().join(format!("rrq-test-{}.sock", Uuid::new_v4()));
            let child = Command::new("sleep").arg("60").spawn().unwrap();
            SocketProcess {
                child,
                socket: ExecutorSocketTarget::Unix(socket_path),
                stdout_task: None,
                stderr_task: None,
                permits: Arc::new(Semaphore::new(1)),
            }
        });
        let pool = SocketExecutorPool {
            cmd: vec!["sleep".to_string(), "60".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            socket_dir: Some(std::env::temp_dir()),
            owned_socket_dir: None,
            tcp_socket: None,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS as u64),
            spawn_override: Some(spawn_override),
        };
        pool.ensure_started().await.unwrap();
        let proc = {
            let processes = pool.processes.lock().await;
            processes.first().cloned().unwrap()
        };
        let old_socket = { proc.lock().await.socket.clone() };
        {
            let mut guard = proc.lock().await;
            let _ = guard.child.kill().await;
            let _ = guard.child.wait().await;
        }
        sleep(Duration::from_millis(50)).await;
        pool.ensure_process(&proc).await.unwrap();
        let new_socket = { proc.lock().await.socket.clone() };
        assert_ne!(old_socket, new_socket);
        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn connect_socket_errors_when_child_exits() {
        let pool = SocketExecutorPool {
            cmd: vec!["true".to_string()],
            pool_size: 1,
            max_in_flight: 1,
            env: None,
            cwd: None,
            socket_dir: Some(std::env::temp_dir()),
            owned_socket_dir: None,
            tcp_socket: None,
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(50),
            spawn_override: None,
        };
        let socket_path = std::env::temp_dir().join(format!("rrq-missing-{}.sock", Uuid::new_v4()));
        let mut child = Command::new("true").spawn().unwrap();
        let err = pool
            .connect_socket(&ExecutorSocketTarget::Unix(socket_path), &mut child)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("executor exited before socket ready")
        );
    }

    #[test]
    fn resolve_pool_sizes_and_max_in_flight_watch_mode() {
        let mut settings = RRQSettings::default();
        let mut executors = HashMap::new();
        executors.insert(
            "python".to_string(),
            ExecutorConfig {
                executor_type: ExecutorType::Socket,
                cmd: Some(vec!["rrq-executor".to_string()]),
                pool_size: Some(4),
                max_in_flight: Some(5),
                env: None,
                cwd: None,
                socket_dir: None,
                tcp_socket: None,
                response_timeout_seconds: None,
            },
        );
        settings.executors = executors;

        let pool_sizes = resolve_executor_pool_sizes(&settings, true, None).unwrap();
        let max_in_flight = resolve_executor_max_in_flight(&settings, true).unwrap();
        assert_eq!(pool_sizes.get("python"), Some(&1));
        assert_eq!(max_in_flight.get("python"), Some(&1));
    }

    #[test]
    fn resolve_pool_sizes_and_max_in_flight_validate_zero() {
        let mut settings = RRQSettings::default();
        let mut executors = HashMap::new();
        executors.insert(
            "python".to_string(),
            ExecutorConfig {
                executor_type: ExecutorType::Socket,
                cmd: Some(vec!["rrq-executor".to_string()]),
                pool_size: Some(0),
                max_in_flight: Some(0),
                env: None,
                cwd: None,
                socket_dir: None,
                tcp_socket: None,
                response_timeout_seconds: None,
            },
        );
        settings.executors = executors;

        let err = resolve_executor_pool_sizes(&settings, false, None).unwrap_err();
        assert!(err.to_string().contains("pool_size must be positive"));
        let err = resolve_executor_max_in_flight(&settings, false).unwrap_err();
        assert!(err.to_string().contains("max_in_flight must be positive"));
    }

    #[test]
    fn parse_tcp_socket_allows_localhost() {
        let spec = parse_tcp_socket("localhost:1234").unwrap();
        assert_eq!(
            spec.addr(spec.port),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234)
        );
    }

    #[test]
    fn parse_tcp_socket_rejects_non_localhost() {
        let err = parse_tcp_socket("10.0.0.1:1234").unwrap_err();
        assert!(err.to_string().contains("localhost"));
    }

    #[test]
    fn connect_timeout_from_settings_defaults_for_non_positive() {
        let expected = Duration::from_millis(DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS as u64);
        assert_eq!(connect_timeout_from_settings(0), expected);
        assert_eq!(connect_timeout_from_settings(-5), expected);
        assert_eq!(
            connect_timeout_from_settings(5000),
            Duration::from_millis(5000)
        );
    }

    #[tokio::test]
    async fn tcp_socket_pool_range_rejects_overflow() {
        let err = SocketExecutorPool::new(
            vec!["true".to_string()],
            2,
            1,
            None,
            None,
            None,
            Some("127.0.0.1:65535".to_string()),
            None,
            Duration::from_millis(DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS as u64),
        )
        .await;
        match err {
            Err(err) => assert!(err.to_string().contains("range too small")),
            Ok(_) => panic!("expected tcp socket range error"),
        }
    }

    #[test]
    fn tcp_socket_pool_assigns_incrementing_ports() {
        let pool = SocketExecutorPool {
            cmd: vec!["true".to_string()],
            pool_size: 2,
            max_in_flight: 1,
            env: None,
            cwd: None,
            socket_dir: None,
            owned_socket_dir: None,
            tcp_socket: Some(TcpSocketSpec {
                host: IpAddr::V4(Ipv4Addr::LOCALHOST),
                port: 9000,
            }),
            tcp_port_cursor: AtomicUsize::new(0),
            processes: Mutex::new(Vec::new()),
            cursor: AtomicUsize::new(0),
            availability: Arc::new(Notify::new()),
            response_timeout: None,
            connect_timeout: Duration::from_millis(DEFAULT_EXECUTOR_CONNECT_TIMEOUT_MS as u64),
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

    #[tokio::test]
    async fn cancel_sends_cancel_message() {
        let socket_path = std::env::temp_dir().join(format!("rrq-cancel-{}.sock", Uuid::new_v4()));
        let _ = tokio::fs::remove_file(&socket_path).await;
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let message = read_message(&mut stream).await.unwrap().unwrap();
            match message {
                ExecutorMessage::Cancel { payload } => {
                    assert_eq!(payload.job_id, "job-1");
                    assert_eq!(payload.request_id.as_deref(), Some("req-1"));
                }
                _ => panic!("expected cancel message"),
            }
        });

        let pool = build_test_pool(1);
        let executor = SocketExecutor {
            pool: Arc::new(pool),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        };
        {
            let mut in_flight = executor.in_flight.lock().await;
            in_flight.insert(
                "req-1".to_string(),
                InFlightRequest {
                    job_id: "job-1".to_string(),
                    socket: ExecutorSocketTarget::Unix(socket_path.clone()),
                },
            );
        }
        executor.cancel("job-1", None).await.unwrap();
        server.await.unwrap();

        let in_flight = executor.in_flight.lock().await;
        assert!(in_flight.is_empty());
        let _ = tokio::fs::remove_file(&socket_path).await;
    }

    #[tokio::test]
    async fn execute_with_process_rejects_mismatched_request_id() {
        let socket_path =
            std::env::temp_dir().join(format!("rrq-mismatch-{}.sock", Uuid::new_v4()));
        let _ = tokio::fs::remove_file(&socket_path).await;
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let message = read_message(&mut stream).await.unwrap().unwrap();
            let request = match message {
                ExecutorMessage::Request { payload } => payload,
                _ => panic!("expected request"),
            };
            let response = ExecutionOutcome::success(
                request.job_id,
                "wrong-req",
                serde_json::json!({"ok": true}),
            );
            write_message(
                &mut stream,
                &ExecutorMessage::Response { payload: response },
            )
            .await
            .unwrap();
        });

        let child = Command::new("sleep").arg("60").spawn().unwrap();
        let process = SocketProcess {
            child,
            socket: ExecutorSocketTarget::Unix(socket_path.clone()),
            stdout_task: None,
            stderr_task: None,
            permits: Arc::new(Semaphore::new(1)),
        };
        let proc = Arc::new(Mutex::new(process));
        let executor = SocketExecutor {
            pool: Arc::new(build_test_pool(1)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        };

        let request = ExecutionRequest {
            protocol_version: rrq_protocol::PROTOCOL_VERSION.to_string(),
            request_id: "req-1".to_string(),
            job_id: "job-1".to_string(),
            function_name: "echo".to_string(),
            args: Vec::new(),
            kwargs: HashMap::new(),
            context: rrq_protocol::ExecutionContext {
                job_id: "job-1".to_string(),
                attempt: 1,
                enqueue_time: "2024-01-01T00:00:00Z".parse().unwrap(),
                queue_name: "default".to_string(),
                deadline: None,
                trace_context: None,
                worker_id: None,
            },
        };
        let err = executor
            .execute_with_process(&proc, &request)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("request_id mismatch"));

        server.await.unwrap();
        let _ = tokio::fs::remove_file(&socket_path).await;
        let mut guard = proc.lock().await;
        let _ = guard.child.kill().await;
        let _ = guard.child.wait().await;
    }
}
