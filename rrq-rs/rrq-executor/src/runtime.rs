use crate::registry::Registry;
use crate::telemetry::{NoopTelemetry, Telemetry};
use crate::types::{ExecutionError, ExecutionOutcome};
use rrq_protocol::{ExecutorMessage, PROTOCOL_VERSION, encode_frame};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

pub const ENV_EXECUTOR_SOCKET: &str = "RRQ_EXECUTOR_SOCKET";
const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

pub struct ExecutorRuntime {
    runtime: tokio::runtime::Runtime,
}

impl ExecutorRuntime {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            runtime: tokio::runtime::Runtime::new()?,
        })
    }

    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.runtime.enter()
    }

    pub fn run_socket(
        &self,
        registry: &Registry,
        socket_path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let telemetry = NoopTelemetry;
        self.run_socket_with(registry, socket_path, &telemetry)
    }

    pub fn run_socket_with<T: Telemetry + ?Sized>(
        &self,
        registry: &Registry,
        socket_path: impl AsRef<Path>,
        telemetry: &T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        run_socket_loop(&self.runtime, registry, socket_path.as_ref(), telemetry)
    }
}

pub fn run_socket(
    registry: &Registry,
    socket_path: impl AsRef<Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    ExecutorRuntime::new()?.run_socket(registry, socket_path)
}

pub fn run_socket_with<T: Telemetry + ?Sized>(
    registry: &Registry,
    socket_path: impl AsRef<Path>,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    ExecutorRuntime::new()?.run_socket_with(registry, socket_path, telemetry)
}

fn run_socket_loop<T: Telemetry + ?Sized>(
    runtime: &tokio::runtime::Runtime,
    registry: &Registry,
    socket_path: &Path,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = registry.clone();
    runtime.block_on(async move {
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(socket_path).await;
        }
        let listener = UnixListener::bind(socket_path)?;
        loop {
            let (stream, _) = listener.accept().await?;
            handle_connection(stream, &registry, telemetry).await?;
        }
    })
}

async fn handle_connection<T: Telemetry + ?Sized>(
    mut stream: UnixStream,
    registry: &Registry,
    telemetry: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let message = match read_message(&mut stream).await? {
            Some(message) => message,
            None => break,
        };
        let request = match message {
            ExecutorMessage::Request { payload } => payload,
            ExecutorMessage::Response { .. } => {
                let outcome = ExecutionOutcome {
                    job_id: Some("unknown".to_string()),
                    request_id: None,
                    status: rrq_protocol::OutcomeStatus::Error,
                    result: None,
                    error: Some(ExecutionError {
                        message: "unexpected response message".to_string(),
                        error_type: None,
                        code: None,
                        details: None,
                    }),
                    retry_after_seconds: None,
                };
                write_message(&mut stream, &ExecutorMessage::Response { payload: outcome }).await?;
                continue;
            }
        };

        if request.protocol_version != PROTOCOL_VERSION {
            let outcome = ExecutionOutcome::error(
                request.job_id.clone(),
                request.request_id.clone(),
                "Unsupported protocol version",
            );
            write_message(&mut stream, &ExecutorMessage::Response { payload: outcome }).await?;
            continue;
        }

        let outcome = registry.execute_with(request, telemetry).await;
        let response = ExecutorMessage::Response { payload: outcome };
        write_message(&mut stream, &response).await?;
    }

    Ok(())
}

async fn read_message(
    stream: &mut UnixStream,
) -> Result<Option<ExecutorMessage>, Box<dyn std::error::Error>> {
    let mut header = [0u8; 4];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err("executor message payload cannot be empty".into());
    }
    if length > MAX_FRAME_LEN {
        return Err("executor message payload too large".into());
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    let message = serde_json::from_slice(&payload)?;
    Ok(Some(message))
}

async fn write_message(
    stream: &mut UnixStream,
    message: &ExecutorMessage,
) -> Result<(), Box<dyn std::error::Error>> {
    let framed = encode_frame(message)?;
    stream.write_all(&framed).await?;
    stream.flush().await?;
    Ok(())
}
