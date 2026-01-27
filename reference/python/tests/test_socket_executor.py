from __future__ import annotations

import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from rrq.executor import ExecutionContext, ExecutionOutcome, ExecutionRequest

ENV_EXECUTOR_SOCKET = "RRQ_EXECUTOR_SOCKET"
FRAME_HEADER_SIZE = 4


def _read_exact(conn: socket.socket, size: int) -> bytes | None:
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def _read_frame(conn: socket.socket) -> dict:
    header = _read_exact(conn, FRAME_HEADER_SIZE)
    assert header is not None
    (length,) = struct.unpack(">I", header)
    payload = _read_exact(conn, length)
    assert payload is not None
    return json.loads(payload.decode("utf-8"))


def _encode_frame(message: dict) -> bytes:
    data = json.dumps(message).encode("utf-8")
    return struct.pack(">I", len(data)) + data


def _connect_socket(path: str, timeout: float = 2.0) -> socket.socket:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            conn.connect(path)
            return conn
        except (FileNotFoundError, ConnectionRefusedError, OSError) as exc:
            last_error = exc
            time.sleep(0.05)
    raise RuntimeError(f"Socket not ready: {last_error}")


def _run_executor(messages: Iterable[dict]) -> list[ExecutionOutcome]:
    script_path = Path(__file__).resolve().parents[1] / "socket_executor.py"
    with tempfile.TemporaryDirectory(prefix="rrq-socket-test-") as temp_dir:
        socket_path = os.path.join(temp_dir, "executor.sock")
        env = os.environ.copy()
        env[ENV_EXECUTOR_SOCKET] = socket_path
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        conn = _connect_socket(socket_path)
        outcomes: list[ExecutionOutcome] = []
        try:
            for message in messages:
                conn.sendall(_encode_frame(message))
                response = _read_frame(conn)
                payload = response.get("payload")
                outcomes.append(ExecutionOutcome.model_validate(payload))
        finally:
            conn.close()
            proc.wait(timeout=5)
        return outcomes


def _request(function_name: str) -> ExecutionRequest:
    context = ExecutionContext(
        job_id="job-1",
        attempt=1,
        enqueue_time=datetime.now(timezone.utc),
        queue_name="default",
    )
    return ExecutionRequest(
        request_id=f"req-{function_name}",
        job_id="job-1",
        function_name=function_name,
        args=[],
        kwargs={},
        context=context,
    )


def test_socket_executor_echo_handler() -> None:
    request = _request("echo")
    outcomes = _run_executor(
        [
            {
                "type": "request",
                "payload": request.model_dump(mode="json"),
            }
        ]
    )
    assert len(outcomes) == 1
    assert outcomes[0].job_id == "job-1"
    assert outcomes[0].request_id == "req-echo"
    assert outcomes[0].status == "success"
    assert outcomes[0].result == {"job_id": "job-1"}


def test_socket_executor_unknown_handler() -> None:
    request = _request("missing")
    outcomes = _run_executor(
        [
            {
                "type": "request",
                "payload": request.model_dump(mode="json"),
            }
        ]
    )
    assert len(outcomes) == 1
    assert outcomes[0].job_id == "job-1"
    assert outcomes[0].request_id == "req-missing"
    assert outcomes[0].status == "error"
    assert outcomes[0].error is not None
    assert outcomes[0].error.type == "handler_not_found"


def test_socket_executor_invalid_request() -> None:
    outcomes = _run_executor(
        [
            {
                "type": "request",
                "payload": {"job_id": "job-1"},
            }
        ]
    )
    assert len(outcomes) == 1
    assert outcomes[0].status == "error"
    assert outcomes[0].error is not None
    assert outcomes[0].error.message is not None
