"""Reference socket executor implementation (Python).

Reads length-delimited ExecutionRequest messages and writes ExecutionOutcome
responses over a Unix domain socket or a localhost TCP socket.
"""

from __future__ import annotations

import json
import os
import socket
import struct
from ipaddress import ip_address
from typing import Any

from rrq.executor import ExecutionError, ExecutionOutcome, ExecutionRequest

ENV_EXECUTOR_SOCKET = "RRQ_EXECUTOR_SOCKET"
ENV_EXECUTOR_TCP_SOCKET = "RRQ_EXECUTOR_TCP_SOCKET"
FRAME_HEADER_SIZE = 4
_LOCALHOST_ALIASES = {"localhost", "127.0.0.1", "::1"}


def handle_echo(request: ExecutionRequest) -> ExecutionOutcome:
    return ExecutionOutcome(
        job_id=request.job_id,
        request_id=request.request_id,
        status="success",
        result={"job_id": request.job_id},
    )


def _read_exact(conn: socket.socket, size: int) -> bytes | None:
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def _read_frame(conn: socket.socket) -> dict[str, Any] | None:
    header = _read_exact(conn, FRAME_HEADER_SIZE)
    if header is None:
        return None
    (length,) = struct.unpack(">I", header)
    if length == 0:
        raise ValueError("Executor message payload cannot be empty")
    payload = _read_exact(conn, length)
    if payload is None:
        return None
    decoded = json.loads(payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("Executor message must be a JSON object")
    return decoded


def _write_frame(conn: socket.socket, message: dict[str, Any]) -> None:
    data = json.dumps(message).encode("utf-8")
    conn.sendall(struct.pack(">I", len(data)) + data)


def _parse_tcp_socket(value: str) -> tuple[str, int]:
    raw = value.strip()
    if not raw:
        raise ValueError("TCP socket value cannot be empty")

    if raw.startswith("["):
        host_part, sep, port_part = raw.partition("]:")
        if not sep:
            raise ValueError("TCP socket must be in [host]:port format")
        host = host_part.lstrip("[")
    else:
        host, sep, port_part = raw.rpartition(":")
        if not sep:
            raise ValueError("TCP socket must be in host:port format")
        if not host:
            raise ValueError("TCP socket host cannot be empty")

    if host not in _LOCALHOST_ALIASES:
        try:
            ip = ip_address(host)
        except ValueError as exc:
            raise ValueError(f"Invalid TCP socket host: {host}") from exc
        if not ip.is_loopback:
            raise ValueError(f"TCP socket host must be localhost; got {host}")
        host = str(ip)

    try:
        port = int(port_part)
    except ValueError as exc:
        raise ValueError(f"Invalid TCP socket port: {port_part}") from exc
    if port <= 0 or port > 65535:
        raise ValueError(f"TCP socket port out of range: {port}")

    if host == "localhost":
        host = "127.0.0.1"
    return host, port


def main() -> int:
    handlers: dict[str, Any] = {
        "echo": handle_echo,
    }

    tcp_socket = os.getenv(ENV_EXECUTOR_TCP_SOCKET)
    socket_path = os.getenv(ENV_EXECUTOR_SOCKET)
    if tcp_socket and socket_path:
        raise RuntimeError("Provide only one of TCP or Unix socket env vars")
    if tcp_socket:
        host, port = _parse_tcp_socket(tcp_socket)
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        server = socket.socket(family, socket.SOCK_STREAM)
        server.bind((host, port))
        server.listen(1)
    else:
        if not socket_path:
            raise RuntimeError(
                f"{ENV_EXECUTOR_SOCKET} or {ENV_EXECUTOR_TCP_SOCKET} must be set"
            )

        if os.path.exists(socket_path):
            os.unlink(socket_path)

        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(socket_path)
        server.listen(1)

    try:
        conn, _ = server.accept()
        with conn:
            while True:
                try:
                    message = _read_frame(conn)
                except Exception as exc:
                    outcome = ExecutionOutcome(
                        job_id="unknown",
                        request_id=None,
                        status="error",
                        error=ExecutionError(message=str(exc)),
                    )
                    _write_frame(
                        conn,
                        {
                            "type": "response",
                            "payload": outcome.model_dump(mode="json"),
                        },
                    )
                    continue
                if message is None:
                    break

                message_type = message.get("type")
                payload = message.get("payload")
                if message_type != "request" or not isinstance(payload, dict):
                    outcome = ExecutionOutcome(
                        job_id="unknown",
                        request_id=None,
                        status="error",
                        error=ExecutionError(message="Invalid executor message"),
                    )
                    _write_frame(
                        conn,
                        {
                            "type": "response",
                            "payload": outcome.model_dump(mode="json"),
                        },
                    )
                    continue

                try:
                    request = ExecutionRequest.model_validate(payload)
                except Exception as exc:
                    outcome = ExecutionOutcome(
                        job_id="unknown",
                        request_id=None,
                        status="error",
                        error=ExecutionError(message=str(exc)),
                    )
                    _write_frame(
                        conn,
                        {
                            "type": "response",
                            "payload": outcome.model_dump(mode="json"),
                        },
                    )
                    continue

                handler = handlers.get(request.function_name)
                if handler is None:
                    outcome = ExecutionOutcome(
                        job_id=request.job_id,
                        request_id=request.request_id,
                        status="error",
                        error=ExecutionError(
                            message=f"No handler for '{request.function_name}'",
                            type="handler_not_found",
                        ),
                    )
                else:
                    outcome = handler(request)

                _write_frame(
                    conn,
                    {
                        "type": "response",
                        "payload": outcome.model_dump(mode="json"),
                    },
                )
    finally:
        server.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
