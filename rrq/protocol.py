"""Shared IPC protocol helpers for executors (length-delimited JSON frames)."""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any, Literal

ExecutorMessageType = Literal["request", "response", "cancel"]

FRAME_HEADER_SIZE = 4
MAX_FRAME_SIZE = 16 * 1024 * 1024


def encode_message(message_type: ExecutorMessageType, payload: dict[str, Any]) -> bytes:
    data = json.dumps({"type": message_type, "payload": payload}).encode("utf-8")
    return struct.pack(">I", len(data)) + data


def decode_message(data: bytes) -> tuple[ExecutorMessageType, dict[str, Any]]:
    decoded = json.loads(data)
    if not isinstance(decoded, dict):
        raise ValueError("Executor message must be a JSON object")
    message_type = decoded.get("type")
    payload = decoded.get("payload")
    if message_type not in {"request", "response", "cancel"}:
        raise ValueError("Executor message missing valid type")
    if not isinstance(payload, dict):
        raise ValueError("Executor message missing payload object")
    return message_type, payload


async def read_message(
    reader: asyncio.StreamReader,
) -> tuple[ExecutorMessageType, dict[str, Any]] | None:
    try:
        header = await reader.readexactly(FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError as exc:
        if exc.partial:
            raise
        return None
    (length,) = struct.unpack(">I", header)
    if length == 0:
        raise ValueError("Executor message payload cannot be empty")
    if length > MAX_FRAME_SIZE:
        raise ValueError("Executor message payload exceeds max size")
    payload = await reader.readexactly(length)
    return decode_message(payload)


async def write_message(
    writer: asyncio.StreamWriter,
    message_type: ExecutorMessageType,
    payload: dict[str, Any],
) -> None:
    writer.write(encode_message(message_type, payload))
    await writer.drain()
