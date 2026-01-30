from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import ctypes
import threading

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class RustProducerError(RuntimeError):
    pass


class ProducerConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    redis_dsn: str = Field(min_length=1)
    queue_name: str | None = None
    max_retries: int | None = None
    job_timeout_seconds: int | None = None
    result_ttl_seconds: int | None = None
    idempotency_ttl_seconds: int | None = None

    @field_validator("job_timeout_seconds", "idempotency_ttl_seconds")
    @classmethod
    def _validate_positive_int(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value <= 0:
            raise ValueError("must be positive")
        return value

    @field_validator("max_retries", "result_ttl_seconds")
    @classmethod
    def _validate_non_negative_int(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value < 0:
            raise ValueError("must be non-negative")
        return value


class EnqueueOptionsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queue_name: str | None = None
    job_id: str | None = None
    unique_key: str | None = None
    unique_ttl_seconds: int | None = None
    max_retries: int | None = None
    job_timeout_seconds: int | None = None
    result_ttl_seconds: int | None = None
    trace_context: dict[str, str] | None = None
    defer_by_seconds: float | None = None
    defer_until: datetime | None = None
    enqueue_time: datetime | None = None
    rate_limit_key: str | None = None
    rate_limit_seconds: float | None = None
    debounce_key: str | None = None
    debounce_seconds: float | None = None

    @field_validator("unique_ttl_seconds", "job_timeout_seconds")
    @classmethod
    def _validate_positive_int(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value <= 0:
            raise ValueError("must be positive")
        return value

    @field_validator("max_retries", "result_ttl_seconds")
    @classmethod
    def _validate_non_negative_int(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value < 0:
            raise ValueError("must be non-negative")
        return value

    @field_validator("defer_by_seconds")
    @classmethod
    def _validate_non_negative_float(cls, value: float | None) -> float | None:
        if value is None:
            return value
        if not float("-inf") < value < float("inf"):
            raise ValueError("must be finite")
        if value < 0:
            raise ValueError("must be non-negative")
        return value

    @field_validator("rate_limit_seconds", "debounce_seconds")
    @classmethod
    def _validate_positive_float(cls, value: float | None) -> float | None:
        if value is None:
            return value
        if not float("-inf") < value < float("inf"):
            raise ValueError("must be finite")
        if value <= 0:
            raise ValueError("must be positive")
        return value

    @field_validator("defer_until", "enqueue_time")
    @classmethod
    def _normalize_datetime(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class EnqueueRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["enqueue", "unique", "rate_limit", "debounce", "deferred"] | None = (
        None
    )
    function_name: str = Field(min_length=1)
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    options: EnqueueOptionsModel | None = None


class EnqueueResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["enqueued", "rate_limited"]
    job_id: str | None = None


def _find_library() -> Path:
    override = os.environ.get("RRQ_PRODUCER_LIB_PATH")
    if override:
        path = Path(override)
        if path.exists():
            return path
        raise RustProducerError(f"RRQ producer library not found at {path}")

    base_dir = Path(__file__).resolve().parent / "bin"
    candidates = [
        base_dir / "librrq_producer.so",
        base_dir / "librrq_producer.dylib",
        base_dir / "rrq_producer.dll",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise RustProducerError(
        "RRQ producer library not found. Ensure rrq/bin contains the shared library."
    )


def _load_library() -> ctypes.CDLL:
    lib_path = _find_library()
    return ctypes.CDLL(str(lib_path))


def _configure_library(lib: ctypes.CDLL) -> None:
    lib.rrq_producer_new.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_char_p)]
    lib.rrq_producer_new.restype = ctypes.c_void_p
    lib.rrq_producer_free.argtypes = [ctypes.c_void_p]
    lib.rrq_producer_free.restype = None
    lib.rrq_producer_enqueue.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_char_p),
    ]
    lib.rrq_producer_enqueue.restype = ctypes.c_void_p
    lib.rrq_string_free.argtypes = [ctypes.c_void_p]
    lib.rrq_string_free.restype = None


_LIB: ctypes.CDLL | None = None
_LIB_LOCK = threading.Lock()


def _get_library() -> ctypes.CDLL:
    global _LIB
    if _LIB is None:
        with _LIB_LOCK:
            if _LIB is None:
                lib = _load_library()
                _configure_library(lib)
                _LIB = lib
    assert _LIB is not None
    return _LIB


def _take_error(err_ptr: ctypes.c_char_p | None) -> None:
    if not err_ptr:
        return
    try:
        message = ctypes.string_at(err_ptr).decode("utf-8", errors="replace")
    finally:
        _get_library().rrq_string_free(err_ptr)
    raise RustProducerError(message)


class RustProducer:
    def __init__(self, handle: int) -> None:
        self._handle = handle

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "RustProducer":
        try:
            validated = ProducerConfigModel.model_validate(config)
        except ValidationError as exc:
            raise RustProducerError(str(exc)) from exc
        lib = _get_library()
        payload = validated.model_dump_json().encode("utf-8")
        err = ctypes.c_char_p()
        handle = lib.rrq_producer_new(payload, ctypes.byref(err))
        if not handle:
            _take_error(err)
            raise RustProducerError("Failed to create producer")
        return cls(handle)

    def close(self) -> None:
        if self._handle:
            _get_library().rrq_producer_free(self._handle)
            self._handle = 0

    def enqueue(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            validated = EnqueueRequestModel.model_validate(request)
        except ValidationError as exc:
            raise RustProducerError(str(exc)) from exc
        payload = validated.model_dump_json().encode("utf-8")
        err = ctypes.c_char_p()
        result_ptr = _get_library().rrq_producer_enqueue(
            self._handle, payload, ctypes.byref(err)
        )
        if not result_ptr:
            _take_error(err)
            raise RustProducerError("Enqueue failed")
        try:
            result_json = ctypes.string_at(result_ptr).decode("utf-8", errors="replace")
        finally:
            _get_library().rrq_string_free(result_ptr)
        try:
            response = EnqueueResponseModel.model_validate_json(result_json)
            return response.model_dump()
        except json.JSONDecodeError as exc:
            raise RustProducerError(f"Invalid response from producer: {exc}") from exc
        except ValidationError as exc:
            raise RustProducerError(f"Invalid response from producer: {exc}") from exc

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
