"""TOML-based configuration loader for RRQ settings."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Callable

try:
    from dotenv import find_dotenv, load_dotenv

    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
from pydantic import ValidationError

from .settings import RRQSettings

DEFAULT_CONFIG_FILENAME = "rrq.toml"
ENV_CONFIG_KEY = "RRQ_CONFIG"


def resolve_config_source(config_path: str | None = None) -> tuple[str | None, str]:
    """Resolve the config path and its source description."""
    if config_path:
        return config_path, "--config parameter"

    env_path = os.getenv(ENV_CONFIG_KEY)
    if env_path:
        return env_path, f"{ENV_CONFIG_KEY} env var"

    default_path = Path(DEFAULT_CONFIG_FILENAME)
    if default_path.is_file():
        return str(default_path), f"{DEFAULT_CONFIG_FILENAME} in cwd"

    return None, "not found"


def _normalize_toml_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize TOML payload to RRQSettings shape."""
    if "rrq" in payload:
        payload = payload["rrq"]
        if not isinstance(payload, dict):
            raise ValueError("[rrq] table must be a TOML table")

    payload.pop("worker_concurrency", None)
    routing = payload.pop("routing", None)
    if routing is not None:
        if not isinstance(routing, dict):
            raise ValueError("[rrq.routing] table must be a TOML table")
        payload["executor_routes"] = routing
    return payload


def _parse_int(env_name: str, raw: str) -> int:
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid {env_name} value: {raw}") from exc


def _parse_float(env_name: str, raw: str) -> float:
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid {env_name} value: {raw}") from exc


def _env_value(env_name: str) -> str | None:
    value = os.getenv(env_name)
    if value is None or value == "":
        return None
    return value


def _env_overrides() -> dict[str, Any]:
    payload: dict[str, Any] = {}

    def set_value(
        key: str,
        env_name: str,
        parser: Callable[[str, str], Any] | None = None,
    ) -> None:
        raw = _env_value(env_name)
        if raw is None:
            return
        if parser is None:
            payload[key] = raw
        else:
            payload[key] = parser(env_name, raw)

    set_value("redis_dsn", "RRQ_REDIS_DSN")
    set_value("default_queue_name", "RRQ_DEFAULT_QUEUE_NAME")
    set_value("default_dlq_name", "RRQ_DEFAULT_DLQ_NAME")
    set_value("default_max_retries", "RRQ_DEFAULT_MAX_RETRIES", _parse_int)
    set_value(
        "default_job_timeout_seconds",
        "RRQ_DEFAULT_JOB_TIMEOUT_SECONDS",
        _parse_int,
    )
    set_value(
        "default_result_ttl_seconds",
        "RRQ_DEFAULT_RESULT_TTL_SECONDS",
        _parse_int,
    )
    set_value(
        "default_poll_delay_seconds",
        "RRQ_DEFAULT_POLL_DELAY_SECONDS",
        _parse_float,
    )
    set_value(
        "executor_connect_timeout_ms",
        "RRQ_EXECUTOR_CONNECT_TIMEOUT_MS",
        _parse_int,
    )
    set_value(
        "default_lock_timeout_extension_seconds",
        "RRQ_DEFAULT_LOCK_TIMEOUT_EXTENSION_SECONDS",
        _parse_int,
    )
    set_value(
        "default_unique_job_lock_ttl_seconds",
        "RRQ_DEFAULT_UNIQUE_JOB_LOCK_TTL_SECONDS",
        _parse_int,
    )
    set_value("default_executor_name", "RRQ_DEFAULT_EXECUTOR_NAME")
    set_value(
        "worker_health_check_interval_seconds",
        "RRQ_WORKER_HEALTH_CHECK_INTERVAL_SECONDS",
        _parse_float,
    )
    set_value("base_retry_delay_seconds", "RRQ_BASE_RETRY_DELAY_SECONDS", _parse_float)
    set_value("max_retry_delay_seconds", "RRQ_MAX_RETRY_DELAY_SECONDS", _parse_float)
    set_value(
        "worker_shutdown_grace_period_seconds",
        "RRQ_WORKER_SHUTDOWN_GRACE_PERIOD_SECONDS",
        _parse_float,
    )
    set_value("expected_job_ttl", "RRQ_EXPECTED_JOB_TTL", _parse_int)

    return payload


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_toml_settings(config_path: str | None) -> RRQSettings:
    """Load RRQSettings from a TOML file."""
    if DOTENV_AVAILABLE:
        dotenv_path = find_dotenv(usecwd=True)
        if dotenv_path:
            load_dotenv(dotenv_path=dotenv_path, override=False)
    path, _ = resolve_config_source(config_path)
    if path is None:
        raise FileNotFoundError(
            "RRQ config not found. Provide --config, set RRQ_CONFIG, or add rrq.toml."
        )

    with open(path, "rb") as handle:
        payload = tomllib.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("RRQ config must be a TOML table")
    payload = _normalize_toml_payload(payload)
    env_payload = _env_overrides()
    payload = _deep_merge(payload, env_payload)
    try:
        return RRQSettings.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid RRQ config: {exc}") from exc
