"""Pydantic Logfire integration for RRQ.

Logfire is built on OpenTelemetry. RRQ's OTEL integration will emit spans that
Logfire can collect once Logfire is configured in the application.
"""

from __future__ import annotations

from .otel import enable as _enable_otel


def enable(*, service_name: str = "rrq") -> None:
    """Enable RRQ tracing for Logfire-enabled applications.

    This function requires `logfire` to be installed and configured by the app.
    """
    try:
        import logfire  # type: ignore[import-not-found]  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "logfire is not installed; install logfire to use rrq.integrations.logfire."
        ) from e
    _enable_otel(service_name=service_name)
