import ctypes
import json

import pytest
from pydantic import ValidationError

import rrq.producer_ffi as producer_ffi
from rrq.producer_ffi import ProducerConfigModel, ProducerSettingsModel, RustProducer


def test_producer_settings_model_accepts_correlation_mappings() -> None:
    settings = ProducerSettingsModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "queue_name": "rrq:queue:default",
            "max_retries": 3,
            "job_timeout_seconds": 120,
            "result_ttl_seconds": 3600,
            "idempotency_ttl_seconds": 300,
            "correlation_mappings": {"session_id": "params.session.id"},
        }
    )
    assert settings.correlation_mappings == {"session_id": "params.session.id"}


def test_producer_settings_model_defaults_correlation_mappings() -> None:
    settings = ProducerSettingsModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "queue_name": "rrq:queue:default",
            "max_retries": 3,
            "job_timeout_seconds": 120,
            "result_ttl_seconds": 3600,
            "idempotency_ttl_seconds": 300,
        }
    )
    assert settings.correlation_mappings == {}


def test_producer_config_model_accepts_correlation_mappings() -> None:
    config = ProducerConfigModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "correlation_mappings": {"session_id": "params.session.id"},
        }
    )
    assert config.correlation_mappings == {"session_id": "params.session.id"}


def test_producer_config_model_defaults_correlation_mappings() -> None:
    config = ProducerConfigModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
        }
    )
    assert config.correlation_mappings == {}


def test_producer_config_model_preserves_bare_queue_name() -> None:
    config = ProducerConfigModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "queue_name": "custom",
        }
    )
    assert config.queue_name == "custom"


def test_producer_config_model_preserves_prefixed_queue_name() -> None:
    config = ProducerConfigModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "queue_name": "rrq:queue:custom",
        }
    )
    assert config.queue_name == "rrq:queue:custom"


def test_producer_config_model_rejects_blank_queue_name() -> None:
    with pytest.raises(ValidationError):
        ProducerConfigModel.model_validate(
            {
                "redis_dsn": "redis://localhost:6379/0",
                "queue_name": "   ",
            }
        )


def test_producer_config_model_validation_does_not_load_ffi(monkeypatch) -> None:
    def _raise_if_called() -> None:
        raise AssertionError("validation must not load FFI")

    monkeypatch.setattr(producer_ffi, "_get_library", _raise_if_called)
    config = ProducerConfigModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "queue_name": "custom",
        }
    )
    assert config.queue_name == "custom"


def test_rust_producer_wait_for_completion_calls_ffi(monkeypatch) -> None:
    class _FakeLibrary:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []
            self._response_buf: ctypes.Array[ctypes.c_char] | None = None

        def rrq_producer_wait_for_completion(self, handle, payload, error_out):
            assert handle == 123
            assert error_out is not None
            del error_out
            payload_json = payload.decode("utf-8")
            self.requests.append(json.loads(payload_json))
            response = {
                "completed": True,
                "job": {
                    "status": "COMPLETED",
                    "result": {"ok": True},
                    "last_error": None,
                },
            }
            self._response_buf = ctypes.create_string_buffer(
                json.dumps(response).encode("utf-8")
            )
            return ctypes.cast(self._response_buf, ctypes.c_void_p)

        def rrq_string_free(self, ptr) -> None:
            del ptr

    fake_lib = _FakeLibrary()
    monkeypatch.setattr(producer_ffi, "_get_library", lambda: fake_lib)

    producer = RustProducer(123)
    response = producer.wait_for_completion("job-wait-1")

    assert fake_lib.requests == [
        {
            "job_id": "job-wait-1",
            "timeout_seconds": 30.0,
            "block_interval_seconds": 0.25,
        }
    ]
    assert response.completed is True
    assert response.job is not None
    assert response.job.status == "COMPLETED"
    assert response.job.result == {"ok": True}


def test_rust_producer_wait_for_completion_requires_symbol(monkeypatch) -> None:
    class _FakeLibrary:
        def rrq_string_free(self, ptr) -> None:
            del ptr

    monkeypatch.setattr(producer_ffi, "_get_library", lambda: _FakeLibrary())
    producer = RustProducer(123)

    with pytest.raises(
        producer_ffi.RustProducerError, match="rrq_producer_wait_for_completion"
    ):
        producer.wait_for_completion("job-wait-2")
