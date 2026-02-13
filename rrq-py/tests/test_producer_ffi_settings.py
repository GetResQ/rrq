import pytest
from pydantic import ValidationError

from rrq.producer_ffi import ProducerConfigModel, ProducerSettingsModel


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


def test_producer_config_model_normalizes_bare_queue_name() -> None:
    config = ProducerConfigModel.model_validate(
        {
            "redis_dsn": "redis://localhost:6379/0",
            "queue_name": "custom",
        }
    )
    assert config.queue_name == "rrq:queue:custom"


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
