from rrq.producer_ffi import ProducerSettingsModel


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
