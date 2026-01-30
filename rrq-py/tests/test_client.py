from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from redis.asyncio import Redis as AsyncRedis

from rrq.client import RRQClient
from rrq.producer_ffi import get_producer_constants
from rrq.settings import RRQSettings
from rrq.telemetry import EnqueueSpan, Telemetry, configure, disable

_CONSTANTS = get_producer_constants()
DEFAULT_QUEUE_NAME = _CONSTANTS.default_queue_name
JOB_KEY_PREFIX = _CONSTANTS.job_key_prefix
IDEMPOTENCY_KEY_PREFIX = _CONSTANTS.idempotency_key_prefix


@pytest.fixture(scope="session")
def redis_url_for_client() -> str:
    return "redis://localhost:6379/2"  # DB 2 for client tests


@pytest_asyncio.fixture(scope="function")
async def rrq_settings_for_client(redis_url_for_client: str) -> RRQSettings:
    return RRQSettings(
        redis_dsn=redis_url_for_client, default_unique_job_lock_ttl_seconds=2
    )


@pytest_asyncio.fixture(scope="function")
async def redis_for_client_tests(
    redis_url_for_client: str,
) -> AsyncGenerator[AsyncRedis, None]:
    redis = AsyncRedis.from_url(redis_url_for_client, decode_responses=True)
    await redis.flushdb()
    yield redis
    await redis.flushdb()
    await redis.aclose()


@pytest_asyncio.fixture(scope="function")
async def rrq_client(
    rrq_settings_for_client: RRQSettings,
) -> AsyncGenerator[RRQClient, None]:
    client = RRQClient(settings=rrq_settings_for_client)
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_enqueue_job_writes_job_and_queue(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "test_function_client"
    args_ = [1, "hello"]
    kwargs_ = {"world": True}

    job_id = await rrq_client.enqueue(func_name, *args_, world=True)
    assert job_id is not None

    job_key = f"{JOB_KEY_PREFIX}{job_id}"
    job_data = await redis_for_client_tests.hgetall(job_key)
    assert job_data["function_name"] == func_name
    assert job_data["status"] == "PENDING"

    score = await redis_for_client_tests.zscore(DEFAULT_QUEUE_NAME, job_id)
    assert score is not None


@pytest.mark.asyncio
async def test_enqueue_job_with_defer_by(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "deferred_by_func"
    defer_seconds = 10
    start_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    job_id = await rrq_client.enqueue(
        func_name, _defer_by=timedelta(seconds=defer_seconds)
    )
    assert job_id is not None

    score = await redis_for_client_tests.zscore(DEFAULT_QUEUE_NAME, job_id)
    assert score is not None
    min_score = start_ms + defer_seconds * 1000 - 1000
    assert score >= min_score


@pytest.mark.asyncio
async def test_enqueue_job_with_defer_until(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "deferred_until_func"
    defer_until_dt = datetime.now(timezone.utc) + timedelta(minutes=1)

    job_id = await rrq_client.enqueue(func_name, _defer_until=defer_until_dt)
    assert job_id is not None

    score = await redis_for_client_tests.zscore(DEFAULT_QUEUE_NAME, job_id)
    assert score is not None
    expected_score_ms = int(defer_until_dt.timestamp() * 1000)
    assert score == pytest.approx(expected_score_ms, abs=100)


@pytest.mark.asyncio
async def test_enqueue_unique_key_defer_extends_idempotency_ttl(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    unique_key = "defer-unique-ttl"
    defer_seconds = 5

    job_id = await rrq_client.enqueue(
        "defer_unique_ttl_func",
        _unique_key=unique_key,
        _defer_by=timedelta(seconds=defer_seconds),
    )
    assert job_id is not None

    ttl = await redis_for_client_tests.ttl(f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}")
    assert ttl >= defer_seconds - 1


@pytest.mark.asyncio
async def test_enqueue_job_to_specific_queue(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    custom_queue_name = "rrq:queue:custom_test_queue"
    func_name = "custom_queue_func"

    job_id = await rrq_client.enqueue(func_name, _queue_name=custom_queue_name)
    assert job_id is not None

    score_default = await redis_for_client_tests.zscore(DEFAULT_QUEUE_NAME, job_id)
    assert score_default is None

    score_custom = await redis_for_client_tests.zscore(custom_queue_name, job_id)
    assert score_custom is not None


@pytest.mark.asyncio
async def test_enqueue_with_user_specified_job_id(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    user_job_id = "my-custom-job-id-123"
    func_name = "custom_id_func"

    job_id = await rrq_client.enqueue(func_name, _job_id=user_job_id)
    assert job_id == user_job_id

    job_key = f"{JOB_KEY_PREFIX}{user_job_id}"
    stored_job = await redis_for_client_tests.hgetall(job_key)
    assert stored_job["id"] == user_job_id

    with pytest.raises(ValueError):
        await rrq_client.enqueue(func_name, "new_arg", _job_id=user_job_id)


@pytest.mark.asyncio
async def test_enqueue_rejects_non_positive_timeout(rrq_client: RRQClient) -> None:
    with pytest.raises(ValueError):
        await rrq_client.enqueue("func", _job_timeout_seconds=0)
    with pytest.raises(ValueError):
        await rrq_client.enqueue("func", _job_timeout_seconds=-5)


@pytest.mark.asyncio
async def test_enqueue_invalid_timeout_does_not_set_idempotency_key(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    unique_key = "bad-timeout-idem"
    with pytest.raises(ValueError):
        await rrq_client.enqueue("func", _unique_key=unique_key, _job_timeout_seconds=0)
    stored = await redis_for_client_tests.get(f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}")
    assert stored is None


@pytest.mark.asyncio
async def test_enqueue_with_unique_key_idempotent(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    unique_key = "idempotent-op-user-555"
    func_name = "unique_func"

    job1 = await rrq_client.enqueue(func_name, _unique_key=unique_key)
    assert job1 is not None

    idempotency_value = await redis_for_client_tests.get(
        f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}"
    )
    assert idempotency_value is not None

    job2 = await rrq_client.enqueue(func_name, "different_arg", _unique_key=unique_key)
    assert job2 == job1


@pytest.mark.asyncio
async def test_enqueue_with_rate_limit_returns_none_when_limited(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "rate_limited_func"
    rate_key = "user-123"

    job1 = await rrq_client.enqueue_with_rate_limit(
        func_name, rate_limit_key=rate_key, rate_limit_seconds=5
    )
    assert job1 is not None

    job2 = await rrq_client.enqueue_with_rate_limit(
        func_name, rate_limit_key=rate_key, rate_limit_seconds=5
    )
    assert job2 is None

    score = await redis_for_client_tests.zscore(DEFAULT_QUEUE_NAME, job1)
    assert score is not None


def test_client_producer_config_uses_explicit_overrides(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    class _DummyProducer:
        def close(self) -> None:
            return None

    def _fake_from_config(config: dict[str, object]) -> _DummyProducer:
        captured.append(config)
        return _DummyProducer()

    monkeypatch.setattr("rrq.client.RustProducer.from_config", _fake_from_config)

    default_settings = RRQSettings()
    RRQClient(default_settings)
    assert captured[0] == {"redis_dsn": default_settings.redis_dsn}

    override_settings = RRQSettings(
        default_job_timeout_seconds=123,
        default_unique_job_lock_ttl_seconds=321,
    )
    RRQClient(override_settings)
    assert captured[1] == {
        "redis_dsn": override_settings.redis_dsn,
        "job_timeout_seconds": override_settings.default_job_timeout_seconds,
        "idempotency_ttl_seconds": override_settings.default_unique_job_lock_ttl_seconds,
    }


class _TestEnqueueSpan(EnqueueSpan):
    def __enter__(self):  # type: ignore[override]
        return {"traceparent": "00-abc-123-01"}


class _TestTelemetry(Telemetry):
    def enqueue_span(self, *, job_id: str, function_name: str, queue_name: str):
        return _TestEnqueueSpan()


@pytest.mark.asyncio
async def test_enqueue_stores_trace_context(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    configure(_TestTelemetry())
    try:
        job_id = await rrq_client.enqueue("test_function_trace_context")
        assert job_id is not None
        job_key = f"{JOB_KEY_PREFIX}{job_id}"
        raw = await redis_for_client_tests.hget(job_key, "trace_context")
        assert raw is not None
        assert json.loads(raw) == {"traceparent": "00-abc-123-01"}
    finally:
        disable()
