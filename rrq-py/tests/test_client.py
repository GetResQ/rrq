from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Awaitable, cast

import pytest
import pytest_asyncio
from redis.asyncio import Redis as AsyncRedis

from rrq.client import RRQClient
from rrq.job import JobStatus
from rrq.producer_ffi import get_producer_constants

_CONSTANTS = get_producer_constants()
JOB_KEY_PREFIX = _CONSTANTS.job_key_prefix
QUEUE_KEY_PREFIX = _CONSTANTS.queue_key_prefix
IDEMPOTENCY_KEY_PREFIX = _CONSTANTS.idempotency_key_prefix
TEST_QUEUE_NAME = "client_default"
TEST_QUEUE_KEY = f"{QUEUE_KEY_PREFIX}{TEST_QUEUE_NAME}"


@pytest.fixture(scope="session")
def redis_url_for_client() -> str:
    return "redis://localhost:6379/2"  # DB 2 for client tests


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
    redis_url_for_client: str,
) -> AsyncGenerator[RRQClient, None]:
    client = RRQClient(
        config={
            "redis_dsn": redis_url_for_client,
            "queue_name": TEST_QUEUE_NAME,
            "idempotency_ttl_seconds": 2,
        }
    )
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_enqueue_job_writes_job_and_queue(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "test_function_client"

    job_id = await rrq_client.enqueue(
        func_name,
        {"params": {"a": 1, "b": "hello", "world": True}},
    )

    job_key = f"{JOB_KEY_PREFIX}{job_id}"
    job_data = await cast(
        Awaitable[dict[str, str]], redis_for_client_tests.hgetall(job_key)
    )
    assert job_data["function_name"] == func_name
    assert job_data["status"] == "PENDING"
    assert job_data["queue_name"] == TEST_QUEUE_KEY

    score = await redis_for_client_tests.zscore(TEST_QUEUE_KEY, job_id)
    assert score is not None


@pytest.mark.asyncio
async def test_enqueue_job_with_defer_by(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "deferred_by_func"
    defer_seconds = 10
    start_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    job_id = await rrq_client.enqueue(
        func_name,
        {"defer_by_seconds": defer_seconds},
    )

    score = await redis_for_client_tests.zscore(TEST_QUEUE_KEY, job_id)
    assert score is not None
    min_score = start_ms + defer_seconds * 1000 - 1000
    assert score >= min_score


@pytest.mark.asyncio
async def test_enqueue_job_with_defer_until(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "deferred_until_func"
    defer_until_dt = datetime.now(timezone.utc) + timedelta(minutes=1)

    job_id = await rrq_client.enqueue(
        func_name,
        {"defer_until": defer_until_dt},
    )

    score = await redis_for_client_tests.zscore(TEST_QUEUE_KEY, job_id)
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
        {
            "unique_key": unique_key,
            "defer_by_seconds": defer_seconds,
        },
    )
    assert job_id is not None

    ttl = await redis_for_client_tests.ttl(f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}")
    assert ttl >= defer_seconds - 1


@pytest.mark.asyncio
async def test_enqueue_job_to_specific_queue(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    custom_queue_name = "custom_test_queue"
    custom_queue_key = f"{QUEUE_KEY_PREFIX}{custom_queue_name}"
    func_name = "custom_queue_func"

    job_id = await rrq_client.enqueue(func_name, {"queue_name": custom_queue_name})

    score_default = await redis_for_client_tests.zscore(TEST_QUEUE_KEY, job_id)
    assert score_default is None

    score_custom = await redis_for_client_tests.zscore(custom_queue_key, job_id)
    assert score_custom is not None


@pytest.mark.asyncio
async def test_enqueue_job_preserves_prefixed_queue_name(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    prefixed_queue_name = f"{QUEUE_KEY_PREFIX}prefixed_queue"
    func_name = "prefixed_queue_func"

    job_id = await rrq_client.enqueue(func_name, {"queue_name": prefixed_queue_name})

    job_key = f"{JOB_KEY_PREFIX}{job_id}"
    job_data = await cast(
        Awaitable[dict[str, str]], redis_for_client_tests.hgetall(job_key)
    )
    assert job_data["queue_name"] == prefixed_queue_name
    score = await redis_for_client_tests.zscore(prefixed_queue_name, job_id)
    assert score is not None


@pytest.mark.asyncio
async def test_enqueue_with_user_specified_job_id(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    user_job_id = "my-custom-job-id-123"
    func_name = "custom_id_func"

    job_id = await rrq_client.enqueue(func_name, {"job_id": user_job_id})
    assert job_id == user_job_id

    job_key = f"{JOB_KEY_PREFIX}{user_job_id}"
    stored_job = await cast(
        Awaitable[dict[str, str]], redis_for_client_tests.hgetall(job_key)
    )
    assert stored_job["id"] == user_job_id

    with pytest.raises(ValueError):
        await rrq_client.enqueue(
            func_name, {"job_id": user_job_id, "params": {"new_arg": 1}}
        )


@pytest.mark.asyncio
async def test_enqueue_rejects_non_positive_timeout(rrq_client: RRQClient) -> None:
    with pytest.raises(ValueError):
        await rrq_client.enqueue("func", {"job_timeout_seconds": 0})
    with pytest.raises(ValueError):
        await rrq_client.enqueue("func", {"job_timeout_seconds": -5})


@pytest.mark.asyncio
async def test_enqueue_invalid_timeout_does_not_set_idempotency_key(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    unique_key = "bad-timeout-idem"
    with pytest.raises(ValueError):
        await rrq_client.enqueue(
            "func",
            {"unique_key": unique_key, "job_timeout_seconds": 0},
        )
    stored = await redis_for_client_tests.get(f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}")
    assert stored is None


@pytest.mark.asyncio
async def test_enqueue_with_unique_key_idempotent(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    unique_key = "idempotent-op-user-555"
    func_name = "unique_func"

    job1 = await rrq_client.enqueue(func_name, {"unique_key": unique_key})
    assert job1 is not None

    idempotency_value = await redis_for_client_tests.get(
        f"{IDEMPOTENCY_KEY_PREFIX}{unique_key}"
    )
    assert idempotency_value is not None

    job2 = await rrq_client.enqueue(
        func_name, {"unique_key": unique_key, "params": {"different_arg": 1}}
    )
    assert job2 == job1


@pytest.mark.asyncio
async def test_enqueue_with_rate_limit_returns_none_when_limited(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    func_name = "rate_limited_func"
    rate_key = "user-123"

    job1 = await rrq_client.enqueue_with_rate_limit(
        func_name,
        {"rate_limit_key": rate_key, "rate_limit_seconds": 5},
    )
    assert job1 is not None

    job2 = await rrq_client.enqueue_with_rate_limit(
        func_name,
        {"rate_limit_key": rate_key, "rate_limit_seconds": 5},
    )
    assert job2 is None


@pytest.mark.asyncio
async def test_get_job_status_returns_result(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    job_id = "job-status-1"
    job_key = f"{JOB_KEY_PREFIX}{job_id}"
    payload = {"ok": True}
    await cast(
        Awaitable[int],
        redis_for_client_tests.hset(
            job_key,
            mapping={
                "status": "COMPLETED",
                "result": json.dumps(payload),
            },
        ),
    )

    result = await rrq_client.get_job_status(job_id)

    assert result is not None
    assert result.status == JobStatus.COMPLETED
    assert result.result == payload


@pytest.mark.asyncio
async def test_get_job_status_returns_failure(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    job_id = "job-status-2"
    job_key = f"{JOB_KEY_PREFIX}{job_id}"
    await cast(
        Awaitable[int],
        redis_for_client_tests.hset(
            job_key,
            mapping={
                "status": "FAILED",
                "last_error": "boom",
            },
        ),
    )

    result = await rrq_client.get_job_status(job_id)

    assert result is not None
    assert result.status == JobStatus.FAILED
    assert result.last_error == "boom"


def test_client_producer_config_uses_explicit_overrides(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    class _DummyProducer:
        def close(self) -> None:
            return None

    def _fake_from_config(config: dict[str, object]) -> _DummyProducer:
        captured.append(config)
        return _DummyProducer()

    monkeypatch.setattr("rrq.client.RustProducer.from_config", _fake_from_config)

    RRQClient(config={"redis_dsn": "redis://localhost:6379/0"})
    assert captured[0] == {"redis_dsn": "redis://localhost:6379/0"}

    RRQClient(
        config={
            "redis_dsn": "redis://localhost:6379/0",
            "job_timeout_seconds": 123,
            "idempotency_ttl_seconds": 321,
        }
    )
    assert captured[1] == {
        "redis_dsn": "redis://localhost:6379/0",
        "job_timeout_seconds": 123,
        "idempotency_ttl_seconds": 321,
    }


@pytest.mark.asyncio
async def test_enqueue_stores_trace_context(
    rrq_client: RRQClient, redis_for_client_tests: AsyncRedis
) -> None:
    job_id = await rrq_client.enqueue(
        "test_function_trace_context",
        {"trace_context": {"traceparent": "00-abc-123-01"}},
    )
    job_key = f"{JOB_KEY_PREFIX}{job_id}"
    raw = await cast(
        Awaitable[str | None],
        redis_for_client_tests.hget(job_key, "trace_context"),
    )
    assert raw is not None
    assert json.loads(raw) == {"traceparent": "00-abc-123-01"}
