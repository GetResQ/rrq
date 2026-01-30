from rrq.producer_ffi import get_producer_constants


def test_producer_constants_have_expected_shapes() -> None:
    constants = get_producer_constants()

    assert constants.default_queue_name.startswith(constants.queue_key_prefix)
    assert constants.default_max_retries > 0
    assert constants.default_job_timeout_seconds > 0
    assert constants.default_result_ttl_seconds > 0
    assert constants.default_unique_job_lock_ttl_seconds > 0

    assert constants.job_key_prefix
    assert constants.queue_key_prefix
    assert constants.idempotency_key_prefix
