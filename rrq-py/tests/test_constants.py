from rrq.producer_ffi import get_producer_constants


def test_producer_constants_have_expected_shapes() -> None:
    constants = get_producer_constants()
    assert constants.job_key_prefix
    assert constants.queue_key_prefix
    assert constants.idempotency_key_prefix
