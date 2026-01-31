import re

import rrq
from rrq.producer_ffi import get_producer_constants, get_producer_version


def test_producer_constants_have_expected_shapes() -> None:
    constants = get_producer_constants()
    assert constants.job_key_prefix
    assert constants.queue_key_prefix
    assert constants.idempotency_key_prefix


def test_producer_version_has_semver_format() -> None:
    version = get_producer_version()
    assert re.match(r"^\d+\.\d+\.\d+", version)


def test_package_version_is_exported() -> None:
    assert hasattr(rrq, "__version__")
    assert re.match(r"^\d+\.\d+\.\d+", rrq.__version__)
