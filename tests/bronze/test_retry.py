from __future__ import annotations

from unittest.mock import patch

import pytest

from src.bronze.retry import r2_retry, transient_retry
from src.extractors._base import (
    AuthenticationError,
    RateLimitError,
    TransientExtractionError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_failing_fn(exc_factory, *, call_counter: list[int]):
    """Return a function that increments call_counter and raises the given exception."""

    def fn():
        call_counter.append(1)
        raise exc_factory()

    return fn


def _make_succeeding_fn(*, call_counter: list[int]):
    """Return a function that increments call_counter and returns 'ok'."""

    def fn():
        call_counter.append(1)
        return "ok"

    return fn


# ---------------------------------------------------------------------------
# transient_retry
# ---------------------------------------------------------------------------


def test_transient_retry_retries_on_transient_extraction_error_up_to_5_attempts() -> None:
    calls: list[int] = []
    decorated = transient_retry(_make_failing_fn(TransientExtractionError, call_counter=calls))
    with patch("time.sleep"), pytest.raises(TransientExtractionError):
        decorated()
    assert len(calls) == 5


def test_transient_retry_retries_on_rate_limit_error_up_to_5_attempts() -> None:
    calls: list[int] = []
    decorated = transient_retry(_make_failing_fn(RateLimitError, call_counter=calls))
    with patch("time.sleep"), pytest.raises(RateLimitError):
        decorated()
    assert len(calls) == 5


def test_transient_retry_reraises_original_exception_on_exhaustion() -> None:
    decorated = transient_retry(_make_failing_fn(TransientExtractionError, call_counter=[]))
    with patch("time.sleep"), pytest.raises(TransientExtractionError):
        decorated()


def test_transient_retry_does_not_retry_on_authentication_error() -> None:
    calls: list[int] = []
    decorated = transient_retry(_make_failing_fn(AuthenticationError, call_counter=calls))
    # No time.sleep mock needed — should fail immediately without retrying.
    with pytest.raises(AuthenticationError):
        decorated()
    assert len(calls) == 1


def test_transient_retry_does_not_retry_on_generic_exception() -> None:
    calls: list[int] = []

    def fn():
        calls.append(1)
        raise ValueError("unexpected")

    decorated = transient_retry(fn)
    with pytest.raises(ValueError, match="unexpected"):
        decorated()
    assert len(calls) == 1


def test_transient_retry_returns_value_on_success() -> None:
    calls: list[int] = []
    decorated = transient_retry(_make_succeeding_fn(call_counter=calls))
    result = decorated()
    assert result == "ok"
    assert len(calls) == 1


def test_transient_retry_succeeds_after_transient_failure_then_recovery() -> None:
    attempts: list[int] = []

    def fn():
        attempts.append(1)
        if len(attempts) < 3:
            raise TransientExtractionError("flaky")
        return "recovered"

    decorated = transient_retry(fn)
    with patch("time.sleep"):
        result = decorated()
    assert result == "recovered"
    assert len(attempts) == 3


# ---------------------------------------------------------------------------
# r2_retry
# ---------------------------------------------------------------------------


def test_r2_retry_retries_on_transient_extraction_error_up_to_3_attempts() -> None:
    calls: list[int] = []
    decorated = r2_retry(_make_failing_fn(TransientExtractionError, call_counter=calls))
    with patch("time.sleep"), pytest.raises(TransientExtractionError):
        decorated()
    assert len(calls) == 3


def test_r2_retry_reraises_original_exception_on_exhaustion() -> None:
    decorated = r2_retry(_make_failing_fn(TransientExtractionError, call_counter=[]))
    with patch("time.sleep"), pytest.raises(TransientExtractionError):
        decorated()


def test_r2_retry_does_not_retry_on_rate_limit_error() -> None:
    # r2_retry only handles TransientExtractionError — RateLimitError must NOT be retried.
    calls: list[int] = []
    decorated = r2_retry(_make_failing_fn(RateLimitError, call_counter=calls))
    with pytest.raises(RateLimitError):
        decorated()
    assert len(calls) == 1


def test_r2_retry_does_not_retry_on_generic_exception() -> None:
    calls: list[int] = []

    def fn():
        calls.append(1)
        raise RuntimeError("boom")

    decorated = r2_retry(fn)
    with pytest.raises(RuntimeError, match="boom"):
        decorated()
    assert len(calls) == 1


def test_r2_retry_returns_value_on_success() -> None:
    calls: list[int] = []
    decorated = r2_retry(_make_succeeding_fn(call_counter=calls))
    result = decorated()
    assert result == "ok"
    assert len(calls) == 1


def test_r2_retry_succeeds_after_transient_failure_then_recovery() -> None:
    attempts: list[int] = []

    def fn():
        attempts.append(1)
        if len(attempts) < 2:
            raise TransientExtractionError("r2 glitch")
        return "uploaded"

    decorated = r2_retry(fn)
    with patch("time.sleep"):
        result = decorated()
    assert result == "uploaded"
    assert len(attempts) == 2
