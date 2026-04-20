"""
Integration tests for CpscExtractor — 9 scenarios (ADR 0015).

Uses respx to mock the CPSC HTTP layer. R2 landing and BronzeLoader.load are
patched so tests exercise validate_records + check_invariants for real while
isolating network I/O and database I/O.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import RateLimitError, TransientExtractionError
from src.extractors.cpsc import CpscExtractor

_BASE_URL = "https://www.saferproducts.gov/RestWebServices/Recall"
_FAKE_R2_PATH = "cpsc/2024-01-15/abc.json.gz"
_WATERMARK = date(2024, 1, 14)

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


def _valid_record(n: int = 1) -> dict[str, Any]:
    return {
        "RecallID": 24000 + n,
        "RecallNumber": f"24-{n:03d}",
        "RecallDate": "2024-01-15",
        "LastPublishDate": "2024-01-15",
        "Title": f"Recall {n}",
    }


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> CpscExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.cpsc.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return CpscExtractor(base_url=_BASE_URL, settings=settings)


def _run_with_mocks(
    extractor: CpscExtractor,
    api_response: httpx.Response,
    *,
    bronze_insert_count: int = 1,
) -> Any:
    """Run extractor.run() with all externals mocked except HTTP."""
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("src.extractors.cpsc.BronzeLoader") as mock_loader_cls,
        patch.object(extractor, "_update_watermark"),
    ):
        respx.get(_BASE_URL).mock(return_value=api_response)
        mock_loader_cls.return_value.load.return_value = bronze_insert_count
        # engine.begin() used in load_bronze() needs a context manager mock
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path — batch of valid records
# ---------------------------------------------------------------------------


def test_scenario_happy_path(extractor: CpscExtractor) -> None:
    records = [_valid_record(1), _valid_record(2), _valid_record(3)]
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=records),
        bronze_insert_count=3,
    )
    assert result.records_fetched == 3
    assert result.records_valid == 3
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.records_loaded == 3
    assert result.raw_landing_path == _FAKE_R2_PATH


# ---------------------------------------------------------------------------
# Scenario 2: Large result set — many records processed correctly
# ---------------------------------------------------------------------------


def test_scenario_large_result_set(extractor: CpscExtractor) -> None:
    records = [_valid_record(i) for i in range(1, 26)]  # 25 records
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=records),
        bronze_insert_count=25,
    )
    assert result.records_fetched == 25
    assert result.records_valid == 25
    assert result.records_loaded == 25


# ---------------------------------------------------------------------------
# Scenario 3: Empty result — zero records handled without error
# ---------------------------------------------------------------------------


def test_scenario_empty_result(extractor: CpscExtractor) -> None:
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=[]),
        bronze_insert_count=0,
    )
    assert result.records_fetched == 0
    assert result.records_valid == 0
    assert result.records_loaded == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 4: Small result set — single record
# ---------------------------------------------------------------------------


def test_scenario_small_result_set(extractor: CpscExtractor) -> None:
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=[_valid_record(1)]),
        bronze_insert_count=1,
    )
    assert result.records_fetched == 1
    assert result.records_loaded == 1


# ---------------------------------------------------------------------------
# Scenario 5: 429 rate limit — RateLimitError bubbles up through run()
# ---------------------------------------------------------------------------


def test_scenario_rate_limit_raises(extractor: CpscExtractor) -> None:
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("time.sleep"),  # skip tenacity waits
    ):
        respx.get(_BASE_URL).mock(return_value=httpx.Response(429, headers={"Retry-After": "60"}))
        with pytest.raises(RateLimitError) as exc_info:
            extractor.run()
    assert exc_info.value.retry_after == 60.0


# ---------------------------------------------------------------------------
# Scenario 6: 500 transient — TransientExtractionError bubbles up
# ---------------------------------------------------------------------------


def test_scenario_transient_500_raises(extractor: CpscExtractor) -> None:
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("time.sleep"),  # skip tenacity waits
    ):
        respx.get(_BASE_URL).mock(return_value=httpx.Response(500))
        with pytest.raises(TransientExtractionError):
            extractor.run()


# ---------------------------------------------------------------------------
# Scenario 7: Malformed record in response — routes to _rejected, others proceed
# ---------------------------------------------------------------------------


def test_scenario_malformed_record_quarantined(extractor: CpscExtractor) -> None:
    # 21 valid + 1 bad → rejection rate ≈ 4.5%, under the 5% threshold so run completes
    valid = [_valid_record(i) for i in range(1, 22)]
    bad = {**_valid_record(22), "RecallID": "not-an-int"}  # strict=True rejects
    records = [*valid, bad]
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=records),
        bronze_insert_count=21,
    )
    assert result.records_fetched == 22
    assert result.records_valid == 21
    assert result.records_rejected_validate == 1
    assert result.records_loaded == 21


def test_scenario_schema_drift_extra_field_aborts(extractor: CpscExtractor) -> None:
    # extra='forbid': new field from API drift → 100% rejection → ExtractionAbortedError
    # This demonstrates ADR 0014: schema drift surfaces loudly, never silent.
    from src.extractors._base import ExtractionAbortedError

    drifted = {**_valid_record(1), "NewCpscField": "surprise"}
    with pytest.raises(ExtractionAbortedError):
        _run_with_mocks(
            extractor,
            httpx.Response(200, json=[drifted]),
            bronze_insert_count=0,
        )


# ---------------------------------------------------------------------------
# Scenario 8: Content-hash dedup — running twice produces same count (BronzeLoader)
#
# The dedup logic lives in BronzeLoader (already unit-tested in test_loader.py).
# Here we verify that the extractor correctly passes records to the loader
# and trusts its return value (0 on second run = no new inserts).
# ---------------------------------------------------------------------------


def test_scenario_content_hash_dedup_second_run_inserts_zero(
    extractor: CpscExtractor,
) -> None:
    records = [_valid_record(1)]
    # Second run: BronzeLoader.load returns 0 (all hashes already present)
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=records),
        bronze_insert_count=0,
    )
    assert result.records_fetched == 1
    assert result.records_valid == 1
    assert result.records_loaded == 0  # deduped — nothing new to insert


# ---------------------------------------------------------------------------
# Scenario 9: Connection error — TransientExtractionError bubbles up
# ---------------------------------------------------------------------------


def test_scenario_connection_error_raises(extractor: CpscExtractor) -> None:
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("time.sleep"),  # skip tenacity waits
    ):
        respx.get(_BASE_URL).mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(TransientExtractionError):
            extractor.run()
