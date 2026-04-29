"""
Integration tests for FdaExtractor — 8 scenarios mirroring test_cpsc_extractor.py.

Uses respx to mock the FDA iRES HTTP layer (POST /recalls/). R2 landing and
BronzeLoader.load are patched so tests exercise validate_records +
check_invariants for real while isolating network I/O and database I/O.

FDA response envelope: {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": [...]}
Empty result:          {"STATUSCODE": 412, "MESSAGE": "No results found"}
Auth failure:          {"STATUSCODE": 401, "MESSAGE": "Authorization denied"}

These differ from CPSC (plain JSON array) and require the respx mock to return
the full envelope. The signature= query parameter on every POST URL is ignored
by respx when matching against the base endpoint URL.
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
from src.extractors._base import (
    ExtractionAbortedError,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.fda import FdaExtractor

_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_RECALLS_URL = _BASE_URL + "/recalls/"
_FAKE_R2_PATH = "fda/2026-04-28/test.json.gz"
_WATERMARK = date(2026, 4, 27)

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
    "FDA_AUTHORIZATION_USER": "test-user",
    "FDA_AUTHORIZATION_KEY": "test-key",
}


def _valid_record(n: int = 1) -> dict[str, Any]:
    return {
        "PRODUCTID": str(200000 + n),
        "RECALLEVENTID": str(90000 + n),
        "RID": n,
        "CENTERCD": "CFSAN",
        "PRODUCTTYPESHORT": "Food",
        "EVENTLMD": "04/27/2026",
        "FIRMLEGALNAM": f"Firm {n} LLC",
    }


def _fda_response(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": records}


def _fda_empty() -> dict[str, Any]:
    return {"STATUSCODE": 412, "MESSAGE": "No results found"}


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> FdaExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.fda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return FdaExtractor(base_url=_BASE_URL, settings=settings)


def _run_with_mocks(
    extractor: FdaExtractor,
    api_response: httpx.Response,
    *,
    bronze_insert_count: int = 1,
) -> Any:
    """Run extractor.run() with all externals mocked except HTTP."""
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("src.extractors.fda.BronzeLoader") as mock_loader_cls,
        patch.object(extractor, "_update_watermark"),
    ):
        respx.post(_RECALLS_URL).mock(return_value=api_response)
        mock_loader_cls.return_value.load.return_value = bronze_insert_count
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path — batch of valid records
# ---------------------------------------------------------------------------


def test_scenario_happy_path(extractor: FdaExtractor) -> None:
    records = [_valid_record(1), _valid_record(2), _valid_record(3)]
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=_fda_response(records)),
        bronze_insert_count=3,
    )
    assert result.records_fetched == 3
    assert result.records_valid == 3
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.records_loaded == 3
    assert result.raw_landing_path == _FAKE_R2_PATH


# ---------------------------------------------------------------------------
# Scenario 2: Empty result — STATUSCODE 412, zero records
# ---------------------------------------------------------------------------


def test_scenario_empty_result(extractor: FdaExtractor) -> None:
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=_fda_empty()),
        bronze_insert_count=0,
    )
    assert result.records_fetched == 0
    assert result.records_valid == 0
    assert result.records_loaded == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 3: Malformed record — extra forbidden field quarantines that row,
# valid rows proceed. 1 of 22 bad → rate ≈ 4.5%, under 5% threshold.
# ---------------------------------------------------------------------------


def test_scenario_malformed_record_quarantined(extractor: FdaExtractor) -> None:
    valid = [_valid_record(i) for i in range(1, 22)]
    bad = {**_valid_record(22), "UNKNOWN_EXTRA_FIELD": "forbidden"}  # extra='forbid'
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=_fda_response([*valid, bad])),
        bronze_insert_count=21,
    )
    assert result.records_fetched == 22
    assert result.records_valid == 21
    assert result.records_rejected_validate == 1
    assert result.records_loaded == 21


# ---------------------------------------------------------------------------
# Scenario 4: Schema drift → ExtractionAbortedError
# A new unexpected field on every record causes 100% rejection (extra='forbid'),
# which exceeds the 5% threshold and aborts the run.
# ---------------------------------------------------------------------------


def test_scenario_schema_drift_aborts(extractor: FdaExtractor) -> None:
    drifted = [{**_valid_record(i), "NEW_FDA_FIELD": "surprise"} for i in range(1, 4)]
    with pytest.raises(ExtractionAbortedError):
        _run_with_mocks(
            extractor,
            httpx.Response(200, json=_fda_response(drifted)),
            bronze_insert_count=0,
        )


# ---------------------------------------------------------------------------
# Scenario 5: Content-hash dedup — BronzeLoader.load returns 0 on second run
# ---------------------------------------------------------------------------


def test_scenario_content_hash_dedup_second_run_inserts_zero(
    extractor: FdaExtractor,
) -> None:
    result = _run_with_mocks(
        extractor,
        httpx.Response(200, json=_fda_response([_valid_record(1)])),
        bronze_insert_count=0,
    )
    assert result.records_fetched == 1
    assert result.records_valid == 1
    assert result.records_loaded == 0


# ---------------------------------------------------------------------------
# Scenario 6: 429 rate limit — RateLimitError bubbles up through run()
# ---------------------------------------------------------------------------


def test_scenario_rate_limit_raises(extractor: FdaExtractor) -> None:
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("time.sleep"),
    ):
        respx.post(_RECALLS_URL).mock(
            return_value=httpx.Response(429, headers={"Retry-After": "60"})
        )
        with pytest.raises(RateLimitError) as exc_info:
            extractor.run()
    assert exc_info.value.retry_after == 60.0


# ---------------------------------------------------------------------------
# Scenario 7: 500 transient — TransientExtractionError bubbles up
# ---------------------------------------------------------------------------


def test_scenario_transient_500_raises(extractor: FdaExtractor) -> None:
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("time.sleep"),
    ):
        respx.post(_RECALLS_URL).mock(return_value=httpx.Response(500))
        with pytest.raises(TransientExtractionError):
            extractor.run()


# ---------------------------------------------------------------------------
# Scenario 8: Connection error — TransientExtractionError bubbles up
# ---------------------------------------------------------------------------


def test_scenario_connection_error_raises(extractor: FdaExtractor) -> None:
    with (
        respx.mock,
        patch.object(extractor, "_get_watermark", return_value=_WATERMARK),
        patch("time.sleep"),
    ):
        respx.post(_RECALLS_URL).mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(TransientExtractionError):
            extractor.run()
