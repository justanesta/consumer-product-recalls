from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import (
    AuthenticationError,
    QuarantineRecord,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.usda_establishment import (
    _MAX_TOTAL_RECORDS,
    UsdaEstablishmentExtractor,
)

_BASE_URL = "https://www.fsis.usda.gov/fsis/api/establishments/v/1"
_FAKE_R2_PATH = "usda_establishments/2026-05-01/abc.json"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}

_VALID_RAW: dict[str, Any] = {
    "establishment_id": "6163082",
    "establishment_name": "CS Beef Packers, LLC",
    "establishment_number": "M630",
    "address": "123 Main St",
    "city": "Kuna",
    "state": "ID",
    "zip": "83634",
    "LatestMPIActiveDate": "2026-04-27",
    "status_regulated_est": "",
    "activities": ["Meat Processing"],
    "dbas": [],
}


def _make_response(
    status_code: int,
    *,
    json_body: Any = None,
    headers: dict[str, str] | None = None,
    text: str = "",
) -> httpx.Response:
    request = httpx.Request("GET", _BASE_URL)
    if json_body is not None:
        import json as _json

        return httpx.Response(
            status_code,
            request=request,
            content=_json.dumps(json_body).encode("utf-8"),
            headers={"content-type": "application/json", **(headers or {})},
        )
    return httpx.Response(
        status_code,
        request=request,
        content=text.encode("utf-8"),
        headers=headers or {},
    )


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> UsdaEstablishmentExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda_establishment.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return UsdaEstablishmentExtractor(base_url=_BASE_URL, settings=settings)


# ---------------------------------------------------------------------------
# _fetch — status code routing
# ---------------------------------------------------------------------------


class TestFetch:
    def test_200_returns_records(self, extractor: UsdaEstablishmentExtractor) -> None:
        response = _make_response(200, json_body=[_VALID_RAW])
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records = extractor._fetch()
        assert records == [_VALID_RAW]

    def test_200_non_array_returns_empty(self, extractor: UsdaEstablishmentExtractor) -> None:
        # Defensive: if the API ever returns an envelope shape, don't crash.
        response = _make_response(200, json_body={"unexpected": "envelope"})
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            assert extractor._fetch() == []

    def test_429_raises_rate_limit(self, extractor: UsdaEstablishmentExtractor) -> None:
        response = _make_response(429, text="rate limited", headers={"Retry-After": "30"})
        with (
            patch("httpx.Client") as mock_client,
            patch.object(extractor, "_capture_error_response"),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(RateLimitError) as exc_info:
                extractor._fetch()
        assert exc_info.value.retry_after == 30.0

    def test_401_raises_authentication_error(self, extractor: UsdaEstablishmentExtractor) -> None:
        response = _make_response(401, text="unauthorized")
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(AuthenticationError):
                extractor._fetch()

    def test_5xx_raises_transient(self, extractor: UsdaEstablishmentExtractor) -> None:
        response = _make_response(503, text="bad gateway")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(extractor, "_capture_error_response"),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(TransientExtractionError):
                extractor._fetch()

    def test_transport_error_wrapped(self, extractor: UsdaEstablishmentExtractor) -> None:
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.ConnectError(
                "dns fail"
            )
            with pytest.raises(TransientExtractionError, match="network error"):
                extractor._fetch()


# ---------------------------------------------------------------------------
# extract — count guard
# ---------------------------------------------------------------------------


class TestExtractGuard:
    def test_above_guard_raises(self, extractor: UsdaEstablishmentExtractor) -> None:
        oversized = [_VALID_RAW] * (_MAX_TOTAL_RECORDS + 1)
        with (
            patch.object(extractor, "_fetch", return_value=oversized),
            pytest.raises(TransientExtractionError, match="exceeds guard"),
        ):
            extractor.extract()

    def test_at_guard_passes(self, extractor: UsdaEstablishmentExtractor) -> None:
        sized = [_VALID_RAW] * _MAX_TOTAL_RECORDS
        with patch.object(extractor, "_fetch", return_value=sized):
            assert len(extractor.extract()) == _MAX_TOTAL_RECORDS


# ---------------------------------------------------------------------------
# validate_records — happy path + quarantine
# ---------------------------------------------------------------------------


class TestValidateRecords:
    def test_happy_path(self, extractor: UsdaEstablishmentExtractor) -> None:
        valid, quarantined = extractor.validate_records([_VALID_RAW])
        assert len(valid) == 1
        assert len(quarantined) == 0
        assert valid[0].source_recall_id == "6163082"

    def test_invalid_record_quarantined(self, extractor: UsdaEstablishmentExtractor) -> None:
        bad = {**_VALID_RAW}
        del bad["establishment_name"]  # required
        valid, quarantined = extractor.validate_records([bad])
        assert valid == []
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "validate_records"
        assert quarantined[0].source_recall_id == "6163082"

    def test_unknown_id_marked_in_quarantine(self, extractor: UsdaEstablishmentExtractor) -> None:
        bad = {"foo": "bar"}  # missing everything including establishment_id
        valid, quarantined = extractor.validate_records([bad])
        assert valid == []
        assert quarantined[0].source_recall_id == "<unknown>"

    def test_false_sentinel_geolocation_normalized(
        self, extractor: UsdaEstablishmentExtractor
    ) -> None:
        # End-to-end check that the schema's BeforeValidator runs through
        # the extractor's validate_records path.
        with_false = {**_VALID_RAW, "geolocation": False, "county": False}
        valid, _ = extractor.validate_records([with_false])
        assert valid[0].geolocation is None
        assert valid[0].county is None


# ---------------------------------------------------------------------------
# check_invariants — only null-id check
# ---------------------------------------------------------------------------


class TestCheckInvariants:
    def test_passing_record(self, extractor: UsdaEstablishmentExtractor) -> None:
        from src.schemas.usda_establishment import UsdaFsisEstablishment

        record = UsdaFsisEstablishment.model_validate(_VALID_RAW)
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 1
        assert len(quarantined) == 0

    def test_no_date_sanity_check(self, extractor: UsdaEstablishmentExtractor) -> None:
        # latest_mpi_active_date is administrative, not a publication date —
        # an FSIS re-baseline could legitimately reset it. The extractor
        # explicitly does NOT apply check_date_sanity. Use an obviously-future
        # date to confirm the record still passes.
        from src.schemas.usda_establishment import UsdaFsisEstablishment

        future = {**_VALID_RAW, "LatestMPIActiveDate": "2099-01-01"}
        record = UsdaFsisEstablishment.model_validate(future)
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 1
        assert len(quarantined) == 0

    def test_null_source_id_routes_to_quarantine(
        self, extractor: UsdaEstablishmentExtractor
    ) -> None:
        # Construct a real record then forcibly empty its source_recall_id so
        # check_null_source_id flags it. Pydantic strict-mode would reject ""
        # at validate_records time, so we bypass that to exercise the
        # invariants-quarantine path directly.
        from src.schemas.usda_establishment import UsdaFsisEstablishment

        record = UsdaFsisEstablishment.model_validate(_VALID_RAW)
        # Use Pydantic v2's model_copy to override the field without re-validating.
        bad_record = record.model_copy(update={"source_recall_id": ""})
        extractor._current_landing_path = _FAKE_R2_PATH
        passing, quarantined = extractor.check_invariants([bad_record])
        assert len(passing) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "invariants"
        assert quarantined[0].raw_landing_path == _FAKE_R2_PATH


# ---------------------------------------------------------------------------
# _capture_error_response — invoked on 4xx/5xx paths in _fetch
# ---------------------------------------------------------------------------


class TestCaptureErrorResponse:
    def test_capture_invokes_r2_land_error_response(
        self, extractor: UsdaEstablishmentExtractor
    ) -> None:
        # Build a real httpx.Response (so .request.method / .request.url / .text
        # / .headers are real) and confirm _capture_error_response forwards the
        # right kwargs to R2LandingClient.land_error_response.
        request = httpx.Request("GET", _BASE_URL)
        response = httpx.Response(
            500,
            request=request,
            content=b"upstream is down",
            headers={"content-type": "text/plain"},
        )
        extractor._capture_error_response(response)
        extractor._r2_client.land_error_response.assert_called_once()  # type: ignore[attr-defined]
        kwargs = extractor._r2_client.land_error_response.call_args.kwargs  # type: ignore[attr-defined]
        assert kwargs["source"] == "usda_establishments"
        assert kwargs["status_code"] == 500
        assert kwargs["response_body"] == "upstream is down"

    def test_capture_swallows_r2_failure(self, extractor: UsdaEstablishmentExtractor) -> None:
        # If R2 itself is down, _capture_error_response must not propagate —
        # error capture is best-effort, the original 5xx is what callers care about.
        request = httpx.Request("GET", _BASE_URL)
        response = httpx.Response(503, request=request, content=b"")
        extractor._r2_client.land_error_response.side_effect = RuntimeError("R2 down")  # type: ignore[attr-defined]
        # No exception expected; the broad except/logger.warning swallows it.
        extractor._capture_error_response(response)


# ---------------------------------------------------------------------------
# _record_run — broad-except diagnostic path (FK violation, etc.)
# ---------------------------------------------------------------------------


class TestRecordRun:
    def test_db_failure_is_logged_not_raised(self, extractor: UsdaEstablishmentExtractor) -> None:
        # The bronze write has already committed by the time _record_run is
        # called; a failure here (e.g., missing source_watermarks FK row, as
        # seen on Phase 5b.2 first extraction) must not propagate. The fix
        # added on 2026-05-01 captures the exception type + message so the
        # cause is diagnosable from the structured-log output.
        from datetime import UTC, datetime

        # Make conn.execute raise to simulate the FK violation.
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = RuntimeError("FK violation")
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Should not raise — the broad except in _record_run swallows it.
        extractor._record_run(
            run_id="test-run-id",
            started_at=datetime.now(UTC),
            status="success",
            result=None,
            error_message=None,
        )


# ---------------------------------------------------------------------------
# Smoke test for QuarantineRecord shape (loader contract)
# ---------------------------------------------------------------------------


def test_quarantine_record_has_landing_path(
    extractor: UsdaEstablishmentExtractor,
) -> None:
    extractor._current_landing_path = _FAKE_R2_PATH
    bad = {**_VALID_RAW}
    del bad["establishment_name"]
    _, quarantined = extractor.validate_records([bad])
    q: QuarantineRecord = quarantined[0]
    assert q.raw_landing_path == _FAKE_R2_PATH
