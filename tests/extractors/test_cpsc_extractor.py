from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import (
    AuthenticationError,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.cpsc import CpscExtractor
from src.schemas.cpsc import CpscRecord

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.saferproducts.gov/RestWebServices/Recall"

_VALID_RAW: dict[str, Any] = {
    "RecallID": 24001,
    "RecallNumber": "24-001",
    "RecallDate": "2024-01-15",
    "LastPublishDate": "2024-01-15",
    "Title": "Widget Recall",
}

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}

_FAKE_R2_PATH = "cpsc/2024-01-15/abc.json.gz"
_FAKE_WATERMARK = date(2024, 1, 14)


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, val in _REQUIRED_ENV.items():
        monkeypatch.setenv(key, val)


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> CpscExtractor:
    """CpscExtractor with mocked engine and R2 client."""
    _set_required_env(monkeypatch)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.cpsc.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return CpscExtractor(base_url=_BASE_URL, settings=settings)


# ---------------------------------------------------------------------------
# _fetch() / extract() — HTTP status code dispatch
# ---------------------------------------------------------------------------


class TestFetch:
    def test_200_returns_list(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(200, json=[_VALID_RAW]))
            with patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK):
                result = extractor.extract()
        assert result == [_VALID_RAW]

    def test_200_empty_array_returns_empty_list(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(200, json=[]))
            with patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK):
                result = extractor.extract()
        assert result == []

    def test_429_raises_rate_limit_error(self, extractor: CpscExtractor) -> None:
        # extract() raises directly — no tenacity retry wrapping here
        with respx.mock:
            respx.get(_BASE_URL).mock(
                return_value=httpx.Response(429, headers={"Retry-After": "30"})
            )
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(RateLimitError) as exc_info,
            ):
                extractor.extract()
        assert exc_info.value.retry_after == 30.0

    def test_429_uses_default_retry_after_when_header_absent(
        self, extractor: CpscExtractor
    ) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(429))
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(RateLimitError) as exc_info,
            ):
                extractor.extract()
        assert exc_info.value.retry_after == 60.0

    def test_500_raises_transient_extraction_error(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(500))
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(TransientExtractionError),
            ):
                extractor.extract()

    def test_503_raises_transient_extraction_error(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(503))
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(TransientExtractionError),
            ):
                extractor.extract()

    def test_401_raises_authentication_error(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(401))
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(AuthenticationError),
            ):
                extractor.extract()

    def test_403_raises_authentication_error(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(return_value=httpx.Response(403))
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(AuthenticationError),
            ):
                extractor.extract()

    def test_transport_error_raises_transient_extraction_error(
        self, extractor: CpscExtractor
    ) -> None:
        with respx.mock:
            respx.get(_BASE_URL).mock(side_effect=httpx.ConnectError("refused"))
            with (
                patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
                pytest.raises(TransientExtractionError, match="network error"),
            ):
                extractor.extract()

    def test_url_includes_last_publish_date_start(self, extractor: CpscExtractor) -> None:
        watermark = date(2024, 3, 5)
        with respx.mock:
            route = respx.get(_BASE_URL).mock(return_value=httpx.Response(200, json=[]))
            with patch.object(extractor, "_get_watermark", return_value=watermark):
                extractor.extract()
        assert "LastPublishDateStart=2024-03-05" in str(route.calls[0].request.url)

    def test_url_includes_format_json(self, extractor: CpscExtractor) -> None:
        with respx.mock:
            route = respx.get(_BASE_URL).mock(return_value=httpx.Response(200, json=[]))
            with patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK):
                extractor.extract()
        assert "format=json" in str(route.calls[0].request.url)

    def test_watermark_wrong_type_raises_transient_error(self, extractor: CpscExtractor) -> None:
        with (
            patch.object(extractor, "_get_watermark", return_value="2024-01-14"),
            pytest.raises(TransientExtractionError, match="unexpected type"),
        ):
            extractor.extract()

    def test_oversized_response_raises_transient_error(self, extractor: CpscExtractor) -> None:
        from src.extractors.cpsc import _MAX_INCREMENTAL_RECORDS

        oversized = [_VALID_RAW] * (_MAX_INCREMENTAL_RECORDS + 1)
        with (
            patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
            patch.object(extractor, "_fetch", return_value=oversized),
            pytest.raises(TransientExtractionError, match="exceeds guard"),
        ):
            extractor.extract()


# ---------------------------------------------------------------------------
# _get_watermark() — reads from source_watermarks; falls back to yesterday
# ---------------------------------------------------------------------------


class TestGetWatermark:
    def test_returns_stored_date_when_present(self, extractor: CpscExtractor) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = ("2024-03-01",)
        result = extractor._get_watermark(mock_conn)
        assert result == date(2024, 3, 1)

    def test_falls_back_to_yesterday_when_null(self, extractor: CpscExtractor) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        result = extractor._get_watermark(mock_conn)
        yesterday = datetime.now(UTC).date() - __import__("datetime").timedelta(days=1)
        assert result == yesterday

    def test_falls_back_to_yesterday_when_no_row(self, extractor: CpscExtractor) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        result = extractor._get_watermark(mock_conn)
        yesterday = datetime.now(UTC).date() - __import__("datetime").timedelta(days=1)
        assert result == yesterday


# ---------------------------------------------------------------------------
# land_raw() — serializes to JSON, delegates to R2, stores path
# ---------------------------------------------------------------------------


class TestLandRaw:
    def test_returns_r2_path(self, extractor: CpscExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        path = extractor.land_raw([_VALID_RAW])
        assert path == _FAKE_R2_PATH

    def test_stores_path_for_quarantine_records(self, extractor: CpscExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        extractor.land_raw([_VALID_RAW])
        assert extractor._current_landing_path == _FAKE_R2_PATH

    def test_calls_r2_with_cpsc_source_and_json_suffix(self, extractor: CpscExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        extractor.land_raw([_VALID_RAW])
        call_kwargs = mock_r2.land.call_args
        assert call_kwargs.kwargs["source"] == "cpsc"
        assert call_kwargs.kwargs["suffix"] == "json"

    def test_passes_serialized_bytes(self, extractor: CpscExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        extractor.land_raw([_VALID_RAW])
        call_kwargs = mock_r2.land.call_args
        content = call_kwargs.kwargs["content"]
        assert isinstance(content, bytes)
        import json

        deserialized = json.loads(content)
        assert deserialized == [_VALID_RAW]


# ---------------------------------------------------------------------------
# validate_records() — Pydantic parse; quarantine failures
# ---------------------------------------------------------------------------


class TestValidateRecords:
    def test_valid_records_all_pass(self, extractor: CpscExtractor) -> None:
        valid, quarantined = extractor.validate_records([_VALID_RAW])
        assert len(valid) == 1
        assert len(quarantined) == 0
        assert isinstance(valid[0], CpscRecord)

    def test_invalid_record_quarantined_with_validate_stage(self, extractor: CpscExtractor) -> None:
        bad = {**_VALID_RAW, "RecallID": "not-an-int"}
        valid, quarantined = extractor.validate_records([bad])
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "validate_records"
        assert quarantined[0].source_recall_id == "24-001"

    def test_mixed_batch_routes_correctly(self, extractor: CpscExtractor) -> None:
        bad = {"RecallNumber": "24-002", "RecallID": "oops"}
        valid, quarantined = extractor.validate_records([_VALID_RAW, bad])
        assert len(valid) == 1
        assert len(quarantined) == 1

    def test_quarantine_record_includes_landing_path(self, extractor: CpscExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        bad = {**_VALID_RAW, "ExtraField": "oops"}
        _, quarantined = extractor.validate_records([bad])
        assert quarantined[0].raw_landing_path == _FAKE_R2_PATH

    def test_unknown_field_quarantined(self, extractor: CpscExtractor) -> None:
        bad = {**_VALID_RAW, "UnexpectedNewField": "schema drift"}
        valid, quarantined = extractor.validate_records([bad])
        assert len(valid) == 0
        assert len(quarantined) == 1


# ---------------------------------------------------------------------------
# check_invariants() — null ID and date sanity
# ---------------------------------------------------------------------------


class TestCheckInvariants:
    def _make_record(self, **overrides: Any) -> CpscRecord:
        data = {**_VALID_RAW, **overrides}
        return CpscRecord.model_validate(data)

    def test_valid_records_all_pass(self, extractor: CpscExtractor) -> None:
        record = self._make_record()
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 1
        assert len(quarantined) == 0

    def test_future_recall_date_quarantined(self, extractor: CpscExtractor) -> None:
        from datetime import timedelta

        future_dt = datetime.now(UTC) + timedelta(days=5)
        record = self._make_record(RecallDate=future_dt)
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 0
        assert len(quarantined) == 1
        assert "future" in quarantined[0].failure_reason
        assert quarantined[0].failure_stage == "invariants"

    def test_quarantine_record_includes_landing_path(self, extractor: CpscExtractor) -> None:
        from datetime import timedelta

        extractor._current_landing_path = _FAKE_R2_PATH
        future_dt = datetime.now(UTC) + timedelta(days=5)
        record = self._make_record(RecallDate=future_dt)
        _, quarantined = extractor.check_invariants([record])
        assert quarantined[0].raw_landing_path == _FAKE_R2_PATH


# ---------------------------------------------------------------------------
# _capture_error_response() — logs warning when R2 raises
# ---------------------------------------------------------------------------


class TestCaptureErrorResponse:
    def test_warning_logged_when_land_error_response_raises(self, extractor: CpscExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        response = MagicMock(spec=httpx.Response)
        response.status_code = 429
        response.text = "Too Many Requests"

        import structlog.testing

        with structlog.testing.capture_logs() as captured:
            extractor._capture_error_response("https://api.example.com", response)

        assert any(e.get("event") == "cpsc.error_capture_failed" for e in captured)

    def test_does_not_raise_when_land_error_response_raises(self, extractor: CpscExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        response = MagicMock(spec=httpx.Response)
        response.status_code = 500
        response.text = "Internal Server Error"

        # Must not propagate the exception
        extractor._capture_error_response("https://api.example.com", response)


# ---------------------------------------------------------------------------
# _update_watermark() — writes new cursor to source_watermarks
# ---------------------------------------------------------------------------


class TestUpdateWatermark:
    def test_executes_update_statement(self, extractor: CpscExtractor) -> None:
        mock_conn = MagicMock()
        new_date = date(2024, 6, 1)
        extractor._update_watermark(mock_conn, new_date)
        mock_conn.execute.assert_called_once()
