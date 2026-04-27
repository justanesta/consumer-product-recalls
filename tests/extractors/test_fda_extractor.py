from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import sqlalchemy as sa
import structlog.testing

from src.config.settings import Settings
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.fda import FdaDeepRescanLoader, FdaExtractor
from src.schemas.fda import FdaRecord

_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_FAKE_R2_PATH = "fda/2026-04-24/abc.json.gz"
_FAKE_WATERMARK = date(2026, 4, 23)

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
    "FDA_AUTHORIZATION_USER": "test-user",
    "FDA_AUTHORIZATION_KEY": "test-key",
}

_VALID_RAW: dict[str, Any] = {
    "PRODUCTID": "219875",
    "RECALLEVENTID": "98815",
    "RID": 1,
    "CENTERCD": "CFSAN",
    "PRODUCTTYPESHORT": "Food",
    "EVENTLMD": "04/24/2026",
    "FIRMLEGALNAM": "Acme Foods LLC",
}


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> FdaExtractor:
    """FdaExtractor with mocked engine and R2 client."""
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


@pytest.fixture
def deep_rescan(monkeypatch: pytest.MonkeyPatch) -> FdaDeepRescanLoader:
    """FdaDeepRescanLoader with mocked engine and R2 client."""
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
        return FdaDeepRescanLoader(base_url=_BASE_URL, settings=settings)


# ---------------------------------------------------------------------------
# _parse_bulk_post_response — STATUSCODE routing
# ---------------------------------------------------------------------------


class TestParseBulkPostResponse:
    def test_statuscode_400_returns_result_list(self, extractor: FdaExtractor) -> None:
        body = {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": [_VALID_RAW]}
        result = extractor._parse_bulk_post_response(body, "http://example.com")
        assert result == [_VALID_RAW]

    def test_statuscode_412_returns_empty_list(self, extractor: FdaExtractor) -> None:
        body = {"STATUSCODE": 412, "MESSAGE": "No results found"}
        result = extractor._parse_bulk_post_response(body, "http://example.com")
        assert result == []

    def test_statuscode_401_raises_authentication_error(self, extractor: FdaExtractor) -> None:
        body = {"STATUSCODE": 401, "MESSAGE": "Authorization denied"}
        with pytest.raises(AuthenticationError):
            extractor._parse_bulk_post_response(body, "http://example.com")

    def test_other_statuscode_raises_extraction_error(self, extractor: FdaExtractor) -> None:
        body = {"STATUSCODE": 406, "MESSAGE": "Invalid displaycolumns"}
        with pytest.raises(ExtractionError):
            extractor._parse_bulk_post_response(body, "http://example.com")

    def test_result_not_list_raises_transient_error(self, extractor: FdaExtractor) -> None:
        body = {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": {"not": "a list"}}
        with pytest.raises(TransientExtractionError):
            extractor._parse_bulk_post_response(body, "http://example.com")


# ---------------------------------------------------------------------------
# extract() — watermark guard and count guard
# ---------------------------------------------------------------------------


class TestExtract:
    def test_extract_returns_records_from_fetch_page(self, extractor: FdaExtractor) -> None:
        with (
            patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
            patch.object(extractor, "_fetch_page", return_value=[_VALID_RAW]),
        ):
            result = extractor.extract()
        assert result == [_VALID_RAW]

    def test_extract_aborts_on_count_guard(self, extractor: FdaExtractor) -> None:
        # Mock _paginate — NOT _fetch_page — to avoid an infinite pagination loop.
        # Mocking _fetch_page with >PAGE_SIZE items keeps len(page) >= PAGE_SIZE so
        # _paginate never breaks, accumulating memory until OOM.
        oversized = [_VALID_RAW] * 5_001
        with (
            patch.object(extractor, "_get_watermark", return_value=_FAKE_WATERMARK),
            patch.object(extractor, "_paginate", return_value=oversized),
            pytest.raises(TransientExtractionError, match="exceeds guard"),
        ):
            extractor.extract()

    def test_extract_raises_on_bad_watermark_type(self, extractor: FdaExtractor) -> None:
        with (
            patch.object(extractor, "_get_watermark", return_value="not-a-date"),
            pytest.raises(TransientExtractionError, match="unexpected type"),
        ):
            extractor.extract()


# ---------------------------------------------------------------------------
# land_raw()
# ---------------------------------------------------------------------------


class TestLandRaw:
    def test_returns_r2_path(self, extractor: FdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        assert extractor.land_raw([_VALID_RAW]) == _FAKE_R2_PATH

    def test_stores_current_landing_path(self, extractor: FdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        extractor.land_raw([_VALID_RAW])
        assert extractor._current_landing_path == _FAKE_R2_PATH

    def test_calls_r2_with_fda_source_and_json_suffix(self, extractor: FdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        extractor.land_raw([_VALID_RAW])
        kwargs = mock_r2.land.call_args.kwargs
        assert kwargs["source"] == "fda"
        assert kwargs["suffix"] == "json"

    def test_passes_serialized_bytes(self, extractor: FdaExtractor) -> None:
        import json

        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        extractor.land_raw([_VALID_RAW])
        content = mock_r2.land.call_args.kwargs["content"]
        assert isinstance(content, bytes)
        assert json.loads(content) == [_VALID_RAW]


# ---------------------------------------------------------------------------
# validate_records
# ---------------------------------------------------------------------------


class TestValidateRecords:
    def test_valid_record_passes(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        valid, quarantined = extractor.validate_records([_VALID_RAW])
        assert len(valid) == 1
        assert len(quarantined) == 0

    def test_invalid_record_quarantined(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        bad = {**_VALID_RAW, "UNKNOWN_KEY": "value"}
        valid, quarantined = extractor.validate_records([bad])
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "validate_records"

    def test_mixed_records(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        bad = {**_VALID_RAW, "EXTRA": "value"}
        valid2 = {**_VALID_RAW, "PRODUCTID": "111"}
        valid, quarantined = extractor.validate_records([_VALID_RAW, bad, valid2])
        assert len(valid) == 2
        assert len(quarantined) == 1

    def test_quarantine_record_includes_landing_path(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        bad = {**_VALID_RAW, "EXTRA": "value"}
        _, quarantined = extractor.validate_records([bad])
        assert quarantined[0].raw_landing_path == _FAKE_R2_PATH


# ---------------------------------------------------------------------------
# check_invariants
# ---------------------------------------------------------------------------


class TestCheckInvariants:
    def test_valid_record_passes(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        record = FdaRecord.model_validate(_VALID_RAW)
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 1
        assert len(quarantined) == 0

    def test_null_source_recall_id_quarantined(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        record = FdaRecord.model_validate(_VALID_RAW)
        object.__setattr__(record, "source_recall_id", "")
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "invariants"

    def test_future_initiation_date_quarantined(self, extractor: FdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        row = {**_VALID_RAW, "RECALLINITIATIONDT": "12/31/2099"}
        record = FdaRecord.model_validate(row)
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 0
        assert len(quarantined) == 1


# ---------------------------------------------------------------------------
# _get_watermark() and _update_watermark()
# ---------------------------------------------------------------------------


class TestGetWatermark:
    def test_returns_stored_date_when_present(self, extractor: FdaExtractor) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = ("2026-04-20",)
        assert extractor._get_watermark(mock_conn) == date(2026, 4, 20)

    def test_falls_back_to_yesterday_when_no_row(self, extractor: FdaExtractor) -> None:
        import datetime as dt_module

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        result = extractor._get_watermark(mock_conn)
        yesterday = datetime.now(UTC).date() - dt_module.timedelta(days=1)
        assert result == yesterday

    def test_falls_back_to_yesterday_when_cursor_null(self, extractor: FdaExtractor) -> None:
        import datetime as dt_module

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        result = extractor._get_watermark(mock_conn)
        yesterday = datetime.now(UTC).date() - dt_module.timedelta(days=1)
        assert result == yesterday


class TestUpdateWatermark:
    def test_executes_update_statement(self, extractor: FdaExtractor) -> None:
        mock_conn = MagicMock()
        extractor._update_watermark(mock_conn, date(2026, 4, 24))
        mock_conn.execute.assert_called_once()


# ---------------------------------------------------------------------------
# _capture_error_response()
# ---------------------------------------------------------------------------


class TestCaptureErrorResponse:
    def test_does_not_raise_when_r2_raises(self, extractor: FdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        response = MagicMock(spec=httpx.Response)
        response.status_code = 500
        response.text = "Internal Server Error"
        extractor._capture_error_response("https://example.com", response)  # must not raise

    def test_logs_warning_when_r2_raises(self, extractor: FdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        response = MagicMock(spec=httpx.Response)
        response.status_code = 429
        response.text = "Too Many Requests"
        with structlog.testing.capture_logs() as captured:
            extractor._capture_error_response("https://example.com", response)
        assert any(e.get("event") == "fda.error_capture_failed" for e in captured)


# ---------------------------------------------------------------------------
# load_bronze — watermark update
# ---------------------------------------------------------------------------


class TestLoadBronze:
    def test_load_updates_watermark_to_max_event_lmd(self, extractor: FdaExtractor) -> None:
        r1 = FdaRecord.model_validate({**_VALID_RAW, "EVENTLMD": "04/23/2026"})
        r2 = FdaRecord.model_validate({**_VALID_RAW, "EVENTLMD": "04/24/2026", "PRODUCTID": "999"})

        mock_conn = MagicMock()
        with (
            patch("src.extractors.fda.BronzeLoader") as mock_loader_cls,
            patch.object(extractor, "_update_watermark") as mock_update,
        ):
            mock_loader_cls.return_value.load.return_value = 2
            extractor._engine.begin.return_value.__enter__ = lambda _: mock_conn  # type: ignore[attr-defined]
            extractor._engine.begin.return_value.__exit__ = MagicMock(return_value=False)  # type: ignore[attr-defined]
            extractor.load_bronze([r1, r2], [], _FAKE_R2_PATH)

        mock_update.assert_called_once_with(mock_conn, date(2026, 4, 24))

    def test_load_skips_watermark_when_no_records(self, extractor: FdaExtractor) -> None:
        with (
            patch("src.extractors.fda.BronzeLoader") as mock_loader_cls,
            patch.object(extractor, "_update_watermark") as mock_update,
        ):
            mock_loader_cls.return_value.load.return_value = 0
            extractor._engine.begin.return_value.__enter__ = lambda _: MagicMock()  # type: ignore[attr-defined]
            extractor._engine.begin.return_value.__exit__ = MagicMock(return_value=False)  # type: ignore[attr-defined]
            extractor.load_bronze([], [], _FAKE_R2_PATH)

        mock_update.assert_not_called()


# ---------------------------------------------------------------------------
# FdaDeepRescanLoader
# ---------------------------------------------------------------------------


class TestFdaDeepRescanLoader:
    def test_extract_uses_start_and_end_date(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan.set_date_range(date(2026, 1, 1), date(2026, 4, 26))
        with patch.object(deep_rescan, "_fetch_page", return_value=[_VALID_RAW]) as mock_fetch:
            result = deep_rescan.extract()
        assert result == [_VALID_RAW]
        filter_arg = mock_fetch.call_args.kwargs["filter_str"]
        assert "01/01/2026" in filter_arg
        assert "04/26/2026" in filter_arg

    def test_extract_sort_is_recalleventid_asc(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan.set_date_range(date(2026, 1, 1), date(2026, 4, 26))
        with patch.object(deep_rescan, "_fetch_page", return_value=[]) as mock_fetch:
            deep_rescan.extract()
        kwargs = mock_fetch.call_args.kwargs
        assert kwargs["sort"] == "recalleventid"
        assert kwargs["sortorder"] == "asc"

    def test_load_bronze_does_not_call_update_watermark(
        self, deep_rescan: FdaDeepRescanLoader
    ) -> None:
        # FdaDeepRescanLoader.load_bronze does not own the watermark cursor.
        # Verify no direct conn.execute calls happen outside BronzeLoader (which is mocked).
        r = FdaRecord.model_validate(_VALID_RAW)
        mock_conn = MagicMock()
        with patch("src.extractors.fda.BronzeLoader") as mock_loader_cls:
            mock_loader_cls.return_value.load.return_value = 1
            deep_rescan._engine.begin.return_value.__enter__ = lambda _: mock_conn  # type: ignore[attr-defined]
            deep_rescan._engine.begin.return_value.__exit__ = MagicMock(return_value=False)  # type: ignore[attr-defined]
            deep_rescan.load_bronze([r], [], _FAKE_R2_PATH)

        mock_conn.execute.assert_not_called()

    def test_statuscode_412_empty_result(self, deep_rescan: FdaDeepRescanLoader) -> None:
        body = {"STATUSCODE": 412, "MESSAGE": "No results found"}
        result = deep_rescan._parse_bulk_post_response(body, "http://example.com")
        assert result == []

    def test_auth_missing_raises(self, deep_rescan: FdaDeepRescanLoader) -> None:
        # Clear auth fields directly in Settings __dict__ — avoids .env file fallback.
        # monkeypatch.delenv can't reliably clear Optional[SecretStr] fields when a .env
        # file is present (pydantic-settings falls back to the file after env removal).
        object.__setattr__(deep_rescan.settings, "fda_authorization_user", None)
        object.__setattr__(deep_rescan.settings, "fda_authorization_key", None)
        with pytest.raises(AuthenticationError):
            deep_rescan._auth_headers()

    def test_rate_limit_propagates(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan.set_date_range(date(2026, 1, 1), date(2026, 1, 31))
        with (
            patch.object(deep_rescan, "_fetch_page", side_effect=RateLimitError(retry_after=10.0)),
            pytest.raises(RateLimitError),
        ):
            deep_rescan.extract()

    def test_land_raw_returns_r2_path(self, deep_rescan: FdaDeepRescanLoader) -> None:
        mock_r2: MagicMock = deep_rescan._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        assert deep_rescan.land_raw([_VALID_RAW]) == _FAKE_R2_PATH

    def test_land_raw_stores_landing_path(self, deep_rescan: FdaDeepRescanLoader) -> None:
        mock_r2: MagicMock = deep_rescan._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        deep_rescan.land_raw([_VALID_RAW])
        assert deep_rescan._current_landing_path == _FAKE_R2_PATH

    def test_validate_records_valid_passes(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan._current_landing_path = _FAKE_R2_PATH
        valid, quarantined = deep_rescan.validate_records([_VALID_RAW])
        assert len(valid) == 1
        assert len(quarantined) == 0

    def test_validate_records_invalid_quarantined(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan._current_landing_path = _FAKE_R2_PATH
        bad = {**_VALID_RAW, "UNKNOWN_KEY": "value"}
        valid, quarantined = deep_rescan.validate_records([bad])
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "validate_records"

    def test_check_invariants_valid_passes(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan._current_landing_path = _FAKE_R2_PATH
        record = FdaRecord.model_validate(_VALID_RAW)
        passing, quarantined = deep_rescan.check_invariants([record])
        assert len(passing) == 1
        assert len(quarantined) == 0

    def test_check_invariants_null_id_quarantined(self, deep_rescan: FdaDeepRescanLoader) -> None:
        deep_rescan._current_landing_path = _FAKE_R2_PATH
        record = FdaRecord.model_validate(_VALID_RAW)
        object.__setattr__(record, "source_recall_id", "")
        passing, quarantined = deep_rescan.check_invariants([record])
        assert len(passing) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "invariants"

    def test_paginate_multi_page_accumulates(self, deep_rescan: FdaDeepRescanLoader) -> None:
        page1 = [_VALID_RAW] * 5_000
        page2 = [{**_VALID_RAW, "PRODUCTID": "999"}]
        with patch.object(deep_rescan, "_fetch_page", side_effect=[page1, page2]):
            result = deep_rescan._paginate("[{}]")
        assert len(result) == 5_001

    def test_fetch_page_200_returns_records(self, deep_rescan: FdaDeepRescanLoader) -> None:
        body = {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": [_VALID_RAW]}
        with respx.mock:
            respx.post(_BASE_URL + "/recalls/").mock(return_value=httpx.Response(200, json=body))
            result = deep_rescan._fetch_page(filter_str="[{}]")
        assert result == [_VALID_RAW]

    def test_fetch_page_429_raises_rate_limit(self, deep_rescan: FdaDeepRescanLoader) -> None:
        with respx.mock:
            respx.post(_BASE_URL + "/recalls/").mock(
                return_value=httpx.Response(429, headers={"Retry-After": "30"})
            )
            with pytest.raises(RateLimitError) as exc_info:
                deep_rescan._fetch_page(filter_str="[{}]")
        assert exc_info.value.retry_after == 30.0

    def test_fetch_page_500_raises_transient(self, deep_rescan: FdaDeepRescanLoader) -> None:
        with respx.mock:
            respx.post(_BASE_URL + "/recalls/").mock(return_value=httpx.Response(500))
            with pytest.raises(TransientExtractionError, match="FDA HTTP 500"):
                deep_rescan._fetch_page(filter_str="[{}]")

    def test_fetch_page_transport_error_raises(self, deep_rescan: FdaDeepRescanLoader) -> None:
        with respx.mock:
            respx.post(_BASE_URL + "/recalls/").mock(side_effect=httpx.ConnectError("down"))
            with pytest.raises(TransientExtractionError, match="FDA network error"):
                deep_rescan._fetch_page(filter_str="[{}]")

    def test_parse_bulk_post_response_400_success(self, deep_rescan: FdaDeepRescanLoader) -> None:
        body = {"STATUSCODE": 400, "RESULT": [_VALID_RAW]}
        result = deep_rescan._parse_bulk_post_response(body, "http://example.com")
        assert result == [_VALID_RAW]

    def test_parse_bulk_post_response_401_raises(self, deep_rescan: FdaDeepRescanLoader) -> None:
        body = {"STATUSCODE": 401, "MESSAGE": "denied"}
        with pytest.raises(AuthenticationError):
            deep_rescan._parse_bulk_post_response(body, "http://example.com")

    def test_parse_bulk_post_response_other_raises(self, deep_rescan: FdaDeepRescanLoader) -> None:
        body = {"STATUSCODE": 406, "MESSAGE": "bad params"}
        with pytest.raises(ExtractionError):
            deep_rescan._parse_bulk_post_response(body, "http://example.com")

    def test_auth_headers_success(self, deep_rescan: FdaDeepRescanLoader) -> None:
        headers = deep_rescan._auth_headers()
        assert headers["Authorization-User"] == "test-user"
        assert headers["Authorization-Key"] == "test-key"

    def test_capture_error_response_does_not_raise(self, deep_rescan: FdaDeepRescanLoader) -> None:
        mock_r2: MagicMock = deep_rescan._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        response = MagicMock(spec=httpx.Response)
        response.status_code = 500
        response.text = "error"
        deep_rescan._capture_error_response("https://example.com", response)  # must not raise

    def test_check_invariants_future_date_quarantined(
        self, deep_rescan: FdaDeepRescanLoader
    ) -> None:
        deep_rescan._current_landing_path = _FAKE_R2_PATH
        row = {**_VALID_RAW, "RECALLINITIATIONDT": "12/31/2099"}
        record = FdaRecord.model_validate(row)
        passing, quarantined = deep_rescan.check_invariants([record])
        assert len(passing) == 0
        assert len(quarantined) == 1

    def test_parse_bulk_post_response_result_not_list_raises(
        self, deep_rescan: FdaDeepRescanLoader
    ) -> None:
        body = {"STATUSCODE": 400, "RESULT": {"not": "a list"}}
        with pytest.raises(TransientExtractionError):
            deep_rescan._parse_bulk_post_response(body, "http://example.com")


# ---------------------------------------------------------------------------
# _fetch_page (FdaExtractor) — HTTP paths via respx
# ---------------------------------------------------------------------------


class TestFetchPageExtractor:
    _RECALLS_URL = _BASE_URL + "/recalls/"

    def test_auth_missing_raises(self, extractor: FdaExtractor) -> None:
        object.__setattr__(extractor.settings, "fda_authorization_user", None)
        object.__setattr__(extractor.settings, "fda_authorization_key", None)
        with pytest.raises(AuthenticationError):
            extractor._auth_headers()

    def test_200_returns_records(self, extractor: FdaExtractor) -> None:
        body = {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": [_VALID_RAW]}
        with respx.mock:
            respx.post(self._RECALLS_URL).mock(return_value=httpx.Response(200, json=body))
            result = extractor._fetch_page(filter_str="[{}]")
        assert result == [_VALID_RAW]

    def test_429_raises_rate_limit_error(self, extractor: FdaExtractor) -> None:
        with respx.mock:
            respx.post(self._RECALLS_URL).mock(
                return_value=httpx.Response(429, headers={"Retry-After": "30"})
            )
            with pytest.raises(RateLimitError) as exc_info:
                extractor._fetch_page(filter_str="[{}]")
        assert exc_info.value.retry_after == 30.0

    def test_500_raises_transient_error(self, extractor: FdaExtractor) -> None:
        with respx.mock:
            respx.post(self._RECALLS_URL).mock(return_value=httpx.Response(500))
            with pytest.raises(TransientExtractionError, match="FDA HTTP 500"):
                extractor._fetch_page(filter_str="[{}]")

    def test_transport_error_raises_transient(self, extractor: FdaExtractor) -> None:
        with respx.mock:
            respx.post(self._RECALLS_URL).mock(side_effect=httpx.ConnectError("network down"))
            with pytest.raises(TransientExtractionError, match="FDA network error"):
                extractor._fetch_page(filter_str="[{}]")


# ---------------------------------------------------------------------------
# _paginate (FdaExtractor) — multi-page
# ---------------------------------------------------------------------------


class TestPaginateExtractor:
    def test_multi_page_accumulates_records(self, extractor: FdaExtractor) -> None:
        page1 = [_VALID_RAW] * 5_000
        page2 = [{**_VALID_RAW, "PRODUCTID": "999"}]
        with patch.object(extractor, "_fetch_page", side_effect=[page1, page2]):
            result = extractor._paginate("[{}]")
        assert len(result) == 5_001
