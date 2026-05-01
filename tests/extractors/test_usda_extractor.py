from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa
import structlog.testing

from src.config.settings import Settings
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    ExtractionResult,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.usda import (
    _FALLBACK_FIREFOX_UA,
    UsdaDeepRescanLoader,
    UsdaExtractor,
    _browser_headers,
    _load_user_agent,
    _parse_http_date,
)
from src.schemas.usda import UsdaFsisRecord

_BASE_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"
_FAKE_R2_PATH = "usda/2026-04-30/abc.json"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}

_VALID_RAW: dict[str, Any] = {
    "field_recall_number": "004-2020",
    "langcode": "English",
    "field_title": "Sample recall",
    "field_recall_date": "2020-05-15",
    "field_recall_type": "Active Recall",
    "field_recall_classification": "Class I",
    "field_archive_recall": "True",
    "field_has_spanish": "True",
    "field_active_notice": "False",
}


def _make_response(
    status_code: int,
    *,
    json_body: Any = None,
    headers: dict[str, str] | None = None,
    text: str = "",
) -> httpx.Response:
    """Build a real httpx.Response so .json() / .headers / .request.url are real."""
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
def extractor(monkeypatch: pytest.MonkeyPatch) -> UsdaExtractor:
    """UsdaExtractor with mocked engine and R2 client.

    `etag_enabled=True` is set explicitly here so the existing TestFetch /
    TestExtract tests continue to exercise the conditional-GET code path. The
    production class default is False (per Finding N — disabled until multi-day
    probe data confirms consistency); see TestEtagDefaults below.
    """
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return UsdaExtractor(base_url=_BASE_URL, settings=settings, etag_enabled=True)


@pytest.fixture
def deep_rescan(monkeypatch: pytest.MonkeyPatch) -> UsdaDeepRescanLoader:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.usda.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return UsdaDeepRescanLoader(base_url=_BASE_URL, settings=settings)


# ---------------------------------------------------------------------------
# _parse_http_date
# ---------------------------------------------------------------------------


class TestParseHttpDate:
    def test_imf_fixdate(self) -> None:
        from datetime import UTC, datetime

        result = _parse_http_date("Wed, 29 Apr 2026 14:29:36 GMT")
        assert result == datetime(2026, 4, 29, 14, 29, 36, tzinfo=UTC)

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_http_date("2026-04-29")


# ---------------------------------------------------------------------------
# _fetch — status code routing and ETag handling
# ---------------------------------------------------------------------------


class TestFetch:
    def test_200_returns_records(self, extractor: UsdaExtractor) -> None:
        response = _make_response(
            200,
            json_body=[_VALID_RAW],
            headers={"etag": '"123"', "last-modified": "Wed, 29 Apr 2026 14:29:36 GMT"},
        )
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records, status, etag, lm = extractor._fetch(prior_etag=None, prior_last_modified=None)
        assert records == [_VALID_RAW]
        assert status == 200
        assert etag == '"123"'
        assert lm == "Wed, 29 Apr 2026 14:29:36 GMT"

    def test_304_returns_empty_records(self, extractor: UsdaExtractor) -> None:
        response = _make_response(
            304,
            text="",
            headers={"etag": '"123"', "last-modified": "Wed, 29 Apr 2026 14:29:36 GMT"},
        )
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            records, status, etag, lm = extractor._fetch(
                prior_etag='"123"',
                prior_last_modified="Wed, 29 Apr 2026 14:29:36 GMT",
            )
        assert records == []
        assert status == 304
        assert etag == '"123"'

    def test_sends_if_none_match_when_prior_etag_present(self, extractor: UsdaExtractor) -> None:
        response = _make_response(304, text="", headers={})
        with patch("httpx.Client") as mock_client:
            mock_get = mock_client.return_value.__enter__.return_value.get
            mock_get.return_value = response
            extractor._fetch(prior_etag='"abc"', prior_last_modified=None)
        sent_headers = mock_get.call_args.kwargs["headers"]
        assert sent_headers.get("If-None-Match") == '"abc"'

    def test_skips_if_none_match_when_etag_disabled(self, extractor: UsdaExtractor) -> None:
        extractor.etag_enabled = False
        response = _make_response(200, json_body=[], headers={})
        with patch("httpx.Client") as mock_client:
            mock_get = mock_client.return_value.__enter__.return_value.get
            mock_get.return_value = response
            extractor._fetch(prior_etag='"abc"', prior_last_modified="something")
        sent_headers = mock_get.call_args.kwargs["headers"]
        assert "If-None-Match" not in sent_headers
        assert "If-Modified-Since" not in sent_headers

    def test_429_raises_rate_limit_error(self, extractor: UsdaExtractor) -> None:
        response = _make_response(429, text="rate limited", headers={"Retry-After": "30"})
        with (
            patch("httpx.Client") as mock_client,
            patch.object(extractor, "_capture_error_response"),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(RateLimitError):
                extractor._fetch(prior_etag=None, prior_last_modified=None)

    def test_401_raises_authentication_error(self, extractor: UsdaExtractor) -> None:
        response = _make_response(401, text="denied")
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(AuthenticationError):
                extractor._fetch(prior_etag=None, prior_last_modified=None)

    def test_500_raises_transient_error(self, extractor: UsdaExtractor) -> None:
        response = _make_response(500, text="oops")
        with (
            patch("httpx.Client") as mock_client,
            patch.object(extractor, "_capture_error_response"),
        ):
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(TransientExtractionError):
                extractor._fetch(prior_etag=None, prior_last_modified=None)

    def test_transport_error_raises_transient(self, extractor: UsdaExtractor) -> None:
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.ConnectError(
                "boom"
            )
            with pytest.raises(TransientExtractionError):
                extractor._fetch(prior_etag=None, prior_last_modified=None)


# ---------------------------------------------------------------------------
# extract() — count guard and 304 short-circuit
# ---------------------------------------------------------------------------


class TestExtract:
    def test_returns_records_on_200(self, extractor: UsdaExtractor) -> None:
        with (
            patch.object(extractor, "_read_etag_state", return_value=(None, None)),
            patch.object(
                extractor,
                "_fetch",
                return_value=([_VALID_RAW], 200, '"new"', "Wed, 30 Apr 2026 00:00:00 GMT"),
            ),
        ):
            result = extractor.extract()
        assert result == [_VALID_RAW]
        assert extractor._not_modified is False
        assert extractor._captured_etag == '"new"'

    def test_short_circuits_on_304(self, extractor: UsdaExtractor) -> None:
        with (
            patch.object(
                extractor,
                "_read_etag_state",
                return_value=('"old"', "Wed, 29 Apr 2026 14:29:36 GMT"),
            ),
            patch.object(
                extractor,
                "_fetch",
                return_value=([], 304, '"old"', "Wed, 29 Apr 2026 14:29:36 GMT"),
            ),
        ):
            result = extractor.extract()
        assert result == []
        assert extractor._not_modified is True

    def test_aborts_on_count_guard(self, extractor: UsdaExtractor) -> None:
        oversized = [_VALID_RAW] * 5_001
        with (
            patch.object(extractor, "_read_etag_state", return_value=(None, None)),
            patch.object(extractor, "_fetch", return_value=(oversized, 200, None, None)),
            pytest.raises(TransientExtractionError, match="exceeds guard"),
        ):
            extractor.extract()

    def test_contradiction_guard_fires_on_advanced_last_modified(
        self, extractor: UsdaExtractor
    ) -> None:
        # 304 returned but last-modified header advanced — server-side stale-positive ETag.
        with (
            patch.object(
                extractor,
                "_read_etag_state",
                return_value=('"old"', "Wed, 29 Apr 2026 14:29:36 GMT"),
            ),
            patch.object(
                extractor,
                "_fetch",
                return_value=([], 304, '"old"', "Wed, 30 Apr 2026 00:00:00 GMT"),
            ),
            pytest.raises(ExtractionError, match="contradiction guard"),
        ):
            extractor.extract()

    def test_contradiction_guard_silent_when_last_modified_unchanged(
        self, extractor: UsdaExtractor
    ) -> None:
        with (
            patch.object(
                extractor,
                "_read_etag_state",
                return_value=('"old"', "Wed, 29 Apr 2026 14:29:36 GMT"),
            ),
            patch.object(
                extractor,
                "_fetch",
                return_value=([], 304, '"old"', "Wed, 29 Apr 2026 14:29:36 GMT"),
            ),
        ):
            # Should not raise.
            assert extractor.extract() == []


# ---------------------------------------------------------------------------
# land_raw — 304 path skips R2 write
# ---------------------------------------------------------------------------


class TestLandRaw:
    def test_returns_r2_path_on_normal_path(self, extractor: UsdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land.return_value = _FAKE_R2_PATH
        assert extractor.land_raw([_VALID_RAW]) == _FAKE_R2_PATH

    def test_skips_r2_on_not_modified(self, extractor: UsdaExtractor) -> None:
        extractor._not_modified = True
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        result = extractor.land_raw([])
        assert result == ""
        mock_r2.land.assert_not_called()


# ---------------------------------------------------------------------------
# validate_records / check_invariants
# ---------------------------------------------------------------------------


class TestValidateRecords:
    def test_valid_record_passes(self, extractor: UsdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        valid, quarantined = extractor.validate_records([_VALID_RAW])
        assert len(valid) == 1
        assert len(quarantined) == 0

    def test_invalid_record_quarantined(self, extractor: UsdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        bad = {**_VALID_RAW, "field_unknown": "extra"}
        valid, quarantined = extractor.validate_records([bad])
        assert len(valid) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "validate_records"


class TestCheckInvariants:
    def test_english_record_passes(self, extractor: UsdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        record = UsdaFsisRecord.model_validate(_VALID_RAW)
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 1
        assert len(quarantined) == 0

    def test_orphan_spanish_record_quarantined(self, extractor: UsdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        spanish_orphan = UsdaFsisRecord.model_validate({**_VALID_RAW, "langcode": "Spanish"})
        passing, quarantined = extractor.check_invariants([spanish_orphan])
        assert len(passing) == 0
        assert len(quarantined) == 1
        assert quarantined[0].failure_stage == "invariants"
        assert "Spanish" in quarantined[0].failure_reason

    def test_bilingual_pair_both_pass(self, extractor: UsdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        en = UsdaFsisRecord.model_validate(_VALID_RAW)
        es = UsdaFsisRecord.model_validate({**_VALID_RAW, "langcode": "Spanish"})
        passing, quarantined = extractor.check_invariants([en, es])
        assert len(passing) == 2
        assert len(quarantined) == 0

    def test_null_source_recall_id_quarantined(self, extractor: UsdaExtractor) -> None:
        extractor._current_landing_path = _FAKE_R2_PATH
        record = UsdaFsisRecord.model_validate(_VALID_RAW)
        object.__setattr__(record, "source_recall_id", "")
        passing, quarantined = extractor.check_invariants([record])
        assert len(passing) == 0
        assert len(quarantined) == 1


# ---------------------------------------------------------------------------
# load_bronze — 304 short-circuit, watermark update
# ---------------------------------------------------------------------------


class TestLoadBronze:
    def test_not_modified_skips_loader_and_touches_freshness(
        self, extractor: UsdaExtractor
    ) -> None:
        extractor._not_modified = True
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        # _engine.begin() is a context manager
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        with patch("src.extractors.usda.BronzeLoader") as MockLoader:
            count = extractor.load_bronze([], [], "")
        assert count == 0
        MockLoader.assert_not_called()  # short-circuited; no loader instantiated
        # _touch_freshness ran an UPDATE
        mock_conn.execute.assert_called()

    def test_normal_path_calls_loader_and_updates_watermark(self, extractor: UsdaExtractor) -> None:
        extractor._captured_etag = '"new"'
        extractor._captured_last_modified = "Wed, 30 Apr 2026 00:00:00 GMT"
        record = UsdaFsisRecord.model_validate(_VALID_RAW)

        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        mock_loader = MagicMock()
        mock_loader.load.return_value = 1
        with patch("src.extractors.usda.BronzeLoader", return_value=mock_loader):
            count = extractor.load_bronze([record], [], _FAKE_R2_PATH)

        assert count == 1
        mock_loader.load.assert_called_once()
        # watermark UPDATE was issued (in addition to BronzeLoader's internal calls)
        assert mock_conn.execute.called


# ---------------------------------------------------------------------------
# UsdaDeepRescanLoader — never sends If-None-Match, never updates watermark
# ---------------------------------------------------------------------------


class TestEtagDefaults:
    def test_usda_extractor_etag_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Production posture: do not depend on Akamai's cached path until
        # multi-day probes confirm consistency (Finding N).
        for k, v in _REQUIRED_ENV.items():
            monkeypatch.setenv(k, v)
        with (
            patch("sqlalchemy.create_engine"),
            patch("src.extractors.usda.R2LandingClient"),
        ):
            settings = Settings()  # type: ignore[call-arg]
            extractor = UsdaExtractor(base_url=_BASE_URL, settings=settings)
        assert extractor.etag_enabled is False


class TestUsdaDeepRescanLoader:
    def test_etag_disabled_by_default(self, deep_rescan: UsdaDeepRescanLoader) -> None:
        assert deep_rescan.etag_enabled is False

    def test_load_bronze_does_not_update_watermark(self, deep_rescan: UsdaDeepRescanLoader) -> None:
        record = UsdaFsisRecord.model_validate(_VALID_RAW)
        mock_engine: MagicMock = deep_rescan._engine  # type: ignore[assignment]
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        mock_loader = MagicMock()
        mock_loader.load.return_value = 1
        with patch("src.extractors.usda.BronzeLoader", return_value=mock_loader):
            deep_rescan.load_bronze([record], [], _FAKE_R2_PATH)

        # The deep-rescan path issues no UPDATE statements on source_watermarks —
        # only the loader's internal calls are expected. Verify by counting that no
        # call to mock_conn.execute carried an Update statement.
        for call in mock_conn.execute.call_args_list:
            stmt = call.args[0] if call.args else None
            # If any call passed a SQLAlchemy update() against source_watermarks, fail.
            if stmt is not None and "UPDATE source_watermarks" in str(stmt).upper():
                pytest.fail(
                    "UsdaDeepRescanLoader.load_bronze should not update "
                    f"source_watermarks; got {stmt}"
                )


# ---------------------------------------------------------------------------
# _guard_etag_contradiction — branches not covered by extract() tests
# ---------------------------------------------------------------------------


class TestGuardEtagContradiction:
    def test_returns_silently_when_prior_last_modified_missing(
        self, extractor: UsdaExtractor
    ) -> None:
        # If we have no prior last-modified to compare against, the guard is a no-op.
        extractor._guard_etag_contradiction(None, "Wed, 30 Apr 2026 00:00:00 GMT")

    def test_returns_silently_when_current_last_modified_missing(
        self, extractor: UsdaExtractor
    ) -> None:
        # If the current response carries no last-modified, we cannot detect drift —
        # treat as inconclusive rather than failing the run.
        extractor._guard_etag_contradiction("Wed, 29 Apr 2026 14:29:36 GMT", None)

    def test_unparseable_dates_raise_extraction_error(self, extractor: UsdaExtractor) -> None:
        # Headers differ but neither parses as an IMF-fixdate. Defensive branch:
        # treat as suspicious (potential stale-positive ETag) and raise.
        with pytest.raises(ExtractionError, match="Could not parse"):
            extractor._guard_etag_contradiction("garbage1", "garbage2")

    def test_advanced_last_modified_raises(self, extractor: UsdaExtractor) -> None:
        # Already covered by TestExtract but exercised here for symmetry with the
        # other branches of the guard, and to lock the error message in.
        with pytest.raises(ExtractionError, match="advanced from"):
            extractor._guard_etag_contradiction(
                "Wed, 29 Apr 2026 14:29:36 GMT",
                "Wed, 30 Apr 2026 00:00:00 GMT",
            )


# ---------------------------------------------------------------------------
# _capture_error_response — best-effort R2 land + warning on failure
# ---------------------------------------------------------------------------


class TestCaptureErrorResponse:
    def test_calls_land_error_response_on_r2_with_correct_args(
        self, extractor: UsdaExtractor
    ) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        request = httpx.Request("GET", _BASE_URL)
        response = httpx.Response(
            500,
            request=request,
            content=b"oops",
            headers={"content-type": "text/plain"},
        )
        extractor._capture_error_response(response)
        mock_r2.land_error_response.assert_called_once()
        kwargs = mock_r2.land_error_response.call_args.kwargs
        assert kwargs["source"] == "usda"
        assert kwargs["request_method"] == "GET"
        assert kwargs["status_code"] == 500
        assert kwargs["response_body"] == "oops"

    def test_does_not_raise_when_r2_raises(self, extractor: UsdaExtractor) -> None:
        # Best-effort: R2 failures during error capture must not mask the original
        # extraction error or interrupt the lifecycle.
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        request = httpx.Request("GET", _BASE_URL)
        response = httpx.Response(429, request=request, content=b"too many")
        extractor._capture_error_response(response)  # must not raise

    def test_logs_warning_when_r2_raises(self, extractor: UsdaExtractor) -> None:
        mock_r2: MagicMock = extractor._r2_client  # type: ignore[assignment]
        mock_r2.land_error_response.side_effect = RuntimeError("R2 down")
        request = httpx.Request("GET", _BASE_URL)
        response = httpx.Response(500, request=request, content=b"oops")
        with structlog.testing.capture_logs() as captured:
            extractor._capture_error_response(response)
        assert any(e.get("event") == "usda.error_capture_failed" for e in captured)


# ---------------------------------------------------------------------------
# _read_etag_state — reads from source_watermarks
# ---------------------------------------------------------------------------


class TestReadEtagState:
    def _wire_engine(self, extractor: UsdaExtractor, fetchone_value: Any) -> MagicMock:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = fetchone_value
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)
        return mock_conn

    def test_returns_etag_and_last_modified_when_row_present(
        self, extractor: UsdaExtractor
    ) -> None:
        self._wire_engine(extractor, ('"abc"', "Wed, 29 Apr 2026 14:29:36 GMT"))
        etag, lm = extractor._read_etag_state()
        assert etag == '"abc"'
        assert lm == "Wed, 29 Apr 2026 14:29:36 GMT"

    def test_returns_none_pair_when_no_row(self, extractor: UsdaExtractor) -> None:
        self._wire_engine(extractor, None)
        etag, lm = extractor._read_etag_state()
        assert etag is None
        assert lm is None

    def test_returns_none_pair_when_columns_null(self, extractor: UsdaExtractor) -> None:
        # Row exists (USDA was provisioned in 0001 baseline) but columns are NULL.
        # Caller treats both Nones as "no prior state" and skips the conditional GET.
        self._wire_engine(extractor, (None, None))
        etag, lm = extractor._read_etag_state()
        assert etag is None
        assert lm is None


# ---------------------------------------------------------------------------
# _record_run — extraction_runs row insertion (success + failure paths)
# ---------------------------------------------------------------------------


class TestRecordRun:
    def _wire_begin(self, extractor: UsdaExtractor) -> MagicMock:
        mock_conn = MagicMock()
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=None)
        return mock_conn

    def test_inserts_minimal_row_when_no_result(self, extractor: UsdaExtractor) -> None:
        mock_conn = self._wire_begin(extractor)
        extractor._record_run(
            run_id="run-1",
            started_at=datetime(2026, 4, 30, tzinfo=UTC),
            status="failed",
            error_message="boom",
        )
        mock_conn.execute.assert_called_once()
        # Verify the row dict carried the fields the lifecycle expects.
        compiled = mock_conn.execute.call_args.args[0]
        params = compiled.compile().params
        assert params["source"] == "usda"
        assert params["status"] == "failed"
        assert params["run_id"] == "run-1"
        assert params["error_message"] == "boom"
        # No result → counts and landing path absent
        assert "records_extracted" not in params or params.get("records_extracted") is None

    def test_inserts_full_row_when_result_present(self, extractor: UsdaExtractor) -> None:
        mock_conn = self._wire_begin(extractor)
        result = ExtractionResult(
            source="usda",
            run_id="run-2",
            records_fetched=2001,
            records_landed=2001,
            records_valid=2000,
            records_rejected_validate=1,
            records_rejected_invariants=0,
            records_loaded=2000,
            raw_landing_path="usda/2026-04-30/abc.json",
        )
        extractor._record_run(
            run_id="run-2",
            started_at=datetime(2026, 4, 30, tzinfo=UTC),
            status="success",
            result=result,
        )
        mock_conn.execute.assert_called_once()
        compiled = mock_conn.execute.call_args.args[0]
        params = compiled.compile().params
        assert params["records_extracted"] == 2001
        assert params["records_inserted"] == 2000
        # rejected = validate + invariants
        assert params["records_rejected"] == 1
        assert params["raw_landing_path"] == "usda/2026-04-30/abc.json"

    def test_db_failure_is_swallowed_and_logged(self, extractor: UsdaExtractor) -> None:
        # extraction_runs is monitoring/observability; failure here must not mask
        # the actual extraction outcome (success or otherwise) being returned to the caller.
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.side_effect = RuntimeError("DB down")
        with structlog.testing.capture_logs() as captured:
            extractor._record_run(
                run_id="run-3",
                started_at=datetime(2026, 4, 30, tzinfo=UTC),
                status="success",
            )
        assert any(e.get("event") == "extraction_run.record_failed" for e in captured)


# ---------------------------------------------------------------------------
# _load_user_agent + _browser_headers — Akamai Bot Manager workaround (Finding O)
# ---------------------------------------------------------------------------


class TestLoadUserAgent:
    def test_returns_vendored_ua_when_file_present(self, tmp_path: Any) -> None:
        # Real-file integration: write a tiny user_agents.json, point the loader at it,
        # and verify the firefox_linux value comes back unchanged.
        path = tmp_path / "user_agents.json"
        path.write_text(
            '{"user_agents": {"firefox_linux": "Mozilla/5.0 test/1.0", '
            '"chrome_linux": "irrelevant"}}'
        )
        with patch("src.extractors.usda._USER_AGENTS_PATH", path):
            assert _load_user_agent() == "Mozilla/5.0 test/1.0"

    def test_falls_back_when_file_missing(self, tmp_path: Any) -> None:
        missing = tmp_path / "does_not_exist.json"
        with (
            patch("src.extractors.usda._USER_AGENTS_PATH", missing),
            structlog.testing.capture_logs() as captured,
        ):
            assert _load_user_agent() == _FALLBACK_FIREFOX_UA
        assert any(e.get("event") == "usda.user_agents_load_failed" for e in captured)

    def test_falls_back_when_json_malformed(self, tmp_path: Any) -> None:
        path = tmp_path / "bad.json"
        path.write_text("not json {")
        with (
            patch("src.extractors.usda._USER_AGENTS_PATH", path),
            structlog.testing.capture_logs() as captured,
        ):
            assert _load_user_agent() == _FALLBACK_FIREFOX_UA
        assert any(e.get("event") == "usda.user_agents_load_failed" for e in captured)

    def test_falls_back_when_key_missing(self, tmp_path: Any) -> None:
        path = tmp_path / "missing_key.json"
        # Valid JSON, but no user_agents.firefox_linux
        path.write_text('{"sources": {}}')
        with (
            patch("src.extractors.usda._USER_AGENTS_PATH", path),
            structlog.testing.capture_logs() as captured,
        ):
            assert _load_user_agent() == _FALLBACK_FIREFOX_UA
        assert any(e.get("event") == "usda.user_agents_load_failed" for e in captured)

    def test_falls_back_when_ua_empty_string(self, tmp_path: Any) -> None:
        # Defensive: schema present but value is empty — treat as malformed.
        path = tmp_path / "empty_ua.json"
        path.write_text('{"user_agents": {"firefox_linux": ""}}')
        with (
            patch("src.extractors.usda._USER_AGENTS_PATH", path),
            structlog.testing.capture_logs() as captured,
        ):
            assert _load_user_agent() == _FALLBACK_FIREFOX_UA
        assert any(e.get("event") == "usda.user_agents_load_failed" for e in captured)


class TestBrowserHeaders:
    def test_returns_browser_like_headers(self) -> None:
        headers = _browser_headers()
        assert "User-Agent" in headers
        # Must look like a real browser, not python-httpx
        assert "Mozilla/5.0" in headers["User-Agent"]
        assert "python-httpx" not in headers["User-Agent"]
        # Accompanying headers Akamai expects on real-browser requests
        assert headers["Accept"] == "application/json,*/*"
        assert headers["Accept-Language"] == "en-US,en;q=0.9"
        assert headers["Accept-Encoding"] == "gzip, deflate"


class TestFetchAppliesBrowserHeaders:
    def test_httpx_client_constructed_with_browser_headers(self, extractor: UsdaExtractor) -> None:
        response = _make_response(200, json_body=[], headers={})
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            extractor._fetch(prior_etag=None, prior_last_modified=None)
        # httpx.Client was constructed with our browser headers as defaults.
        ctor_kwargs = mock_client.call_args.kwargs
        assert "headers" in ctor_kwargs
        assert "Mozilla/5.0" in ctor_kwargs["headers"]["User-Agent"]
        assert ctor_kwargs["headers"]["Accept"] == "application/json,*/*"
