from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.fda_press_release import (
    FdaPressReleaseExtractor,
    _interpret_pr_response,
    _rows_from_result,
)

_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_PR_URL = _BASE_URL + "/search/pressreleaseurls/"
_FAKE_R2_PATH = "fda_press_releases/2026-06-03/abc.json"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
    "FDA_AUTHORIZATION_USER": "test-user",
    "FDA_AUTHORIZATION_KEY": "test-key",
}


@pytest.fixture
def pr_extractor(monkeypatch: pytest.MonkeyPatch) -> FdaPressReleaseExtractor:
    """FdaPressReleaseExtractor with mocked engine + R2 and no inter-event sleep."""
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.fda_press_release.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return FdaPressReleaseExtractor(
            base_url=_BASE_URL, settings=settings, inter_event_sleep_seconds=0.0
        )


# ---------------------------------------------------------------------------
# _rows_from_result — RESULT shape normalisation (array / columnar / empty)
# ---------------------------------------------------------------------------


class TestRowsFromResult:
    def test_array_shape(self) -> None:
        body = {"RESULT": [{"PRESSRELEASEURL": "http://x"}]}
        assert _rows_from_result(body) == [{"PRESSRELEASEURL": "http://x"}]

    def test_columnar_shape(self) -> None:
        body = {
            "RESULT": {
                "COLUMNS": ["RECALLEVENTID", "PRESSRELEASEURL"],
                "DATA": [["98815", "http://a"], ["98815", "http://b"]],
            }
        }
        assert _rows_from_result(body) == [
            {"RECALLEVENTID": "98815", "PRESSRELEASEURL": "http://a"},
            {"RECALLEVENTID": "98815", "PRESSRELEASEURL": "http://b"},
        ]

    def test_empty_array(self) -> None:
        assert _rows_from_result({"RESULT": []}) == []

    def test_null_result(self) -> None:
        assert _rows_from_result({"RESULT": None, "RESULTCOUNT": 0}) == []

    def test_missing_result_key(self) -> None:
        assert _rows_from_result({"STATUSCODE": 412}) == []

    def test_empty_columnar(self) -> None:
        assert _rows_from_result({"RESULT": {"COLUMNS": ["A"], "DATA": []}}) == []


# ---------------------------------------------------------------------------
# _interpret_pr_response — FDA STATUSCODE envelope (incl. empty-event no-op)
# ---------------------------------------------------------------------------


class TestInterpretPrResponse:
    def test_success_with_rows(self) -> None:
        body = {"STATUSCODE": 400, "RESULT": [{"PRESSRELEASEURL": "http://x"}]}
        assert _interpret_pr_response(body, event_id=98815, url="u") == [
            {"PRESSRELEASEURL": "http://x"}
        ]

    def test_success_empty_result_is_no_op(self) -> None:
        # Event with no press releases: STATUSCODE 400 + empty RESULT → [] (no rows).
        assert _interpret_pr_response({"STATUSCODE": 400, "RESULT": []}, event_id=1, url="u") == []

    def test_statuscode_412_is_empty(self) -> None:
        assert _interpret_pr_response({"STATUSCODE": 412}, event_id=1, url="u") == []

    def test_auth_denied_raises(self) -> None:
        with pytest.raises(AuthenticationError):
            _interpret_pr_response({"STATUSCODE": 401, "MESSAGE": "denied"}, event_id=1, url="u")

    def test_other_status_raises_extraction_error(self) -> None:
        with pytest.raises(ExtractionError):
            _interpret_pr_response({"STATUSCODE": 406, "MESSAGE": "bad"}, event_id=1, url="u")


# ---------------------------------------------------------------------------
# _fetch_event — HTTP paths via respx
# ---------------------------------------------------------------------------


def _success(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"STATUSCODE": 400, "MESSAGE": "success", "RESULT": rows}


class TestFetchEvent:
    def test_multi_pr_event_returns_rows(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        rows = [
            {"RECALLEVENTID": "98815", "PRESSRELEASEURL": "http://a", "PRESSRELEASETYPE": "Firm"},
            {"RECALLEVENTID": "98815", "PRESSRELEASEURL": "http://b", "PRESSRELEASETYPE": "FDA"},
        ]
        with respx.mock:
            respx.get(_PR_URL + "98815").mock(return_value=httpx.Response(200, json=_success(rows)))
            with httpx.Client() as client:
                assert pr_extractor._fetch_event(client, 98815) == rows

    def test_empty_event_returns_no_rows(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        with respx.mock:
            respx.get(_PR_URL + "1").mock(return_value=httpx.Response(200, json=_success([])))
            with httpx.Client() as client:
                assert pr_extractor._fetch_event(client, 1) == []

    def test_statuscode_412_returns_no_rows(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        with respx.mock:
            respx.get(_PR_URL + "1").mock(
                return_value=httpx.Response(200, json={"STATUSCODE": 412, "MESSAGE": "No results"})
            )
            with httpx.Client() as client:
                assert pr_extractor._fetch_event(client, 1) == []

    def test_401_raises_authentication_error(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        with respx.mock:
            respx.get(_PR_URL + "1").mock(
                return_value=httpx.Response(200, json={"STATUSCODE": 401, "MESSAGE": "denied"})
            )
            with httpx.Client() as client, pytest.raises(AuthenticationError):
                pr_extractor._fetch_event(client, 1)

    def test_429_raises_rate_limit(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        with respx.mock:
            respx.get(_PR_URL + "1").mock(
                return_value=httpx.Response(429, headers={"Retry-After": "30"})
            )
            with httpx.Client() as client, pytest.raises(RateLimitError) as exc:
                pr_extractor._fetch_event(client, 1)
        assert exc.value.retry_after == 30.0

    def test_html_throttle_raises_extraction_error(
        self, pr_extractor: FdaPressReleaseExtractor
    ) -> None:
        with respx.mock:
            respx.get(_PR_URL + "1").mock(
                return_value=httpx.Response(
                    404, text="<html>apology</html>", headers={"Content-Type": "text/html"}
                )
            )
            with httpx.Client() as client, pytest.raises(ExtractionError, match="anti-abuse"):
                pr_extractor._fetch_event(client, 1)

    def test_500_raises_transient(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        with respx.mock:
            respx.get(_PR_URL + "1").mock(return_value=httpx.Response(500))
            with httpx.Client() as client, pytest.raises(TransientExtractionError):
                pr_extractor._fetch_event(client, 1)


# ---------------------------------------------------------------------------
# extract() — work-list fan-out (flatten rows + watermark target)
# ---------------------------------------------------------------------------


class TestExtract:
    def test_flattens_work_list_and_includes_empty_events(
        self, pr_extractor: FdaPressReleaseExtractor
    ) -> None:
        work = [
            {"recall_event_id": 98815, "event_lmd": datetime(2026, 5, 1, tzinfo=UTC)},
            {"recall_event_id": 98816, "event_lmd": datetime(2026, 5, 3, tzinfo=UTC)},
        ]
        pr_row = {
            "RECALLEVENTID": "98815",
            "PRESSRELEASEURL": "http://a",
            "PRESSRELEASETYPE": "Firm",
        }
        with patch.object(pr_extractor, "_build_work_list", return_value=work), respx.mock:
            respx.get(_PR_URL + "98815").mock(
                return_value=httpx.Response(200, json=_success([pr_row]))
            )
            respx.get(_PR_URL + "98816").mock(return_value=httpx.Response(200, json=_success([])))
            result = pr_extractor.extract()
        assert result == [pr_row]
        # Watermark-advance target = max event_lmd of the events CHECKED (incl. the empty one).
        assert pr_extractor._processed_max_event_lmd == datetime(2026, 5, 3, tzinfo=UTC)

    def test_incremental_count_guard(self, pr_extractor: FdaPressReleaseExtractor) -> None:
        oversized = [
            {"recall_event_id": i, "event_lmd": datetime(2026, 5, 1, tzinfo=UTC)}
            for i in range(5_001)
        ]
        with (
            patch.object(pr_extractor, "_build_work_list", return_value=oversized),
            pytest.raises(TransientExtractionError, match="exceeds guard"),
        ):
            pr_extractor.extract()
