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
    ExtractionAbortedError,
    ExtractionError,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors.fda_press_release import (
    FdaPressReleaseCheckpointedSeedLoader,
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


@pytest.fixture
def seed_loader(monkeypatch: pytest.MonkeyPatch) -> FdaPressReleaseCheckpointedSeedLoader:
    """Checkpointed seed loader with mocked engine + R2 and no inter-event sleep."""
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
        return FdaPressReleaseCheckpointedSeedLoader(
            base_url=_BASE_URL, settings=settings, inter_event_sleep_seconds=0.0
        )


# ---------------------------------------------------------------------------
# FdaPressReleaseCheckpointedSeedLoader — resumable recent-first seed driver
# ---------------------------------------------------------------------------


class TestCheckpointedSeedDriver:
    # run() lives on the base Extractor (no leading underscore), and Pydantic v2 blocks
    # setattr of non-field instance attrs — so run() is patched on the CLASS, not the
    # instance. The underscore-prefixed helpers patch fine on the instance.
    _SEED_CLS = FdaPressReleaseCheckpointedSeedLoader

    def test_loops_until_short_batch_then_marks_complete(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """Three full batches then a short one → 4 run()s, mark complete once, totals summed."""
        loader = seed_loader
        event_counts = iter([250, 250, 250, 137])
        loaded_counts = iter([5, 8, 12, 3])

        def fake_run(change_type: str) -> Any:
            loader._last_batch_event_count = next(event_counts)
            result = MagicMock()
            result.records_loaded = next(loaded_counts)
            return result

        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(self._SEED_CLS, "run", side_effect=fake_run) as run_mock,
            patch.object(loader, "_mark_complete") as mark_mock,
        ):
            summary = loader.run_checkpointed(change_type="historical_seed", batch_size=250)

        assert run_mock.call_count == 4
        assert mark_mock.call_count == 1
        assert summary == {
            "batches": 4,
            "events": 887,  # 250+250+250+137
            "loaded": 28,  # 5+8+12+3
            "already_complete": False,
        }

    def test_exact_multiple_runs_one_trailing_empty_batch(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """When events are an exact multiple of batch_size, the final batch returns 0 → stop."""
        loader = seed_loader
        counts = iter([250, 0])

        def fake_run(change_type: str) -> Any:
            loader._last_batch_event_count = next(counts)
            result = MagicMock()
            result.records_loaded = 0
            return result

        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(self._SEED_CLS, "run", side_effect=fake_run) as run_mock,
            patch.object(loader, "_mark_complete") as mark_mock,
        ):
            summary = loader.run_checkpointed(change_type="historical_seed", batch_size=250)

        assert run_mock.call_count == 2
        assert mark_mock.call_count == 1
        assert summary["batches"] == 2

    def test_noop_when_checkpoint_already_complete(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """A 'complete' checkpoint short-circuits: no run(), no re-mark."""
        loader = seed_loader
        done_cursor = {"init_dt": "2026-05-01T00:00:00+00:00", "event_id": 99000}
        with (
            patch.object(loader, "_read_checkpoint", return_value=(done_cursor, "complete")),
            patch.object(self._SEED_CLS, "run") as run_mock,
            patch.object(loader, "_mark_complete") as mark_mock,
        ):
            summary = loader.run_checkpointed(change_type="historical_seed")

        run_mock.assert_not_called()
        mark_mock.assert_not_called()
        assert summary["already_complete"] is True

    def test_extract_builds_next_cursor_from_last_workitem(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """extract() stashes the batch event count + the recent-first resume cursor (the
        LAST work item = the oldest in this DESC batch)."""
        loader = seed_loader
        loader.batch_size = 250
        work = [
            {
                "recall_event_id": 99000,
                "init_key": datetime(2026, 5, 1, tzinfo=UTC),
                "event_lmd": None,
            },
            {
                "recall_event_id": 98000,
                "init_key": datetime(2026, 4, 1, tzinfo=UTC),
                "event_lmd": None,
            },
        ]
        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(loader, "_build_seed_work_list", return_value=work),
            patch.object(loader, "_fetch_all", return_value=[]) as fetch_mock,
        ):
            loader.extract()

        assert loader._last_batch_event_count == 2
        assert loader._next_cursor == {"init_dt": "2026-04-01T00:00:00+00:00", "event_id": 98000}
        fetch_mock.assert_called_once_with(work)

    def test_extract_empty_work_clears_cursor(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """An empty work-list (past the cursor) → count 0 and no cursor write target."""
        loader = seed_loader
        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(loader, "_build_seed_work_list", return_value=[]),
            patch.object(loader, "_fetch_all", return_value=[]),
        ):
            loader.extract()

        assert loader._last_batch_event_count == 0
        assert loader._next_cursor is None

    def test_transient_batch_failure_cools_down_then_resumes(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """A transient/throttle batch failure is not fatal: the driver sleeps the cooldown and
        re-runs the batch (resuming from the committed cursor), then completes normally. The
        failed attempt does not advance the batch/loaded totals."""
        loader = seed_loader
        loader.cooldown_base_seconds = 10.0
        outcomes = iter(["fail", "ok"])

        def fake_run(change_type: str) -> Any:
            if next(outcomes) == "fail":
                raise TransientExtractionError("503 load-shed")
            loader._last_batch_event_count = 5  # short batch → exhausted → complete
            result = MagicMock()
            result.records_loaded = 2
            return result

        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(self._SEED_CLS, "run", side_effect=fake_run) as run_mock,
            patch.object(loader, "_mark_complete") as mark_mock,
            patch("src.extractors.fda_press_release.time.sleep") as sleep_mock,
        ):
            summary = loader.run_checkpointed(change_type="historical_seed", batch_size=250)

        assert run_mock.call_count == 2  # one failure, then success
        sleep_mock.assert_called_once_with(10.0)  # base cooldown, single failure
        assert mark_mock.call_count == 1
        assert summary == {"batches": 1, "events": 5, "loaded": 2, "already_complete": False}

    def test_circuit_breaker_aborts_after_max_with_escalating_capped_cooldown(
        self, seed_loader: FdaPressReleaseCheckpointedSeedLoader
    ) -> None:
        """A persistent throttle escalates the cooldown (base * 2**(n-1), capped) and, after
        max_consecutive_failures, trips the breaker and raises (cursor preserved → re-runnable);
        the sweep is never marked complete."""
        loader = seed_loader
        loader.cooldown_base_seconds = 10.0
        loader.cooldown_max_seconds = 15.0

        def fake_run(change_type: str) -> Any:
            raise TransientExtractionError("persistent throttle")

        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(self._SEED_CLS, "run", side_effect=fake_run) as run_mock,
            patch.object(loader, "_mark_complete") as mark_mock,
            patch("src.extractors.fda_press_release.time.sleep") as sleep_mock,
            pytest.raises(TransientExtractionError, match="persistent throttle"),
        ):
            loader.run_checkpointed(
                change_type="historical_seed", batch_size=250, max_consecutive_failures=3
            )

        # Failures 1,2,3 sleep (10, 20→cap 15, 40→cap 15); failure 4 (>3) trips the breaker.
        assert run_mock.call_count == 4
        assert [c.args[0] for c in sleep_mock.call_args_list] == [10.0, 15.0, 15.0]
        mark_mock.assert_not_called()

    @pytest.mark.parametrize(
        "make_exc",
        [
            pytest.param(lambda: AuthenticationError("creds bad"), id="auth"),
            pytest.param(
                lambda: ExtractionAbortedError("fda_press_releases", 0.9, 0.05), id="aborted"
            ),
        ],
    )
    def test_deterministic_error_surfaces_immediately_without_cooldown(
        self,
        seed_loader: FdaPressReleaseCheckpointedSeedLoader,
        make_exc: Any,
    ) -> None:
        """Auth failures and rejection-rate aborts are deterministic — a cooldown+resume can't
        change them, so the driver re-raises at once (no sleep, no retry)."""
        loader = seed_loader

        def fake_run(change_type: str) -> Any:
            raise make_exc()

        with (
            patch.object(loader, "_read_checkpoint", return_value=(None, None)),
            patch.object(self._SEED_CLS, "run", side_effect=fake_run) as run_mock,
            patch("src.extractors.fda_press_release.time.sleep") as sleep_mock,
            pytest.raises(ExtractionError),
        ):
            loader.run_checkpointed(change_type="historical_seed")

        assert run_mock.call_count == 1
        sleep_mock.assert_not_called()


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

    def test_503_html_raises_transient_not_fingerprint_block(
        self, pr_extractor: FdaPressReleaseExtractor
    ) -> None:
        """THE regression for the overnight-seed crash. A 5xx WITH an HTML body (Akamai's 503
        "Service Unavailable" reference page) is the edge shedding load — TRANSIENT, like a
        plain 5xx — not the permanent anti-abuse fingerprint block. It must raise
        TransientExtractionError so the per-event retry / driver cooldown recover it, not the
        non-retryable ExtractionError that aborted the seed on a single HTTP 503."""
        with respx.mock:
            respx.get(_PR_URL + "1").mock(
                return_value=httpx.Response(
                    503,
                    text="<html>Service Unavailable</html>",
                    headers={"Content-Type": "text/html"},
                )
            )
            with httpx.Client() as client, pytest.raises(TransientExtractionError):
                pr_extractor._fetch_event(client, 1)

    def test_200_html_fingerprint_raises_non_retryable_extraction_error(
        self, pr_extractor: FdaPressReleaseExtractor
    ) -> None:
        """A 2xx HTML body (a followed 302 → apology page) is the permanent fingerprint block:
        the client identity is refused, so it stays a non-retryable ExtractionError (the driver
        applies a long cooldown / trips the breaker rather than fast-retrying)."""
        with respx.mock:
            respx.get(_PR_URL + "1").mock(
                return_value=httpx.Response(
                    200, text="<html>apology</html>", headers={"Content-Type": "text/html"}
                )
            )
            with httpx.Client() as client, pytest.raises(ExtractionError, match="anti-abuse"):
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
