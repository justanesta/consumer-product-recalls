"""Base-class isolation tests for HtmlScrapingExtractor (Phase 5d Step 2).

Per the Phase 5d Step 2 requirement: "Unit-tested in isolation before
``UscgScrapingExtractor`` lands on top of it." A minimal
``_TestSubclass(HtmlScrapingExtractor)`` fixture stubs the abstract
methods so the base-class helpers (``_fetch_page``, ``_archive_page``,
``_throttle``, ``land_raw``, ``_capture_response_metadata``) can be
exercised without dragging in the USCG-specific parsing logic.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import time
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic import BaseModel

from src.extractors._base import (
    AuthenticationError,
    QuarantineRecord,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors._html_scraping import HtmlScrapingExtractor


class _DummyRecord(BaseModel):
    """Minimal Pydantic record type — needed because HtmlScrapingExtractor is generic."""

    foo: str


class _TestSubclass(HtmlScrapingExtractor[_DummyRecord]):
    """Bare-bones concrete subclass for base-class testing.

    Stubs all 5 abstract methods (extract / validate_records /
    check_invariants / load_bronze plus the two parsing abstracts).
    None of these stubs are exercised by the base-class tests — they
    exist solely to satisfy the ABC contract so the class can be
    instantiated.

    ``_r2_client`` is patched directly via ``MagicMock`` in tests that
    exercise ``land_raw`` so we don't need a full Settings instance.
    """

    source_name: str = "test_scrape"
    start_url: str = "https://example.invalid/listing"

    def model_post_init(self, __context: Any) -> None:
        # Skip the production R2 client wiring; tests inject a mock.
        self._r2_client = MagicMock()  # type: ignore[attr-defined]
        self._r2_client.land.return_value = "test_scrape/2026-05-16/abc.ndjson.gz"

    # Minimal stubs — none invoked in the tests below.
    def extract(self) -> list[dict[str, Any]]:  # pragma: no cover
        return []

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[_DummyRecord], list[QuarantineRecord]]:  # pragma: no cover
        return [], []

    def check_invariants(
        self, records: list[_DummyRecord]
    ) -> tuple[list[_DummyRecord], list[QuarantineRecord]]:  # pragma: no cover
        return [], []

    def load_bronze(
        self,
        records: list[_DummyRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:  # pragma: no cover
        return 0

    def _parse_listing_page(
        self, body: bytes, page_url: str
    ) -> list[dict[str, Any]]:  # pragma: no cover
        return []

    def _parse_details_page(self, body: bytes, page_url: str) -> dict[str, Any]:  # pragma: no cover
        return {}


def _make_response(
    status_code: int = 200, body: bytes = b"<html></html>", headers: dict[str, str] | None = None
) -> httpx.Response:
    request = httpx.Request("GET", "https://example.invalid/page")
    return httpx.Response(
        status_code,
        request=request,
        content=body,
        headers=headers or {"content-type": "text/html"},
    )


@pytest.fixture
def extractor() -> _TestSubclass:
    return _TestSubclass(scrape_delay_seconds=0.1)


@pytest.fixture
def fast_sleep() -> Any:
    """Patch ``time.sleep`` in ``_html_scraping`` so tenacity backoff between
    retry attempts is instant. Used by the 5xx retry tests so each test runs
    in milliseconds instead of seconds (the real ``_PER_FETCH_RETRY`` policy
    has ``wait_exponential_jitter(initial=0.5, max=5)`` — 3 attempts is
    ~1.5-2s of wall-time without this patch). Returns the mock so tests can
    assert on it if they want."""
    with patch("src.extractors._html_scraping.time.sleep") as mock:
        yield mock


# ---------------------------------------------------------------------------
# Throttle behavior
# ---------------------------------------------------------------------------
#
# Tests call ``_throttle()`` directly rather than going through
# ``_fetch_page`` — the latter is wrapped by tenacity's ``Retrying``, which
# calls ``time.monotonic()`` internally for retry telemetry. Routing the
# throttle test through ``_fetch_page`` makes the mock side_effect list
# fragile to tenacity internals (exhausted StopIteration on Retrying.set_result).


class TestThrottle:
    def test_first_call_no_sleep(self, extractor: _TestSubclass) -> None:
        """``_last_request_ts == 0.0`` means the throttle math is non-positive."""
        with patch("src.extractors._html_scraping.time.sleep") as mock_sleep:
            extractor._throttle()
        # The throttle only calls sleep when wait > 0. First call: wait =
        # 0 + 0.1 - now() = large negative → no sleep call expected.
        mock_sleep.assert_not_called()

    def test_second_call_respects_delay(self, extractor: _TestSubclass) -> None:
        """Second throttle call sleeps to enforce minimum-inter-request interval."""
        # Simulate "first fetch completed 0.05s ago" by setting the
        # private timestamp directly — that's what `_fetch_page_once` would
        # have set after the first fetch.
        with (
            patch("src.extractors._html_scraping.time.sleep") as mock_sleep,
            patch("src.extractors._html_scraping.time.monotonic") as mock_now,
        ):
            # Throttle reads monotonic once per call.
            mock_now.return_value = 10.05
            extractor._last_request_ts = 10.0  # 0.05s ago
            # With scrape_delay_seconds=0.1, wait = 10.0 + 0.1 - 10.05 = 0.05.
            extractor._throttle()
        mock_sleep.assert_called_once()
        sleep_duration = mock_sleep.call_args.args[0]
        assert 0.04 < sleep_duration < 0.06, f"Expected ~0.05s sleep, got {sleep_duration}"

    def test_no_sleep_when_delay_already_elapsed(self, extractor: _TestSubclass) -> None:
        """If more than ``scrape_delay_seconds`` has passed since the last fetch,
        no sleep is needed — wait math goes non-positive."""
        with (
            patch("src.extractors._html_scraping.time.sleep") as mock_sleep,
            patch("src.extractors._html_scraping.time.monotonic") as mock_now,
        ):
            mock_now.return_value = 20.0
            extractor._last_request_ts = 10.0  # 10s ago, way past 0.1s delay
            extractor._throttle()
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# _fetch_page status-code classification
# ---------------------------------------------------------------------------


class TestFetchStatusCodes:
    def test_200_returns_response_and_body(self, extractor: _TestSubclass) -> None:
        body = b"<html>ok</html>"
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _make_response(
                status_code=200, body=body
            )
            response, returned_body = extractor._fetch_page("https://example.invalid/page")
        assert response.status_code == 200
        assert returned_body == body

    def test_429_raises_ratelimit_with_retry_after(self, extractor: _TestSubclass) -> None:
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _make_response(
                status_code=429,
                headers={"Retry-After": "30"},
            )
            with pytest.raises(RateLimitError) as exc:
                extractor._fetch_page("https://example.invalid/page")
        assert exc.value.retry_after == 30.0

    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_codes_raise_authentication_error(
        self, extractor: _TestSubclass, status: int
    ) -> None:
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _make_response(
                status_code=status
            )
            with pytest.raises(AuthenticationError):
                extractor._fetch_page("https://example.invalid/page")

    def test_5xx_raises_transient_after_retries_exhausted(
        self, extractor: _TestSubclass, fast_sleep: Any
    ) -> None:
        """503 with persistent failure: per-fetch retry budget (3) exhausts then raises.

        ``fast_sleep`` fixture patches ``time.sleep`` to make tenacity's
        exponential-jitter backoff instant — without it this test takes
        ~1.5-2s of real wall time.
        """
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = _make_response(
                status_code=503
            )
            with pytest.raises(TransientExtractionError):
                extractor._fetch_page("https://example.invalid/page")
        # 3 attempts in the per-fetch budget.
        assert mock_client.return_value.__enter__.return_value.get.call_count == 3

    def test_5xx_recovers_within_retry_budget(
        self, extractor: _TestSubclass, fast_sleep: Any
    ) -> None:
        """First 503, second 200: retry recovers, returns 200 response."""
        responses = [
            _make_response(status_code=503),
            _make_response(status_code=200, body=b"<html>ok</html>"),
        ]
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = responses
            response, body = extractor._fetch_page("https://example.invalid/page")
        assert response.status_code == 200
        assert body == b"<html>ok</html>"

    def test_network_error_raises_transient(self, extractor: _TestSubclass) -> None:
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.ConnectError(
                "DNS failure"
            )
            with pytest.raises(TransientExtractionError):
                extractor._fetch_page("https://example.invalid/page")


# ---------------------------------------------------------------------------
# _archive_page envelope shape
# ---------------------------------------------------------------------------


class TestArchivePage:
    def test_archive_accumulates_pages(self, extractor: _TestSubclass) -> None:
        response = _make_response(body=b"<html>1</html>")
        extractor._archive_page("https://example.invalid/p1", response, b"<html>1</html>")
        extractor._archive_page("https://example.invalid/p2", response, b"<html>2</html>")
        assert len(extractor._archived_pages) == 2
        assert extractor._archived_pages[0]["url"] == "https://example.invalid/p1"
        assert extractor._archived_pages[1]["url"] == "https://example.invalid/p2"

    def test_archive_envelope_has_expected_keys(self, extractor: _TestSubclass) -> None:
        response = _make_response(body=b"<html>hello</html>")
        extractor._archive_page("https://example.invalid/p", response, b"<html>hello</html>")
        archived = extractor._archived_pages[0]
        assert set(archived.keys()) == {
            "url",
            "fetched_at",
            "status",
            "html_sha256",
            "body_base64",
        }
        assert archived["status"] == 200
        assert archived["html_sha256"] == hashlib.sha256(b"<html>hello</html>").hexdigest()
        # body_base64 round-trips to original body.
        assert base64.b64decode(archived["body_base64"]) == b"<html>hello</html>"


# ---------------------------------------------------------------------------
# _capture_response_metadata
# ---------------------------------------------------------------------------


class TestHeaders:
    def test_pkg_version_fallback_to_zero(self, extractor: _TestSubclass) -> None:
        """When importlib.metadata.version raises (e.g., package not installed),
        the UA falls back to a sentinel version rather than crashing."""
        with patch(
            "src.extractors._html_scraping._pkg_version",
            side_effect=Exception("package not installed"),
        ):
            headers = extractor._headers()
        assert headers["User-Agent"].startswith("consumer-product-recalls/0.0.0")


class TestCaptureResponseMetadata:
    def test_captures_status_etag_lastmod_sha(self, extractor: _TestSubclass) -> None:
        body = b"<html>page0</html>"
        response = _make_response(
            status_code=200,
            body=body,
            headers={
                "etag": '"abc"',
                "last-modified": "Mon, 16 May 2026 12:00:00 GMT",
                "content-type": "text/html",
            },
        )
        extractor._capture_response_metadata(response, body)
        assert extractor._captured_response_status_code == 200
        assert extractor._captured_response_etag == '"abc"'
        assert extractor._captured_response_last_modified == "Mon, 16 May 2026 12:00:00 GMT"
        assert extractor._captured_response_body_sha256 == hashlib.sha256(body).hexdigest()
        assert extractor._captured_response_headers is not None
        assert extractor._captured_response_headers.get("content-type") == "text/html"

    def test_uscg_style_no_caching_headers_leaves_none(self, extractor: _TestSubclass) -> None:
        """USCG ships no Last-Modified / ETag (Finding K) — those columns stay None."""
        body = b"<html></html>"
        response = _make_response(
            status_code=200,
            body=body,
            headers={"cache-control": "no-store", "content-type": "text/html"},
        )
        extractor._capture_response_metadata(response, body)
        assert extractor._captured_response_etag is None
        assert extractor._captured_response_last_modified is None
        # body sha256 still populated.
        assert extractor._captured_response_body_sha256 == hashlib.sha256(body).hexdigest()


# ---------------------------------------------------------------------------
# land_raw NDJSON shape
# ---------------------------------------------------------------------------


class TestLandRaw:
    def test_land_raw_produces_ndjson(self, extractor: _TestSubclass) -> None:
        """Each line of the uploaded artifact is a complete JSON record."""
        # Populate two archive entries.
        response = _make_response(body=b"<html>1</html>")
        extractor._archive_page("https://example.invalid/p1", response, b"<html>1</html>")
        extractor._archive_page("https://example.invalid/p2", response, b"<html>2</html>")

        path = extractor.land_raw(raw_records=[])

        # The R2 mock returns its canned path.
        assert path == "test_scrape/2026-05-16/abc.ndjson.gz"
        # Inspect the bytes that were sent to R2.
        r2_call = extractor._r2_client.land.call_args  # type: ignore[attr-defined]
        sent_bytes: bytes = r2_call.kwargs["content"]
        suffix: str = r2_call.kwargs["suffix"]
        assert suffix == "ndjson"

        # NDJSON: one JSON object per line, newline-separated.
        lines = sent_bytes.split(b"\n")
        assert len(lines) == 2
        parsed = [json.loads(line) for line in lines]
        assert parsed[0]["url"] == "https://example.invalid/p1"
        assert parsed[1]["url"] == "https://example.invalid/p2"

    def test_empty_archive_raises(self, extractor: _TestSubclass) -> None:
        """A run with zero fetches indicates extract() didn't archive — fail loudly."""
        with pytest.raises(TransientExtractionError, match="zero archived pages"):
            extractor.land_raw(raw_records=[])


# ---------------------------------------------------------------------------
# Abstract method enforcement
# ---------------------------------------------------------------------------


class TestAbstractEnforcement:
    def test_cannot_instantiate_without_parse_listing(self) -> None:
        """A subclass missing _parse_listing_page fails at instantiation."""

        class _Incomplete(HtmlScrapingExtractor[_DummyRecord]):
            source_name: str = "incomplete"
            start_url: str = "https://example.invalid/"

            def extract(self) -> list[dict[str, Any]]:  # pragma: no cover
                return []

            def validate_records(self, raw_records):  # type: ignore[no-untyped-def]
                return [], []  # pragma: no cover

            def check_invariants(self, records):  # type: ignore[no-untyped-def]
                return [], []  # pragma: no cover

            def load_bronze(self, records, quarantined, raw_landing_path):  # type: ignore[no-untyped-def]
                return 0  # pragma: no cover

            def _parse_details_page(  # pragma: no cover
                self, body: bytes, page_url: str
            ) -> dict[str, Any]:
                return {}

            # _parse_listing_page missing.

        with pytest.raises(TypeError, match="abstract"):
            _Incomplete()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# Unused-import suppression (gzip is imported for future cassette-helper tests)
# ---------------------------------------------------------------------------


def _suppress_unused() -> None:
    _ = gzip
    _ = time
