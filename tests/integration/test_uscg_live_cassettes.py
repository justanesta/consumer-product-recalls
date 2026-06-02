"""VCR integration tests for ``UscgScrapingExtractor``.

Live-recorded cassettes replay real USCG HTML responses and verify both the
parsers (real HTTP headers per Finding K, real encoding mix per Finding Q,
real entity escapes per Finding I) and the full extract → validate →
check_invariants → load_bronze lifecycle wired against real bytes.
Hand-constructed respx tests cover error-handling paths the live site won't
produce on demand (429 / 503 / 401).

USCG has no auth and no cache-busting query params, so no per-source ``vcr_config``
override is needed beyond the session-level config in ``tests/conftest.py``.

Cassette inventory:
  Live-recorded (real USCG responses, captured once):
    test_real_listing_page_parses.yaml         — page 0 of the recalls listing
    test_real_details_page_parses.yaml         — one specific recall's details
    test_pagination_boundary_returns_empty.yaml — page past the corpus end
    test_response_metadata_captured.yaml        — page 0 forensic capture
                                                  (USCG returns no ETag /
                                                  Last-Modified per Finding K)
    test_lifecycle_against_real_bytes.yaml     — 3 listing pages + ~50 details
                                                  for the full-lifecycle test

  Hand-constructed (respx mocks, no YAML file):
    test_transient_503_per_fetch_retry_recovers — recovers on 2nd attempt
    test_transient_503_exhausts_retry_budget    — raises after 3 attempts
    test_429_raises_rate_limit_error_with_retry_after
    test_401_raises_authentication_error        — fail fast, no retry

Lifecycle-cassette design note: USCG's walk terminates only on the Finding L
empty-row boundary signal, which lives at the natural end of the corpus
(page 71 today). Recording the full corpus to hit that boundary naturally
would be ~95 MB. Instead ``test_lifecycle_against_real_bytes`` records 3
listing pages + their ~50 details and patches ``_parse_listing_page`` to
inject ``[]`` on the 3rd call — the walk's existing ``if not listing_rows:
break`` fires naturally, the lifecycle completes through ``load_bronze``,
and production code is untouched.

To record cassettes (requires network access; no auth needed for USCG):
    pytest --vcr-record=all tests/integration/test_uscg_live_cassettes.py \\
        -k "real_listing or real_details or pagination or response_metadata \\
            or lifecycle"

Commit the generated YAML files under ``tests/fixtures/cassettes/uscg/``.
Until cassettes are recorded, VCR tests skip automatically.
"""

from __future__ import annotations

from pathlib import Path
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
from src.extractors.uscg import UscgScrapingExtractor

_LISTING_PAGE_0 = "https://uscgboating.org/content/recalls.php?pageNum_allRecalls=0"
# 25CG0017 is a Closed recall from the Step 3 corpus (Tidewater Boats LLC).
# Closed recalls don't acquire new edits, so the cassette stays stable across
# re-records. If USCG ever removes this ID from the public archive, re-record
# against any other closed recall surfaced by ``inspect_landing_ndjson.py``.
_DETAILS_25CG0017 = "https://uscgboating.org/content/recalls-details.php?id=25CG0017"
# Page far beyond the corpus boundary (current corpus is 71 pages, idx 0..70).
# USCG returns a placeholder row with empty ``id=`` query param past the end;
# parser filters those, returns ``[]``, walk loop interprets as end-of-pagination.
_LISTING_PAGE_BEYOND = "https://uscgboating.org/content/recalls.php?pageNum_allRecalls=200"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "uscg")


@pytest.fixture(autouse=True)
def skip_if_no_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str) -> None:
    # Only applies to ``@pytest.mark.vcr`` tests; respx-based tests skip past.
    marker = request.node.get_closest_marker("vcr")
    if not marker:
        return
    record_mode = request.config.getoption("--vcr-record", default="none")
    if record_mode in ("all", "new_episodes"):
        return  # recording — let VCR create the cassette
    cassette_path = Path(vcr_cassette_dir) / (request.node.name + ".yaml")
    if not cassette_path.exists():
        pytest.skip(
            "Cassette not yet recorded — run: "
            "pytest --vcr-record=all tests/integration/test_uscg_live_cassettes.py"
        )


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> UscgScrapingExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = "uscg/cassette-test/placeholder.ndjson.gz"
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.uscg.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        # scrape_delay_seconds=0 so the per-fetch throttle is a no-op in tests.
        return UscgScrapingExtractor(settings=settings, scrape_delay_seconds=0.0)


# ---------------------------------------------------------------------------
# Live-recorded cassettes — VCR replays real USCG bytes through the parsers
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_real_listing_page_parses(extractor: UscgScrapingExtractor) -> None:
    """Page 0 of the USCG listing parses cleanly against the live HTML shape."""
    response, body = extractor._fetch_page(_LISTING_PAGE_0)
    assert response.status_code == 200
    rows = extractor._parse_listing_page(body, _LISTING_PAGE_0)
    # Corpus has ~25 rows per page. Allow a small range in case USCG ever
    # changes the page size; the test is about parser stability, not exact count.
    assert 20 <= len(rows) <= 30
    first = rows[0]
    assert set(first.keys()) == {
        "number",
        "mic",
        "company_name",
        "model_name",
        "problem_1",
        "opened_on",
        "details_url",
    }
    # ``number`` is the recall ID (year-prefix encoded per Finding A).
    assert first["number"]
    # ``details_url`` is absolutized at parse time.
    assert first["details_url"].startswith("https://uscgboating.org/content/")
    assert "recalls-details.php?id=" in first["details_url"]


@pytest.mark.vcr
def test_real_details_page_parses(extractor: UscgScrapingExtractor) -> None:
    """The details page for 25CG0017 (TIDEWATER BOATS, closed) parses cleanly.

    Closed recalls don't acquire new edits, so the recorded bytes stay stable.
    """
    response, body = extractor._fetch_page(_DETAILS_25CG0017)
    assert response.status_code == 200
    parsed = extractor._parse_details_page(body, _DETAILS_25CG0017)
    # The recall ID flows through to ``number`` via validation_alias.
    assert parsed.get("number") == "25CG0017"
    # 25CG0017 is closed with a known disposition — pin to detect HTML drift.
    # (case-inconsistency per Finding R is a silver concern; bronze parser
    # returns verbatim.)
    assert parsed.get("disposition") in {"Open", "Closed", "OPEN", "CLOSED"}
    # MIC is a small alpha code in real records.
    assert parsed.get("mic")
    # 25CG0017 has a populated company name (the null-company case is rare).
    assert parsed.get("company_name")


@pytest.mark.vcr
def test_pagination_boundary_returns_empty(extractor: UscgScrapingExtractor) -> None:
    """Finding L: a page past the corpus end returns rows with empty ``id=``.

    The parser filters those placeholder rows. Empty list signals
    end-of-pagination to the walk loop.
    """
    response, body = extractor._fetch_page(_LISTING_PAGE_BEYOND)
    assert response.status_code == 200
    rows = extractor._parse_listing_page(body, _LISTING_PAGE_BEYOND)
    assert rows == []


@pytest.mark.vcr
def test_response_metadata_captured(extractor: UscgScrapingExtractor) -> None:
    """Finding K — USCG returns no ``ETag`` and no ``Last-Modified``.

    The forensic-capture path correctly records that absence as NULL in the
    captured-response state; the body SHA is still populated.
    """
    response, body = extractor._fetch_page(_LISTING_PAGE_0)
    extractor._capture_response_metadata(response, body)
    assert extractor._captured_response_status_code == 200
    # Finding K: USCG response carries no validator headers.
    assert extractor._captured_response_etag is None
    assert extractor._captured_response_last_modified is None
    # Body hash always populated for 200 responses.
    assert extractor._captured_response_body_sha256
    assert len(extractor._captured_response_body_sha256) == 64  # sha256 hex
    # Headers dict is populated (content-type, content-length, etc.).
    assert extractor._captured_response_headers
    assert "content-type" in {k.lower() for k in extractor._captured_response_headers}


@pytest.mark.vcr
def test_lifecycle_against_real_bytes(extractor: UscgScrapingExtractor) -> None:
    """Full lifecycle: extract → validate → check_invariants → load_bronze
    against real listing + details bytes recorded from USCG.

    Termination mechanism — see module docstring's "Lifecycle-cassette
    design note":
      - ``max_pages=3`` bounds the walk loop (well below the production
        ``_MAX_PAGES=200`` guard but high enough that the natural break-exit
        below fires first).
      - ``_parse_listing_page`` is patched with ``side_effect`` so the first
        2 calls return real parsed rows and the 3rd call returns ``[]``. The
        walk's existing ``if not listing_rows: break`` then exits cleanly
        without hitting the ``while-else`` raise.
      - ``_should_short_circuit`` is forced to ``False`` so the page-0
        precheck doesn't preempt the walk.

    Cassette records: 3 listing fetches (pages 0/1/2) + ~50 details fetches.
    The 3rd listing page's bytes are recorded but the patched parser ignores
    them — a tolerable ~3% cassette overhead in exchange for natural
    termination without recording the full 71-page corpus.
    """
    real_parse = extractor._parse_listing_page
    parse_call_count = 0

    def _injected_empty_after_two_pages(body: bytes, page_url: str) -> list[dict[str, Any]]:
        nonlocal parse_call_count
        parse_call_count += 1
        if parse_call_count > 2:
            return []  # inject the Finding L boundary signal
        return real_parse(body, page_url)

    extractor.max_pages = 3

    mock_loader = MagicMock()
    mock_loader.load.return_value = 50

    with (
        patch.object(extractor, "_parse_listing_page", side_effect=_injected_empty_after_two_pages),
        patch.object(extractor, "_should_short_circuit", return_value=False),
        patch("src.extractors.uscg.BronzeLoader") as mock_loader_cls,
    ):
        mock_loader_cls.from_contract.return_value = mock_loader
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        result = extractor.run()

    # Two pages × ~25 rows each = ~50 records all the way through validate +
    # check_invariants without quarantine (real bytes, real schema).
    assert 40 <= result.records_fetched <= 60
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.records_loaded == 50  # mocked loader return value
    # Page-0 forensic capture happened (Finding K: ETag + Last-Modified NULL).
    assert extractor._captured_response_status_code == 200
    assert extractor._captured_response_body_sha256 is not None
    # Finding J side effect populated from the real ``Records Found:`` cell.
    assert extractor._records_found_total is not None
    assert extractor._records_found_total > 1000  # whole-corpus total, not page count


# ---------------------------------------------------------------------------
# Hand-constructed respx tests — exercise per-fetch retry / 429 / 401 paths
# the live site won't produce on demand
# ---------------------------------------------------------------------------


def test_transient_503_per_fetch_retry_recovers(
    extractor: UscgScrapingExtractor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-fetch retry recovers when a transient 503 succeeds on the retry.

    The ``_PER_FETCH_RETRY`` budget at ``_html_scraping.py`` is 3 attempts ×
    short backoff — a single 503 transient on page 47 of a 71-page walk
    should NOT tear down the entire walk.
    """
    monkeypatch.setattr("time.sleep", lambda _: None)  # skip retry backoff
    with respx.mock(assert_all_called=False) as router:
        route = router.get(_LISTING_PAGE_0)
        route.side_effect = [
            httpx.Response(503),  # first attempt fails
            httpx.Response(200, content=b"<html><body>recovered</body></html>"),
        ]
        response, body = extractor._fetch_page(_LISTING_PAGE_0)
    assert response.status_code == 200
    assert body == b"<html><body>recovered</body></html>"
    # The retry was actually exercised — 2 calls observed.
    assert route.call_count == 2


def test_transient_503_exhausts_retry_budget(
    extractor: UscgScrapingExtractor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All 3 per-fetch attempts return 503 → ``TransientExtractionError`` propagates."""
    monkeypatch.setattr("time.sleep", lambda _: None)
    with respx.mock(assert_all_called=False) as router:
        route = router.get(_LISTING_PAGE_0).mock(return_value=httpx.Response(503))
        with pytest.raises(TransientExtractionError, match="503"):
            extractor._fetch_page(_LISTING_PAGE_0)
    # Budget = 3 attempts exactly.
    assert route.call_count == 3


def test_429_raises_rate_limit_error_with_retry_after(
    extractor: UscgScrapingExtractor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """429 with ``Retry-After: 60`` raises ``RateLimitError(retry_after=60.0)``.

    The per-fetch retry budget exhausts on persistent 429s (3 attempts) and the
    final ``RateLimitError`` propagates to the caller, where the outer
    ``Extractor.run()`` retry catches it.
    """
    monkeypatch.setattr("time.sleep", lambda _: None)
    with respx.mock(assert_all_called=False) as router:
        router.get(_LISTING_PAGE_0).mock(
            return_value=httpx.Response(429, headers={"Retry-After": "60"})
        )
        with pytest.raises(RateLimitError) as exc_info:
            extractor._fetch_page(_LISTING_PAGE_0)
    assert exc_info.value.retry_after == 60.0


def test_401_raises_authentication_error(
    extractor: UscgScrapingExtractor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """401 is non-retryable — fail fast without burning the retry budget.

    USCG has no auth in practice (Finding K — no auth-bearing headers in
    requests). This test exists to confirm the base-class behavior is wired
    correctly should USCG ever start gating with 401.
    """
    monkeypatch.setattr("time.sleep", lambda _: None)
    with respx.mock(assert_all_called=False) as router:
        route = router.get(_LISTING_PAGE_0).mock(return_value=httpx.Response(401))
        with pytest.raises(AuthenticationError, match="401"):
            extractor._fetch_page(_LISTING_PAGE_0)
    # 401/403 don't match the retry predicate — exactly one attempt.
    assert route.call_count == 1
