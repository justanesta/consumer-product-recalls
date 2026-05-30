"""VCR integration tests for ``UscgManufacturerExtractor`` (Phase 5d Step 7).

Live-recorded cassettes replay real USCG boat-manufacturer-directory HTML
responses and verify both the parser (real HTTP headers per Finding E, real
ASCII-only encoding per Finding F.7, real Cache-Control directives) and the
full extract → validate → check_invariants → load_bronze lifecycle wired
against real bytes. Hand-constructed respx tests cover error-handling paths
the live site won't produce on demand (429 / 503 / 401).

USCG manufacturers has no auth and no cache-busting query params, so no
per-source ``vcr_config`` override is needed beyond the session-level config
in ``tests/conftest.py``.

Cassette inventory:
  Live-recorded (real USCG responses, captured once):
    test_real_listing_page_parses.yaml          — page 0 of the manufacturer
                                                  directory
    test_pagination_boundary_returns_empty.yaml — page past the corpus end
                                                  (Finding L analog: placeholder
                                                  row with empty ``id=`` query
                                                  parameter)
    test_response_metadata_captured.yaml        — page 0 forensic capture
                                                  (Finding E: no ETag, no
                                                  Last-Modified, Cache-Control
                                                  ``no-store, no-cache,
                                                  must-revalidate``)
    test_lifecycle_against_real_bytes.yaml      — 3 listing pages for the
                                                  full-lifecycle test

  Hand-constructed (respx mocks, no YAML file):
    test_transient_503_per_fetch_retry_recovers — recovers on 2nd attempt
    test_transient_503_exhausts_retry_budget    — raises after 3 attempts
    test_429_raises_rate_limit_error_with_retry_after
    test_401_raises_authentication_error        — fail fast, no retry

Listing-only — no details cassette. Per Finding C, the manufacturer directory
has a per-row detail URL (``manufacturers-identification-detail.php?id=N``)
but ``UscgManufacturerExtractor.extract()`` does NOT fetch detail pages — the
listing carries all 5 fields the schema captures (MIC, Company, Address, City,
State). ``_parse_details_page`` raises ``NotImplementedError`` as defense-in-
depth. So no analog to ``test_real_details_page_parses.yaml`` is needed.

Lifecycle-cassette design note: the manufacturer walk terminates only on the
empty-row boundary signal (the parser returns ``[]`` when out-of-range pages
emit placeholder rows with empty ``id=`` query parameter — see Step 3 page-651
boundary warning that confirmed Open Question #10). Recording the full
651-page corpus would be ~25 MB. Instead ``test_lifecycle_against_real_bytes``
records 3 listing pages and patches ``_parse_listing_page`` with a
``side_effect`` so the first 2 calls return real parsed rows and the 3rd
call returns ``[]``. The walk's existing ``if not listing_rows: break`` then
exits cleanly, the lifecycle completes through ``load_bronze``, and
production code is untouched. Expected cassette size ~120 KB (much smaller
than recalls' 361 KB because manufacturers has no per-record details fetch).

To record cassettes (requires network access; no auth needed for USCG):
    pytest --vcr-record=all tests/integration/test_uscg_manufacturer_live_cassettes.py \\
        -k "real_listing or pagination or response_metadata or lifecycle"

Commit the generated YAML files under
``tests/fixtures/cassettes/uscg_manufacturers/``.
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
from src.extractors.uscg_manufacturer import UscgManufacturerExtractor

_LISTING_PAGE_0 = (
    "https://uscgboating.org/content/manufacturers-identification.php?pageNum_manufacturers=0"
)
# Page far beyond the corpus boundary (current corpus is 651 pages, idx 0..650).
# USCG returns a placeholder row with an empty ``id=`` query parameter past the
# end (confirmed empirically at page 651 during the Step 3 first extraction —
# the defensive Finding L guard logged a ``parse.empty_mic`` warning exactly
# once on that page). Parser filters those rows, returns ``[]``, walk loop
# interprets as end-of-pagination.
_LISTING_PAGE_BEYOND = (
    "https://uscgboating.org/content/manufacturers-identification.php?pageNum_manufacturers=999"
)

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "uscg_manufacturers")


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
            "pytest --vcr-record=all "
            "tests/integration/test_uscg_manufacturer_live_cassettes.py"
        )


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> UscgManufacturerExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = "uscg_manufacturers/cassette-test/placeholder.ndjson.gz"
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.uscg_manufacturer.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        # scrape_delay_seconds=0 so the per-fetch throttle is a no-op in tests.
        return UscgManufacturerExtractor(settings=settings, scrape_delay_seconds=0.0)


# ---------------------------------------------------------------------------
# Live-recorded cassettes — VCR replays real USCG bytes through the parser
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_real_listing_page_parses(extractor: UscgManufacturerExtractor) -> None:
    """Page 0 of the USCG manufacturer directory parses cleanly against live HTML."""
    response, body = extractor._fetch_page(_LISTING_PAGE_0)
    assert response.status_code == 200
    rows = extractor._parse_listing_page(body, _LISTING_PAGE_0)
    # Per Finding A: exactly 25 rows per page across all probed pages. Allow a
    # small range in case USCG ever changes the page size; the test is about
    # parser stability, not exact count.
    assert 20 <= len(rows) <= 30
    first = rows[0]
    # Per Finding B + the schema, the parser emits these 7 keys.
    assert set(first.keys()) == {
        "mic",
        "company",
        "address",
        "city",
        "state",
        "uscg_directory_id",
        "detail_url",
    }
    # Page 0 row 1 is empirically MIC 101 = BRP (Bombardier Recreational
    # Products) per the Step 1 probe. The MIC and company string are stable
    # at this position in the alphabetical sort.
    assert first["mic"] == "101"
    assert first["company"] and "BRP" in first["company"]
    # ``uscg_directory_id`` is parsed from the anchor's ``?id=`` query param.
    # Page 0 row 1 has id=1.
    assert first["uscg_directory_id"] == 1
    # ``detail_url`` is absolutized at parse time.
    assert first["detail_url"].startswith("https://uscgboating.org/content/")
    assert "manufacturers-identification-detail.php?id=" in first["detail_url"]


@pytest.mark.vcr
def test_pagination_boundary_returns_empty(extractor: UscgManufacturerExtractor) -> None:
    """Past-corpus-end pages emit a placeholder row with empty ``id=`` query parameter.

    Resolves Open Question #10 (manufacturer observations doc): out-of-range
    pages return HTTP 200 + placeholder row; NOT 404, NOT redirect. The
    defensive Finding L guard in ``_parse_listing_page`` filters placeholder
    rows whose anchor ``href`` ends with ``id=``. Empty parsed list signals
    end-of-pagination to the walk loop. Confirmed empirically at page 651
    during the Step 3 first extraction.
    """
    response, body = extractor._fetch_page(_LISTING_PAGE_BEYOND)
    assert response.status_code == 200
    rows = extractor._parse_listing_page(body, _LISTING_PAGE_BEYOND)
    assert rows == []


@pytest.mark.vcr
def test_response_metadata_captured(extractor: UscgManufacturerExtractor) -> None:
    """Finding E — USCG manufacturers returns no ``ETag`` and no ``Last-Modified``.

    The forensic-capture path correctly records that absence as NULL in the
    captured-response state; the body SHA is still populated. Stronger than
    recalls' Finding K (which lacked ``must-revalidate``): the manufacturer
    directory's ``Cache-Control`` directive adds ``must-revalidate`` to
    ``no-store, no-cache`` — the extractor doesn't act on the directive but
    the captured headers preserve it for forensics.
    """
    response, body = extractor._fetch_page(_LISTING_PAGE_0)
    extractor._capture_response_metadata(response, body)
    assert extractor._captured_response_status_code == 200
    # Finding E: USCG response carries no validator headers.
    assert extractor._captured_response_etag is None
    assert extractor._captured_response_last_modified is None
    # Body hash always populated for 200 responses.
    assert extractor._captured_response_body_sha256
    assert len(extractor._captured_response_body_sha256) == 64  # sha256 hex
    # Headers dict is populated and includes the Cache-Control directive.
    assert extractor._captured_response_headers
    headers_lower = {k.lower(): v for k, v in extractor._captured_response_headers.items()}
    assert "content-type" in headers_lower
    # Finding E specifically — Cache-Control on this source includes the
    # ``must-revalidate`` directive that the recalls page lacks.
    cache_control = headers_lower.get("cache-control", "")
    assert "no-store" in cache_control
    assert "no-cache" in cache_control


@pytest.mark.vcr
def test_lifecycle_against_real_bytes(extractor: UscgManufacturerExtractor) -> None:
    """Full lifecycle: extract → validate → check_invariants → load_bronze
    against real listing bytes recorded from USCG.

    Termination mechanism — see module docstring's "Lifecycle-cassette
    design note":
      - ``max_pages=3`` bounds the walk loop (well below the production
        ``_MAX_PAGES=2000`` guard but high enough that the natural break-exit
        below fires first).
      - ``_parse_listing_page`` is patched with ``side_effect`` so the first
        2 calls return real parsed rows and the 3rd call returns ``[]``. The
        walk's existing ``if not listing_rows: break`` then exits cleanly
        without hitting the ``while-else`` raise.
      - ``_should_short_circuit`` is forced to ``False`` so the page-0
        precheck doesn't preempt the walk.

    Cassette records: 3 listing fetches (pages 0/1/2) — no details fetches
    because the manufacturer extractor is listing-only (Finding C). The 3rd
    listing page's bytes are recorded but the patched parser ignores them
    (~33% cassette overhead) — a tolerable trade-off for natural termination
    without recording the full 651-page corpus.
    """
    real_parse = extractor._parse_listing_page
    parse_call_count = 0

    def _injected_empty_after_two_pages(body: bytes, page_url: str) -> list[dict[str, Any]]:
        nonlocal parse_call_count
        parse_call_count += 1
        if parse_call_count > 2:
            return []  # inject the end-of-pagination signal
        return real_parse(body, page_url)

    extractor.max_pages = 3

    mock_loader = MagicMock()
    mock_loader.load.return_value = 50

    with (
        patch.object(extractor, "_parse_listing_page", side_effect=_injected_empty_after_two_pages),
        patch.object(extractor, "_should_short_circuit", return_value=False),
        patch("src.extractors.uscg_manufacturer.BronzeLoader", return_value=mock_loader),
    ):
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
    # Page-0 forensic capture happened (Finding E: ETag + Last-Modified NULL).
    assert extractor._captured_response_status_code == 200
    assert extractor._captured_response_body_sha256 is not None
    # Finding K side effect populated from the real ``Records Found:`` cell —
    # the manufacturer corpus is ~16,263 records, much larger than recalls.
    assert extractor._records_found_total is not None
    assert extractor._records_found_total > 10_000  # whole-corpus total


# ---------------------------------------------------------------------------
# Hand-constructed respx tests — exercise per-fetch retry / 429 / 401 paths
# the live site won't produce on demand
# ---------------------------------------------------------------------------


def test_transient_503_per_fetch_retry_recovers(
    extractor: UscgManufacturerExtractor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-fetch retry recovers when a transient 503 succeeds on the retry.

    The ``_PER_FETCH_RETRY`` budget at ``_html_scraping.py`` is 3 attempts ×
    short backoff — a single 503 transient on page 200 of a 651-page walk
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
    extractor: UscgManufacturerExtractor, monkeypatch: pytest.MonkeyPatch
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
    extractor: UscgManufacturerExtractor, monkeypatch: pytest.MonkeyPatch
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
    extractor: UscgManufacturerExtractor, monkeypatch: pytest.MonkeyPatch
) -> None:
    """401 is non-retryable — fail fast without burning the retry budget.

    USCG manufacturers has no auth in practice (Finding E — no auth-bearing
    headers in requests). This test exists to confirm the base-class behavior
    is wired correctly should USCG ever start gating with 401.
    """
    monkeypatch.setattr("time.sleep", lambda _: None)
    with respx.mock(assert_all_called=False) as router:
        route = router.get(_LISTING_PAGE_0).mock(return_value=httpx.Response(401))
        with pytest.raises(AuthenticationError, match="401"):
            extractor._fetch_page(_LISTING_PAGE_0)
    # 401/403 don't match the retry predicate — exactly one attempt.
    assert route.call_count == 1
