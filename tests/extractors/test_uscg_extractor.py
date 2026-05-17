"""Unit tests for UscgScrapingExtractor and UscgDeepRescanLoader (Phase 5d Step 2).

Exercises the real ``extract → land_raw → validate_records →
check_invariants → load_bronze`` lifecycle against the deterministic
fixture HTML pages at ``tests/fixtures/uscg/``. The SQLAlchemy
engine and R2 client are mocked; the BeautifulSoup parser, Pydantic
validation, and the polite-scraper / drift-fence logic are exercised
for real.

Test coverage targets the Phase 5d Step 2 drift-detection requirement:
"Schema drift on HTML structure changes raises ``ValidationError``."
Listing-page header mismatch, column rename, malformed row count,
unknown details-page label, and date-format flip all surface as
explicit errors or quarantine rows rather than silent corruption.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import TransientExtractionError
from src.extractors.uscg import (
    _USCG_LISTING_URL,
    UscgDeepRescanLoader,
    UscgScrapingExtractor,
)
from src.schemas.uscg import UscgRecallRecord

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "uscg"
_LISTING_FIXTURE = _FIXTURE_DIR / "sample_listing_page.html"
_DETAILS_FIXTURE = _FIXTURE_DIR / "sample_details_page.html"
_PAGINATION_BOUNDARY_FIXTURE = _FIXTURE_DIR / "sample_pagination_boundary.html"

_FAKE_R2_PATH = "uscg/2026-05-16/abc.ndjson.gz"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


def _make_response(body: bytes, status_code: int = 200) -> httpx.Response:
    request = httpx.Request("GET", "https://uscgboating.org/content/recalls.php")
    return httpx.Response(
        status_code,
        request=request,
        content=body,
        headers={"content-type": "text/html; charset=UTF-8"},
    )


@pytest.fixture
def listing_html() -> bytes:
    return _LISTING_FIXTURE.read_bytes()


@pytest.fixture
def details_html() -> bytes:
    return _DETAILS_FIXTURE.read_bytes()


@pytest.fixture
def boundary_html() -> bytes:
    return _PAGINATION_BOUNDARY_FIXTURE.read_bytes()


def _make_extractor(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cls: type[UscgScrapingExtractor] = UscgScrapingExtractor,
) -> UscgScrapingExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.uscg.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        # scrape_delay_seconds=0 in tests to avoid wall-time delays.
        return cls(settings=settings, scrape_delay_seconds=0.0)  # type: ignore[arg-type]


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> UscgScrapingExtractor:
    return _make_extractor(monkeypatch)


@pytest.fixture
def deep_rescan(monkeypatch: pytest.MonkeyPatch) -> UscgDeepRescanLoader:
    return _make_extractor(monkeypatch, cls=UscgDeepRescanLoader)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# _parse_listing_page
# ---------------------------------------------------------------------------


class TestParseListing:
    def test_happy_path_yields_25_rows(
        self, extractor: UscgScrapingExtractor, listing_html: bytes
    ) -> None:
        rows = extractor._parse_listing_page(listing_html, "https://example.invalid/")
        # Page 0 of the real corpus holds 25 records (Finding A).
        assert len(rows) == 25

    def test_first_row_field_shape(
        self, extractor: UscgScrapingExtractor, listing_html: bytes
    ) -> None:
        rows = extractor._parse_listing_page(listing_html, "https://example.invalid/")
        first = rows[0]
        # Expected keys per the parser contract.
        assert set(first.keys()) == {
            "number",
            "mic",
            "company_name",
            "model_name",
            "problem_1",
            "opened_on",
            "details_url",
        }
        # Pin the exact first-row values to detect parser-output regressions.
        assert first["number"] == "26MF0158"
        assert first["mic"] == "123"
        assert first["company_name"] == "VOLVO GROUP / VOLVO PENTA"
        assert first["opened_on"] == "2026-03-03"
        assert first["details_url"].endswith("recalls-details.php?id=26MF0158")
        assert first["details_url"].startswith("https://uscgboating.org/content/")

    def test_boundary_page_returns_empty(
        self, extractor: UscgScrapingExtractor, boundary_html: bytes
    ) -> None:
        """Finding L: pages past the corpus return a placeholder row with empty id."""
        rows = extractor._parse_listing_page(boundary_html, "https://example.invalid/page71")
        assert rows == []

    def test_drift_extra_column_raises(
        self, extractor: UscgScrapingExtractor, listing_html: bytes
    ) -> None:
        """Inject an extra column into the header row — drift fence fires."""
        # Add a new column header to the table-header row.
        patched = listing_html.replace(
            b"<td><strong>Opened On</strong></td>",
            b"<td><strong>Opened On</strong></td><td><strong>Severity</strong></td>",
        )
        with pytest.raises(TransientExtractionError, match="listing-page schema drift"):
            extractor._parse_listing_page(patched, "https://example.invalid/")

    def test_drift_column_rename_raises(
        self, extractor: UscgScrapingExtractor, listing_html: bytes
    ) -> None:
        """Rename a column in the header row — drift fence fires."""
        patched = listing_html.replace(
            b"<td><strong>Opened On</strong></td>",
            b"<td><strong>Date Opened</strong></td>",
        )
        with pytest.raises(TransientExtractionError, match="listing-page schema drift"):
            extractor._parse_listing_page(patched, "https://example.invalid/")

    def test_no_recalls_table_raises(self, extractor: UscgScrapingExtractor) -> None:
        """Page with tables but none containing the 'Number' header → abort."""
        html = b"<html><body><table><tr><td>navigation</td></tr></table></body></html>"
        with pytest.raises(TransientExtractionError, match="no table with 'Number' header"):
            extractor._parse_listing_page(html, "https://example.invalid/")

    def test_empty_recalls_table_raises(self, extractor: UscgScrapingExtractor) -> None:
        """Table with the 'Number' header but no rows whatsoever → abort."""
        # bs4 with lxml normalizes <table></table> with no tr inside as a
        # zero-row table. Construct one with the strong-Number signal in a
        # caption-ish wrapper outside <tr>, no <tr> children.
        html = b"<html><body><table><strong>Number</strong></table></body></html>"
        with pytest.raises(TransientExtractionError, match="has no rows"):
            extractor._parse_listing_page(html, "https://example.invalid/")

    def test_data_row_wrong_cell_count_raises(self, extractor: UscgScrapingExtractor) -> None:
        """A data row with cell count != expected_columns → abort cleanly."""
        # 6 expected columns; row supplies only 3 cells.
        html = (
            b"<html><body><table>"
            b"<tr>"
            b"<td><strong>Number</strong></td><td><strong>MIC</strong></td>"
            b"<td><strong>Company Name</strong></td><td><strong>Model Name</strong></td>"
            b"<td><strong>Problem 1</strong></td><td><strong>Opened On</strong></td>"
            b"</tr>"
            b'<tr class="defaultFont">'
            b"<td>only</td><td>three</td><td>cells</td>"
            b"</tr>"
            b"</table></body></html>"
        )
        with pytest.raises(TransientExtractionError, match="data row has 3 cells; expected 6"):
            extractor._parse_listing_page(html, "https://example.invalid/")

    def test_data_row_without_anchor_skipped(self, extractor: UscgScrapingExtractor) -> None:
        """A data row whose first cell has no <a> tag is silently skipped."""
        html = (
            b"<html><body><table>"
            b"<tr>"
            b"<td><strong>Number</strong></td><td><strong>MIC</strong></td>"
            b"<td><strong>Company Name</strong></td><td><strong>Model Name</strong></td>"
            b"<td><strong>Problem 1</strong></td><td><strong>Opened On</strong></td>"
            b"</tr>"
            b'<tr class="defaultFont">'
            b"<td>not-an-anchor</td><td>x</td><td>x</td><td>x</td><td>x</td><td>2026-01-01</td>"
            b"</tr>"
            b"</table></body></html>"
        )
        rows = extractor._parse_listing_page(html, "https://example.invalid/")
        assert rows == []


# ---------------------------------------------------------------------------
# _parse_details_page
# ---------------------------------------------------------------------------


class TestParseDetails:
    def test_happy_path_25cg0017(
        self, extractor: UscgScrapingExtractor, details_html: bytes
    ) -> None:
        """25CG0017 — well-populated sample, exercises most fields."""
        parsed = extractor._parse_details_page(
            details_html,
            "https://uscgboating.org/content/recalls-details.php?id=25CG0017",
        )
        # Pin specific values per Finding B observations.
        assert parsed["number"] == "25CG0017"
        assert parsed["mic"] == "NLP"
        assert parsed["company_name"] == "TIDEWATER BOATS LLC"
        assert parsed["company_official"] == "jlu"
        assert parsed["model_name"] == "TIDEWATER 180 CC BAY"
        assert parsed["model_year"] == "2025"
        assert parsed["problem_1"] == "Stability test (starboard"
        assert parsed["problem_2"] is None
        assert parsed["hin"] == "NLPEC117K425"
        assert parsed["case_open_date"] == "6/4/2025"
        assert parsed["disposition"] == "Open"
        assert parsed["case_close_date"] == "7/23/2025"
        assert parsed["units"] == "401"
        assert parsed["campaign_open_date"] == "8/19/2025"
        assert parsed["boat_type"] is None
        assert parsed["campaign_close_date"] is None
        assert parsed["severity"] is None
        assert parsed["last_date"] == "12/2/2025"

    def test_unknown_label_raises(
        self, extractor: UscgScrapingExtractor, details_html: bytes
    ) -> None:
        """Inject a previously-unseen label — Phase 5d Step 2 drift fence fires."""
        patched = details_html.replace(
            b"<strong>Severity:</strong>",
            b"<strong>Risk Category:</strong>",
        )
        with pytest.raises(TransientExtractionError, match="unknown label"):
            extractor._parse_details_page(
                patched,
                "https://uscgboating.org/content/recalls-details.php?id=25CG0017",
            )

    def test_spacer_cell_between_label_and_value(self, extractor: UscgScrapingExtractor) -> None:
        """Defensive while-loop skips spacer <td>&nbsp;</td> cells between
        the label-td and the value-td, in case USCG inserts unusual spacers."""
        html = (
            b"<html><body><table><tr>"
            b'<td><span class="defaultFont"><strong>Number</strong></span></td>'
            b'<td width="1">&nbsp;</td>'  # spacer between label and value
            b'<td><span class="defaultFont">26MF0158</span></td>'
            b"</tr></table></body></html>"
        )
        parsed = extractor._parse_details_page(html, "https://example.invalid/")
        assert parsed.get("number") == "26MF0158"

    def test_value_td_missing_yields_none(self, extractor: UscgScrapingExtractor) -> None:
        """Label with no following td at all → key set to None, not omitted."""
        html = (
            b"<html><body><table><tr>"
            b'<td><span class="defaultFont"><strong>Number</strong></span></td>'
            b"</tr></table></body></html>"
        )
        parsed = extractor._parse_details_page(html, "https://example.invalid/")
        assert parsed.get("number") is None

    def test_value_td_without_defaultfont_span_yields_none(
        self, extractor: UscgScrapingExtractor
    ) -> None:
        """Value td exists with a span, but the span lacks class='defaultFont'.
        find_all('span') is non-empty so the spacer-skip loop doesn't run,
        but find('span', class_='defaultFont') returns None."""
        html = (
            b"<html><body><table><tr>"
            b'<td><span class="defaultFont"><strong>Number</strong></span></td>'
            b'<td><span class="other-class">26MF0158</span></td>'
            b"</tr></table></body></html>"
        )
        parsed = extractor._parse_details_page(html, "https://example.invalid/")
        assert parsed.get("number") is None


# ---------------------------------------------------------------------------
# Pydantic schema integration — date format coercion + drift fences
# ---------------------------------------------------------------------------


class TestSchemaCoercion:
    def _base_dict(self) -> dict[str, Any]:
        """A minimal valid record dict for happy-path schema tests."""
        return {
            "number": "26MF0158",
            "company_name": "VOLVO GROUP / VOLVO PENTA",
            "opened_on": "2026-03-03",
            "mic": "123",
            "model_name": "VOLVO PENTA AUTOPILO",
            "problem_1": None,
            "details_url": ("https://uscgboating.org/content/recalls-details.php?id=26MF0158"),
            "case_open_date": "3/3/2026",
        }

    def test_listing_date_format_coerces(self) -> None:
        record = UscgRecallRecord.model_validate(self._base_dict())
        assert record.opened_on == datetime(2026, 3, 3, tzinfo=UTC)

    def test_details_date_format_coerces(self) -> None:
        record = UscgRecallRecord.model_validate(self._base_dict())
        assert record.case_open_date == datetime(2026, 3, 3, tzinfo=UTC)

    def test_zero_padded_details_date_accepted(self) -> None:
        d = self._base_dict()
        d["case_open_date"] = "03/03/2026"
        record = UscgRecallRecord.model_validate(d)
        assert record.case_open_date == datetime(2026, 3, 3, tzinfo=UTC)

    def test_date_format_flip_listing_to_details_routes_to_quarantine(self) -> None:
        """If the listing parser emits an M/D/YYYY-formatted date by mistake,
        Pydantic's strict mode + the _UscgListingDate validator reject it."""
        from pydantic import ValidationError

        d = self._base_dict()
        d["opened_on"] = "3/3/2026"  # details format, not listing format
        with pytest.raises(ValidationError):
            UscgRecallRecord.model_validate(d)

    def test_unknown_extra_field_rejected(self) -> None:
        """Pydantic ``extra='forbid'`` rejects unknown keys (additive drift fence)."""
        from pydantic import ValidationError

        d = self._base_dict()
        d["unexpected_new_field"] = "future drift"
        with pytest.raises(ValidationError):
            UscgRecallRecord.model_validate(d)

    def test_listing_date_validator_accepts_datetime_directly(self) -> None:
        """A datetime input (e.g., already-parsed value from a re-validation
        flow) is returned with UTC stamped if naive, unchanged if tz-aware."""
        from src.schemas.uscg import _parse_uscg_listing_date

        naive = datetime(2026, 3, 3)
        out = _parse_uscg_listing_date(naive)
        assert out == datetime(2026, 3, 3, tzinfo=UTC)
        # Already tz-aware → returned as-is.
        aware = datetime(2026, 3, 3, tzinfo=UTC)
        assert _parse_uscg_listing_date(aware) is aware

    def test_listing_date_validator_raises_on_bad_input(self) -> None:
        """Non-string, non-datetime input (e.g., an int) raises ValueError."""
        from src.schemas.uscg import _parse_uscg_listing_date

        with pytest.raises(ValueError, match="USCG listing date"):
            _parse_uscg_listing_date(42)

    def test_details_date_validator_accepts_datetime_directly(self) -> None:
        from src.schemas.uscg import _parse_uscg_details_date

        naive = datetime(2026, 3, 3)
        assert _parse_uscg_details_date(naive) == datetime(2026, 3, 3, tzinfo=UTC)
        aware = datetime(2026, 3, 3, tzinfo=UTC)
        assert _parse_uscg_details_date(aware) is aware

    def test_details_date_validator_raises_on_bad_input(self) -> None:
        from src.schemas.uscg import _parse_uscg_details_date

        with pytest.raises(ValueError, match="USCG details date"):
            _parse_uscg_details_date(42)

    def test_nullable_details_date_passes_through_none(self) -> None:
        """The nullable variant returns None for None/empty without parsing."""
        from src.schemas.uscg import _parse_nullable_uscg_details_date

        assert _parse_nullable_uscg_details_date(None) is None
        assert _parse_nullable_uscg_details_date("") is None
        # Non-empty string still parses.
        assert _parse_nullable_uscg_details_date("3/3/2026") == datetime(2026, 3, 3, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Full lifecycle integration — extract through load_bronze
# ---------------------------------------------------------------------------


class TestExtractLifecycle:
    def _setup_http_mock(
        self,
        listing_body: bytes,
        details_body: bytes,
        boundary_body: bytes,
    ) -> MagicMock:
        """Build an httpx.Client mock that returns listing/details by URL.

        URL routing: a request whose URL contains ``recalls-details.php``
        returns the details fixture; otherwise the listing fixture.
        The walk exits when page 1 returns the boundary fixture
        (empty placeholder).
        """
        mock_client = MagicMock()

        # The walk fetches page 0 listing (25 rows), then 25 details pages,
        # then page 1 which returns the boundary fixture (empty).
        page_calls: list[bytes] = []
        # We need to count GETs in order. Use side_effect with a callable
        # so URL routing works correctly.

        def _route(url: str, **kwargs: Any) -> httpx.Response:
            if "recalls-details.php" in url:
                return _make_response(details_body)
            # Listing page request — track count, return boundary on 2nd hit.
            page_calls.append(url.encode("utf-8"))
            if len(page_calls) == 1:
                return _make_response(listing_body)
            return _make_response(boundary_body)

        mock_client.return_value.__enter__.return_value.get.side_effect = _route
        return mock_client

    def test_extract_walks_pages_and_fetches_details(
        self,
        extractor: UscgScrapingExtractor,
        listing_html: bytes,
        details_html: bytes,
        boundary_html: bytes,
    ) -> None:
        """End-to-end extract: 1 listing page (25 rows) + 25 details fetches → 25 records."""
        mock_client = self._setup_http_mock(listing_html, details_html, boundary_html)
        with patch("httpx.Client", mock_client):
            records = extractor.extract()
        assert len(records) == 25
        # Each record has merged listing + details fields. The first
        # record's number comes from listing; details overwrites with
        # the details-page Number which is from the 25CG0017 fixture.
        # (In practice these match per the assert-equality invariant
        # below; the fixture mismatch is a test artifact.)
        first = records[0]
        assert "details_url" in first  # listing-derived, carries through
        # case_open_date is details-only.
        assert "case_open_date" in first

    def test_lifecycle_through_validate_check_load(
        self,
        extractor: UscgScrapingExtractor,
        listing_html: bytes,
        details_html: bytes,
        boundary_html: bytes,
    ) -> None:
        """Full lifecycle: extract → validate → check_invariants → load_bronze.

        BronzeLoader is mocked so we don't hit a real DB; just exercise
        the wiring between the lifecycle steps. Note that due to the
        fixture mismatch (listing rows have varied IDs, but the details
        fixture is always 25CG0017's), the year-prefix invariant will
        reject many rows — that's expected test behavior, exercising
        the quarantine routing.
        """
        mock_client = self._setup_http_mock(listing_html, details_html, boundary_html)

        # Mock BronzeLoader.load to avoid DB; engine.begin is also mocked.
        with (
            patch("httpx.Client", mock_client),
            patch("src.extractors.uscg.BronzeLoader") as mock_loader_cls,
        ):
            mock_loader = MagicMock()
            mock_loader.load.return_value = 5  # arbitrary insert count
            mock_loader_cls.return_value = mock_loader

            raw_records = extractor.extract()
            extractor._current_landing_path = _FAKE_R2_PATH  # would normally be set by land_raw
            valid, schema_rejects = extractor.validate_records(raw_records)
            passing, invariant_rejects = extractor.check_invariants(valid)
            count = extractor.load_bronze(
                passing,
                schema_rejects + invariant_rejects,
                _FAKE_R2_PATH,
            )

        # The fixture mismatch (different listing rows + same details
        # fixture) means some rows fail invariants. We just verify the
        # plumbing connects end-to-end.
        assert count == 5
        mock_loader.load.assert_called_once()


# ---------------------------------------------------------------------------
# Year-prefix invariant
# ---------------------------------------------------------------------------


class TestYearPrefixInvariant:
    def _make_record(self, number: str, opened_on_str: str) -> UscgRecallRecord:
        return UscgRecallRecord.model_validate(
            {
                "number": number,
                "company_name": "TEST COMPANY",
                "opened_on": opened_on_str,
                "details_url": "https://example.invalid/details",
            }
        )

    def test_matching_prefix_passes(self, extractor: UscgScrapingExtractor) -> None:
        """26MF0158 + 2026-03-03 → year-prefix '26' matches year-suffix '26' → passes."""
        record = self._make_record("26MF0158", "2026-03-03")
        passing, quarantined = extractor.check_invariants([record])
        assert passing == [record]
        assert quarantined == []

    def test_mismatched_prefix_quarantined(self, extractor: UscgScrapingExtractor) -> None:
        """26MF0158 + 2025-03-03 → year-prefix '26' vs year-suffix '25' → quarantined."""
        record = self._make_record("26MF0158", "2025-03-03")
        passing, quarantined = extractor.check_invariants([record])
        assert passing == []
        assert len(quarantined) == 1
        assert "year-prefix mismatch" in quarantined[0].failure_reason

    def test_cg_prefix_25_with_2025_passes(self, extractor: UscgScrapingExtractor) -> None:
        """25CG0017 + 2025-06-04 → year-prefix '25' matches → passes."""
        record = self._make_record("25CG0017", "2025-06-04")
        passing, _ = extractor.check_invariants([record])
        assert passing == [record]

    def test_short_source_recall_id_short_circuits(self) -> None:
        """source_recall_id < 2 chars returns None from the invariant — the
        null-id check handles the structural problem elsewhere. We test the
        helper directly because check_null_source_id would short-circuit the
        invariant chain before _check_year_prefix_consistency runs."""
        from src.extractors.uscg import _check_year_prefix_consistency

        # 1-character source_recall_id — not empty (so not caught by
        # check_null_source_id), but too short to extract a year prefix.
        record = UscgRecallRecord.model_validate(
            {
                "number": "X",
                "company_name": "TEST",
                "opened_on": "2026-01-01",
                "details_url": "https://example.invalid/d",
            }
        )
        assert _check_year_prefix_consistency(record) is None

    def test_null_opened_on_short_circuits(self) -> None:
        """`opened_on` is nullable per Finding A scope caveat (38/38 in
        sample, ~2.2% of corpus). When it's None, the year-prefix invariant
        cannot compare and returns None — Step 1.5 corpus probe + a
        potential follow-up invariant would handle null-opened_on rows
        explicitly if the corpus surfaces any."""
        from src.extractors.uscg import _check_year_prefix_consistency

        record = UscgRecallRecord.model_validate(
            {
                "number": "26MF0158",
                "company_name": "TEST",
                "opened_on": None,
                "details_url": "https://example.invalid/d",
            }
        )
        assert record.opened_on is None
        assert _check_year_prefix_consistency(record) is None


# ---------------------------------------------------------------------------
# validate_records — ValidationError quarantine routing
# ---------------------------------------------------------------------------


class TestValidateRecordsQuarantine:
    def test_validation_error_routes_to_quarantine_with_source_id(
        self, extractor: UscgScrapingExtractor
    ) -> None:
        """A raw record that fails Pydantic strict mode gets routed to
        quarantine; the source_recall_id falls through from the dict's
        ``number`` key (validation_alias)."""
        extractor._current_landing_path = _FAKE_R2_PATH
        bad_record = {
            "number": "26MF0999",
            "company_name": "TEST",
            "opened_on": "not-a-date",  # bad date triggers ValidationError
            "details_url": "https://example.invalid/d",
        }
        valid, quarantined = extractor.validate_records([bad_record])
        assert valid == []
        assert len(quarantined) == 1
        q = quarantined[0]
        assert q.source_recall_id == "26MF0999"
        assert q.failure_stage == "validate_records"
        assert q.raw_landing_path == _FAKE_R2_PATH

    def test_validation_error_with_missing_number_quarantines_with_none(
        self, extractor: UscgScrapingExtractor
    ) -> None:
        """If a malformed row lacks the 'number' key entirely, the quarantine
        record stores source_recall_id=None (no crash)."""
        extractor._current_landing_path = _FAKE_R2_PATH
        bad_record = {
            # 'number' missing entirely — also triggers a required-field error
            "company_name": "TEST",
            "opened_on": "2026-01-01",
            "details_url": "https://example.invalid/d",
        }
        valid, quarantined = extractor.validate_records([bad_record])
        assert valid == []
        assert quarantined[0].source_recall_id is None


# ---------------------------------------------------------------------------
# land_raw caches the landing path
# ---------------------------------------------------------------------------


class TestLandRawCachesPath:
    def test_land_raw_sets_current_landing_path(self, extractor: UscgScrapingExtractor) -> None:
        """The UscgScrapingExtractor override of land_raw caches the R2
        path on `_current_landing_path` so quarantine records can reference
        it later in the lifecycle."""
        # Pre-populate at least one archived page so super().land_raw doesn't
        # raise the empty-archive guard.
        extractor._archive_page(
            "https://example.invalid/p",
            _make_response(b"<html></html>"),
            b"<html></html>",
        )
        path = extractor.land_raw(raw_records=[])
        assert path == _FAKE_R2_PATH
        assert extractor._current_landing_path == _FAKE_R2_PATH


# ---------------------------------------------------------------------------
# extract() guards — MAX_PAGES + MAX_INCREMENTAL_RECORDS
# ---------------------------------------------------------------------------


class TestExtractGuards:
    def test_max_pages_guard_raises_when_loop_never_breaks(
        self, extractor: UscgScrapingExtractor
    ) -> None:
        """If pagination never reaches an empty page within ``_MAX_PAGES``
        iterations, the while-else clause fires. Easiest to trigger by
        patching _MAX_PAGES to 0 so the condition is false from the start —
        no iterations run, else clause fires unconditionally."""
        with (
            patch("src.extractors.uscg._MAX_PAGES", 0),
            pytest.raises(TransientExtractionError, match="exceeded 0 pages"),
        ):
            extractor.extract()

    def test_max_incremental_records_guard_raises(
        self,
        extractor: UscgScrapingExtractor,
        listing_html: bytes,
        details_html: bytes,
        boundary_html: bytes,
    ) -> None:
        """When the while loop exits cleanly (empty page) but records >
        _MAX_INCREMENTAL_RECORDS, the post-loop guard fires."""

        def _route(url: str, **kwargs: Any) -> httpx.Response:
            if "recalls-details.php" in url:
                return _make_response(details_html)
            # First listing page = real fixture (25 rows); second = boundary.
            page_calls.append(url)
            if len(page_calls) == 1:
                return _make_response(listing_html)
            return _make_response(boundary_html)

        page_calls: list[str] = []
        mock_client = MagicMock()
        mock_client.return_value.__enter__.return_value.get.side_effect = _route
        with (
            patch("src.extractors.uscg._MAX_INCREMENTAL_RECORDS", 3),
            patch("httpx.Client", mock_client),
            pytest.raises(TransientExtractionError, match="exceeds guard of 3"),
        ):
            extractor.extract()


# ---------------------------------------------------------------------------
# _record_run — extraction_runs row population
# ---------------------------------------------------------------------------


class TestRecordRun:
    def _result(self) -> Any:
        from src.extractors._base import ExtractionResult

        return ExtractionResult(
            source="uscg",
            run_id="test-run-id",
            records_fetched=1,
            records_landed=1,
            records_valid=1,
            records_rejected_validate=0,
            records_rejected_invariants=0,
            records_loaded=1,
            raw_landing_path=_FAKE_R2_PATH,
        )

    def test_success_with_response_captured_inserts_full_row(
        self, extractor: UscgScrapingExtractor
    ) -> None:
        """All forensic + result fields land on the inserted row."""
        from datetime import datetime as dt

        extractor._captured_response_status_code = 200
        extractor._captured_response_etag = '"abc"'
        extractor._captured_response_last_modified = "Mon, 16 May 2026 12:00:00 GMT"
        extractor._captured_response_body_sha256 = "deadbeef"
        extractor._captured_response_headers = {"content-type": "text/html"}

        mock_conn = MagicMock()
        with patch.object(extractor._engine, "begin") as mock_begin:
            mock_begin.return_value.__enter__.return_value = mock_conn
            extractor._record_run(
                run_id="test-run-id",
                started_at=dt(2026, 5, 16, tzinfo=UTC),
                status="success",
                result=self._result(),
                change_type="routine",
            )
        # One insert against the extraction_runs table.
        assert mock_conn.execute.call_count == 1
        insert_stmt = mock_conn.execute.call_args.args[0]
        values = insert_stmt.compile().params
        assert values["source"] == "uscg"
        assert values["status"] == "success"
        assert values["records_extracted"] == 1
        assert values["records_inserted"] == 1
        assert values["records_rejected"] == 0
        assert values["response_status_code"] == 200
        assert values["response_etag"] == '"abc"'
        assert values["response_body_sha256"] == "deadbeef"

    def test_failed_status_without_response_captured(
        self, extractor: UscgScrapingExtractor
    ) -> None:
        """Failure status before any HTTP fetch — response columns omitted."""
        from datetime import datetime as dt

        # No _captured_response_status_code set (stays None).
        mock_conn = MagicMock()
        with patch.object(extractor._engine, "begin") as mock_begin:
            mock_begin.return_value.__enter__.return_value = mock_conn
            extractor._record_run(
                run_id="test-run-id",
                started_at=dt(2026, 5, 16, tzinfo=UTC),
                status="failed",
                error_message="boom",
                change_type="routine",
            )
        insert_stmt = mock_conn.execute.call_args.args[0]
        values = insert_stmt.compile().params
        assert values["status"] == "failed"
        assert values["error_message"] == "boom"
        # response_* columns absent from the params dict (only included when
        # _captured_response_status_code is non-None).
        assert "response_status_code" not in values or values["response_status_code"] is None

    def test_db_failure_is_swallowed_and_logged(
        self, extractor: UscgScrapingExtractor, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An exception during conn.execute is caught — bronze write doesn't
        depend on extraction_runs success; the failure surfaces in a
        structlog warning (per the implementation_plan diagnostic-logging fix)."""
        from datetime import datetime as dt

        import structlog

        with patch.object(extractor._engine, "begin") as mock_begin:
            mock_begin.return_value.__enter__.return_value.execute.side_effect = RuntimeError(
                "simulated DB outage"
            )
            with structlog.testing.capture_logs() as logs:
                extractor._record_run(
                    run_id="test-run-id",
                    started_at=dt(2026, 5, 16, tzinfo=UTC),
                    status="success",
                    error_message=None,
                    result=self._result(),
                )
        # No exception escaped; a warning was logged with diagnostic fields.
        warnings = [log for log in logs if log["log_level"] == "warning"]
        assert warnings, "Expected a structlog warning on extraction_run insert failure"
        record_failed = next(
            (log for log in warnings if log["event"] == "extraction_run.record_failed"),
            None,
        )
        assert record_failed is not None
        assert record_failed["error_type"] == "RuntimeError"
        assert "simulated DB outage" in record_failed["error"]


# ---------------------------------------------------------------------------
# Deep-rescan loader symmetry
# ---------------------------------------------------------------------------


class TestDeepRescan:
    def test_deep_rescan_inherits_parsers(
        self,
        deep_rescan: UscgDeepRescanLoader,
        listing_html: bytes,
    ) -> None:
        """Deep-rescan uses the same parsers — Finding from Plan-agent
        symmetry-only motivation."""
        rows = deep_rescan._parse_listing_page(listing_html, "https://example.invalid/")
        assert len(rows) == 25

    def test_deep_rescan_load_bronze_skips_freshness_touch(
        self,
        deep_rescan: UscgDeepRescanLoader,
    ) -> None:
        """Deep-rescan load_bronze must not call _touch_freshness."""
        with patch("src.extractors.uscg.BronzeLoader") as mock_loader_cls:
            mock_loader = MagicMock()
            mock_loader.load.return_value = 0
            mock_loader_cls.return_value = mock_loader
            with patch.object(deep_rescan, "_touch_freshness") as mock_freshness:
                deep_rescan.load_bronze([], [], _FAKE_R2_PATH)
                mock_freshness.assert_not_called()


# ---------------------------------------------------------------------------
# Registry + CLI wiring smoke (no httpx, no DB)
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_extractor_registered(self) -> None:
        from src.config.source_registry import EXTRACTOR_BY_SOURCE_NAME

        assert EXTRACTOR_BY_SOURCE_NAME["uscg"] is UscgScrapingExtractor

    def test_deep_rescan_registered(self) -> None:
        from src.config.source_registry import DEEP_RESCAN_BY_SOURCE_NAME

        assert DEEP_RESCAN_BY_SOURCE_NAME["uscg"] is UscgDeepRescanLoader

    def test_yaml_config_loads(self) -> None:
        """The shipped config/sources/uscg.yaml parses against the
        HtmlScrapingSourceConfig discriminated-union variant."""
        from src.config.source_loader import load_source_config

        config = load_source_config("uscg")
        assert config.source_name == "uscg"
        # source_type discriminator routes to HtmlScrapingSourceConfig
        assert config.source_type == "html_scraping"  # type: ignore[union-attr]
        assert config.start_url == _USCG_LISTING_URL  # type: ignore[union-attr]
