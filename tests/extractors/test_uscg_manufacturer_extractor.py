"""Unit tests for UscgManufacturerExtractor (Phase 5d Step 7 Step 4).

Exercises the parser against the deterministic fixture HTML page at
``tests/fixtures/uscg/sample_manufacturer_listing_page.html`` (promoted
from the 2026-05-30 Step 1 probe at
``data/exploratory/uscg_manufacturers/probes/``). The SQLAlchemy engine
and R2 client are mocked; the BeautifulSoup parser, Pydantic validation,
and the drift-fence logic are exercised for real.

Coverage focuses on the Step 1 observations doc's load-bearing claims:
- Finding A — table structure, header invariant, tr.defaultFont selector
- Finding B — MIC anchor extraction, uscg_directory_id parse, detail_url
- Finding D — Records Found regex side effect
- Finding F.2 — embedded-newline preservation in address (HONDA case)
- Finding F.3 — UNK / "-" / empty-string sentinels preserved verbatim at bronze
- Finding C — _parse_details_page raises NotImplementedError (listing-only)
- Drift fences — header mismatch, wrong cell count
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import TransientExtractionError
from src.extractors.uscg_manufacturer import (
    UscgManufacturerDeepRescanLoader,
    UscgManufacturerExtractor,
)
from src.schemas.uscg_manufacturer import UscgManufacturerRecord

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "uscg"
_LISTING_FIXTURE = _FIXTURE_DIR / "sample_manufacturer_listing_page.html"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


@pytest.fixture
def listing_html() -> bytes:
    return _LISTING_FIXTURE.read_bytes()


def _make_extractor(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cls: type[UscgManufacturerExtractor] = UscgManufacturerExtractor,
) -> UscgManufacturerExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = "uscg_manufacturers/2026-05-30/abc.ndjson.gz"
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.uscg_manufacturer.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return cls(settings=settings, scrape_delay_seconds=0.0)  # type: ignore[arg-type]


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> UscgManufacturerExtractor:
    return _make_extractor(monkeypatch)


# --- Listing parser smoke tests ---


def test_parses_25_rows_from_page_0_fixture(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding A: page 0 has exactly 25 data rows."""
    rows = extractor._parse_listing_page(listing_html, "https://example/page0")
    assert len(rows) == 25


def test_records_found_total_captured_as_side_effect(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding D: ``Records Found: 16263`` footer is captured by the side-effect regex."""
    extractor._parse_listing_page(listing_html, "https://example/page0")
    assert extractor._records_found_total == 16263


def test_first_row_has_expected_brp_shape(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding B + sample row 1: first row should be MIC 101 (BRP)."""
    rows = extractor._parse_listing_page(listing_html, "https://example/page0")
    first = rows[0]
    assert first["mic"] == "101"
    assert first["company"] == "BRP (Rotax/Evinrude)"
    assert first["address"] == "10101 Science Drive"
    assert first["city"] == "Sturtevant"
    assert first["state"] == "WI"


def test_mic_anchor_id_parsed_as_uscg_directory_id(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding B: href ?id= query parameter is captured as a separate integer."""
    rows = extractor._parse_listing_page(listing_html, "https://example/page0")
    # Row 1 has id=1; the displayed MIC is 101 (NOT the id).
    first = rows[0]
    assert first["uscg_directory_id"] == 1
    assert first["mic"] != str(first["uscg_directory_id"])
    # Last row on page 0 has id=25; MIC is 126 (skips 111).
    last = rows[-1]
    assert last["uscg_directory_id"] == 25
    assert last["mic"] == "126"


def test_mic_111_is_skipped_in_displayed_sequence(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding B: MIC sequence skips 111 (proves MIC is the value, not a row index)."""
    rows = extractor._parse_listing_page(listing_html, "https://example/page0")
    mics = [r["mic"] for r in rows]
    assert "110" in mics
    assert "112" in mics
    assert "111" not in mics


def test_detail_url_is_absolutized(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding C: detail_url is absolutized from the relative href."""
    rows = extractor._parse_listing_page(listing_html, "https://example/page0")
    first = rows[0]
    assert first["detail_url"].startswith(
        "https://uscgboating.org/content/manufacturers-identification-detail.php?id="
    )


def test_honda_row_address_preserves_embedded_newline(
    extractor: UscgManufacturerExtractor, listing_html: bytes
) -> None:
    """Finding F.2: HONDA row (id=8) has a literal embedded newline mid-address.

    Bronze preserves verbatim per ADR 0027; silver collapses if needed.
    """
    rows = extractor._parse_listing_page(listing_html, "https://example/page0")
    honda = next((r for r in rows if r.get("uscg_directory_id") == 8), None)
    assert honda is not None, "HONDA row (id=8) missing"
    assert honda["company"].startswith("HONDA")
    # The embedded newline must survive — strip=True only trims outer whitespace.
    assert "\n" in (honda["address"] or ""), (
        f"expected embedded newline preserved in HONDA address; got {honda['address']!r}"
    )


# --- Drift fence tests ---


def test_drift_fence_raises_on_header_mismatch(
    extractor: UscgManufacturerExtractor,
) -> None:
    """Listing-page header mismatch raises TransientExtractionError."""
    html = (
        b"<html><body><table>"
        b"<tr><td><strong>MIC</strong></td><td><strong>WrongHeader</strong></td>"
        b"<td><strong>Address</strong></td><td><strong>City</strong></td>"
        b"<td><strong>State</strong></td></tr>"
        b'<tr class="defaultFont"><td><a href="x?id=1">101</a></td><td>x</td>'
        b"<td>x</td><td>x</td><td>x</td></tr>"
        b"</table></body></html>"
    )
    with pytest.raises(TransientExtractionError, match="schema drift"):
        extractor._parse_listing_page(html, "https://example/drift")


def test_drift_fence_raises_on_wrong_cell_count(
    extractor: UscgManufacturerExtractor,
) -> None:
    """A row with the wrong number of cells raises TransientExtractionError."""
    html = (
        b"<html><body><table>"
        b"<tr><td><strong>MIC</strong></td><td><strong>Company</strong></td>"
        b"<td><strong>Address</strong></td><td><strong>City</strong></td>"
        b"<td><strong>State</strong></td></tr>"
        b'<tr class="defaultFont"><td><a href="x?id=1">101</a></td><td>x</td>'
        b"<td>x</td></tr>"  # only 3 cells — drift
        b"</table></body></html>"
    )
    with pytest.raises(TransientExtractionError, match="data row has"):
        extractor._parse_listing_page(html, "https://example/drift")


def test_missing_table_raises_transient_error(
    extractor: UscgManufacturerExtractor,
) -> None:
    """A page with no <strong>MIC</strong> header in any table's first <tr> raises."""
    html = b"<html><body><table><tr><td>nothing here</td></tr></table></body></html>"
    with pytest.raises(TransientExtractionError, match="no table with 'MIC'"):
        extractor._parse_listing_page(html, "https://example/notable")


def test_stray_strong_mic_elsewhere_does_not_collide_with_table_finder(
    extractor: UscgManufacturerExtractor,
) -> None:
    """Finder scopes <strong>MIC</strong> search to the first <tr> of each candidate table.

    A stray ``<strong>MIC</strong>`` in a footer disclaimer or unrelated nav
    block must not be selected as the manufacturers table.
    """
    html = (
        b"<html><body>"
        # Decoy table — has <strong>MIC</strong> in a non-first row.
        b"<table>"
        b"<tr><td>About this page</td></tr>"
        b"<tr><td><strong>MIC</strong> stands for Manufacturer Identification Code.</td></tr>"
        b"</table>"
        # Real manufacturers table.
        b"<table>"
        b"<tr><td><strong>MIC</strong></td><td><strong>Company</strong></td>"
        b"<td><strong>Address</strong></td><td><strong>City</strong></td>"
        b"<td><strong>State</strong></td></tr>"
        b'<tr class="defaultFont"><td><a href="x?id=1">101</a></td><td>BRP</td>'
        b"<td>10101 Science Drive</td><td>Sturtevant</td><td>WI</td></tr>"
        b"</table></body></html>"
    )
    rows = extractor._parse_listing_page(html, "https://example/decoys")
    assert len(rows) == 1
    assert rows[0]["mic"] == "101"
    assert rows[0]["company"] == "BRP"


def test_placeholder_row_with_empty_id_query_is_skipped(
    extractor: UscgManufacturerExtractor,
) -> None:
    """Empty ``id=`` query param is treated as placeholder (USCG-recalls' Finding L analog)."""
    html = (
        b"<html><body><table>"
        b"<tr><td><strong>MIC</strong></td><td><strong>Company</strong></td>"
        b"<td><strong>Address</strong></td><td><strong>City</strong></td>"
        b"<td><strong>State</strong></td></tr>"
        b'<tr class="defaultFont">'
        b'<td><a href="manufacturers-identification-detail.php?id=">PLACEHOLDER</a></td>'
        b"<td></td><td></td><td></td><td></td></tr>"
        b"</table></body></html>"
    )
    rows = extractor._parse_listing_page(html, "https://example/boundary")
    assert rows == [], "placeholder row with empty id= must be skipped"


# --- Sentinel preservation tests (bronze keeps verbatim per ADR 0027) ---


def test_unk_and_dash_sentinels_preserved_verbatim_at_bronze(
    extractor: UscgManufacturerExtractor,
) -> None:
    """Finding F.3: bronze preserves 'UNK' and '-' verbatim; silver normalizes."""
    html = (
        b"<html><body><table>"
        b"<tr><td><strong>MIC</strong></td><td><strong>Company</strong></td>"
        b"<td><strong>Address</strong></td><td><strong>City</strong></td>"
        b"<td><strong>State</strong></td></tr>"
        b'<tr class="defaultFont"><td><a href="x?id=15015">YCT</a></td>'
        b"<td>-</td><td>-</td><td>-</td><td>FL</td></tr>"
        b'<tr class="defaultFont"><td><a href="x?id=15011">YCP</a></td>'
        b"<td>YU CHING</td><td>UNK</td><td>UNK</td><td></td></tr>"
        b"</table></body></html>"
    )
    rows = extractor._parse_listing_page(html, "https://example/sentinels")
    yct, ycp = rows
    # YCT: '-' sentinels verbatim at bronze.
    assert yct["company"] == "-"
    assert yct["address"] == "-"
    assert yct["city"] == "-"
    assert yct["state"] == "FL"
    # YCP: 'UNK' sentinels verbatim; empty state coerced to None by the
    # parser's `or None` idiom (silver-friendly).
    assert ycp["company"] == "YU CHING"
    assert ycp["address"] == "UNK"
    assert ycp["city"] == "UNK"
    assert ycp["state"] is None


# --- Schema validation tests ---


def test_pydantic_validates_first_row_via_validation_alias(listing_html: bytes) -> None:
    """The parser's dict keys map cleanly to UscgManufacturerRecord via validation_alias."""
    sample = {
        "mic": "101",
        "company": "BRP (Rotax/Evinrude)",
        "address": "10101 Science Drive",
        "city": "Sturtevant",
        "state": "WI",
        "uscg_directory_id": 1,
        "detail_url": "https://uscgboating.org/content/manufacturers-identification-detail.php?id=1",
    }
    record = UscgManufacturerRecord.model_validate(sample)
    assert record.source_recall_id == "101"
    assert record.company_name == "BRP (Rotax/Evinrude)"
    assert record.uscg_directory_id == 1


def test_pydantic_rejects_unknown_field_per_extra_forbid() -> None:
    """ConfigDict(extra='forbid') catches schema drift."""
    sample = {
        "mic": "101",
        "company": "BRP",
        "address": "10101 Science Drive",
        "city": "Sturtevant",
        "state": "WI",
        "uscg_directory_id": 1,
        "detail_url": "https://example/x",
        "unexpected_new_column": "drift",
    }
    from pydantic import ValidationError as PydanticValidationError

    with pytest.raises(PydanticValidationError):
        UscgManufacturerRecord.model_validate(sample)


# --- Listing-only / details-page contract ---


def test_parse_details_page_raises_not_implemented(
    extractor: UscgManufacturerExtractor,
) -> None:
    """Finding C decision: listing-only extraction; details parser must not be invoked."""
    with pytest.raises(NotImplementedError, match="listing-only"):
        extractor._parse_details_page(b"<html/>", "https://example/details")


def test_deep_rescan_loader_short_circuit_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deep-rescan loader's _should_short_circuit always returns False."""
    loader = _make_extractor(monkeypatch, cls=UscgManufacturerDeepRescanLoader)
    # Pass a dummy non-empty page_0_rows; should still short-circuit to False.
    assert loader._should_short_circuit([{"mic": "101"}]) is False
