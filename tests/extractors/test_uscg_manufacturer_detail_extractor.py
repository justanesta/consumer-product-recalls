"""Unit tests for UscgManufacturerDetailExtractor (Phase 5d Step 7 detail / Path B).

Exercises the promoted detail-page parser against the deterministic fixture at
``tests/fixtures/uscg/sample_manufacturer_details_page.html`` (promoted from the
2026-05-30 cached probe page for MIC AXY / id=655). The SQLAlchemy engine and R2
client are mocked; the BeautifulSoup parser, the RAISE-on-unknown-label drift
fence, and Pydantic validation run for real.

Load-bearing coverage:
- §M.2 field set — the ~20 detail fields parse to the right keys.
- The empty-cell-bleed REGRESSION — a blank ``Parent Company`` value followed by
  the ``Parent MIC`` label must yield None, NOT the literal ``"Parent MIC:"``.
- The production drift fence — an unknown bolded label RAISES (the behavioral
  difference from the exploratory probe, which records-and-continues).
- The ``<h2>`` title + HTML-commented ``Comments:`` are NOT treated as labels.
- Pydantic ``validation_alias`` mapping + ``extra='forbid'`` + M/D/YYYY date coercion.
- ``_parse_listing_page`` raises NotImplementedError (detail-only contract).

The work-list SQL (``_work_list`` / ``_build_work_list``) is IO-bound (queries
``uscg_manufacturers_bronze``); it is exercised by the live/integration path and
the user's run, not by these pure-parser unit tests.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors._base import TransientExtractionError
from src.extractors.uscg_manufacturer_detail import UscgManufacturerDetailExtractor
from src.schemas.uscg_manufacturer_detail import UscgManufacturerDetailRecord

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "uscg"
_DETAILS_FIXTURE = _FIXTURE_DIR / "sample_manufacturer_details_page.html"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


@pytest.fixture
def details_html() -> bytes:
    return _DETAILS_FIXTURE.read_bytes()


def _make_extractor(monkeypatch: pytest.MonkeyPatch) -> UscgManufacturerDetailExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = "uscg_manufacturer_details/2026-05-30/abc.ndjson.gz"
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.uscg_manufacturer_detail.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return UscgManufacturerDetailExtractor(settings=settings, scrape_delay_seconds=0.0)  # type: ignore[arg-type]


@pytest.fixture
def extractor(monkeypatch: pytest.MonkeyPatch) -> UscgManufacturerDetailExtractor:
    return _make_extractor(monkeypatch)


# --- Detail parser happy path ---


def test_parses_axy_detail_fields(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """The AXY (id=655) page parses to the expected field set/values (§M.2)."""
    parsed = extractor._parse_details_page(details_html, "https://example/655")
    assert parsed["mic"] == "AXY"
    assert parsed["company"] == "SOSA PERFORMANCE BOATS"
    assert parsed["dba"] == "AXIOM OFFSHORE"
    assert parsed["company_official"] == "Ortega, Steve"
    assert parsed["past_company_1"] == "ARMY SURPLUS OUTLET OF MEMPH (OOB 1978)"
    assert parsed["address"] == "1891 Industrial Blvd"
    assert parsed["city"] == "Lake Havasu"
    assert parsed["state"] == "AZ"
    assert parsed["zip"] == "86403"
    assert parsed["country"] == "USA"
    assert parsed["phone"] == "(925) 321-6717"
    assert parsed["status"] == "In Business"  # collapses the stray tabs/newlines
    assert parsed["in_business"] == "5/29/2026"
    assert parsed["date_modified"] == "5/29/2026"


def test_blank_parent_company_does_not_bleed_parent_mic_label(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """REGRESSION: a blank value cell must yield None, NOT the next label.

    The Parent Company value cell is empty and is followed (after the spacer) by
    the ``Parent MIC`` label cell; the original skip-empties bug produced
    ``parent_company == "Parent MIC:"``.
    """
    parsed = extractor._parse_details_page(details_html, "https://example/655")
    assert parsed["parent_company"] is None
    assert parsed["parent_mic"] is None


def test_blank_value_cells_are_none(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """Empty / &nbsp; / <br/>-only value cells normalize to None at parse time."""
    parsed = extractor._parse_details_page(details_html, "https://example/655")
    assert parsed["fax"] is None
    assert parsed["out_of_business"] is None
    assert parsed["type"] is None  # value cell is <br/><br/> only
    assert parsed["additional_address"] is None
    assert parsed["past_company_2"] is None
    assert parsed["past_company_3"] is None


def test_h2_title_and_commented_fields_not_treated_as_labels(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """The <h2>COMPANY</h2> title (not bold) and the HTML-commented Comments:
    field must not appear as parsed keys — and must not trip the drift fence."""
    parsed = extractor._parse_details_page(details_html, "https://example/655")
    assert "comments" not in parsed
    # All parsed keys are recognized detail fields (no spurious title key).
    assert set(parsed).issubset(
        {
            "mic",
            "company",
            "dba",
            "parent_company",
            "parent_mic",
            "past_company_1",
            "past_company_2",
            "past_company_3",
            "address",
            "city",
            "state",
            "zip",
            "country",
            "phone",
            "fax",
            "status",
            "company_official",
            "in_business",
            "out_of_business",
            "date_modified",
            "type",
            "additional_address",
        }
    )


# --- Drift fence (the production behavior that differs from the probe) ---


def test_drift_fence_raises_on_unknown_label(
    extractor: UscgManufacturerDetailExtractor,
) -> None:
    """An unknown bolded label RAISES TransientExtractionError (vs the probe's continue)."""
    html = (
        b"<html><body><table>"
        b"<tr><td><strong>MIC:</strong></td><td>AXY</td></tr>"
        b"<tr><td><strong>Risk Category:</strong></td><td>High</td></tr>"
        b"</table></body></html>"
    )
    with pytest.raises(TransientExtractionError, match="unknown label"):
        extractor._parse_details_page(html, "https://example/drift")


def test_parse_listing_page_raises_not_implemented(
    extractor: UscgManufacturerDetailExtractor,
) -> None:
    """Detail extractor never walks listing pages — the guard must raise."""
    with pytest.raises(NotImplementedError, match="detail-only"):
        extractor._parse_listing_page(b"<html/>", "https://example/listing")


# --- Work-list --limit cap (cheap dev validation / chunked seeding) ---


def _fake_work(n: int) -> list[dict[str, object]]:
    """N synthetic work-list items shaped like ``_build_work_list`` output."""
    work: list[dict[str, object]] = []
    for i in range(1, n + 1):
        item: dict[str, object] = {
            "mic": f"M{i:03d}",
            "uscg_directory_id": i,
            "detail_url": f"https://example/{i}",
        }
        work.append(item)
    return work


def test_work_list_limit_caps_fetches(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """``--limit`` (work_list_limit) fetches only the first N work-list items."""
    extractor.work_list_limit = 2
    mock_resp = MagicMock(status_code=200, headers={})
    with (
        patch.object(extractor, "_work_list", return_value=_fake_work(5)),
        patch.object(
            extractor, "_fetch_page", return_value=(mock_resp, details_html)
        ) as mock_fetch,
    ):
        records = extractor.extract()
    assert len(records) == 2
    assert mock_fetch.call_count == 2


def test_no_limit_fetches_full_work_list(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """Default (work_list_limit=None) fetches the entire work-list."""
    assert extractor.work_list_limit is None
    mock_resp = MagicMock(status_code=200, headers={})
    with (
        patch.object(extractor, "_work_list", return_value=_fake_work(3)),
        patch.object(
            extractor, "_fetch_page", return_value=(mock_resp, details_html)
        ) as mock_fetch,
    ):
        records = extractor.extract()
    assert len(records) == 3
    assert mock_fetch.call_count == 3


def test_limit_larger_than_work_list_is_safe(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """A limit >= work-list size fetches everything (slice is safe, no error)."""
    extractor.work_list_limit = 50
    mock_resp = MagicMock(status_code=200, headers={})
    with (
        patch.object(extractor, "_work_list", return_value=_fake_work(2)),
        patch.object(
            extractor, "_fetch_page", return_value=(mock_resp, details_html)
        ) as mock_fetch,
    ):
        records = extractor.extract()
    assert len(records) == 2
    assert mock_fetch.call_count == 2


# --- Schema validation ---


def test_pydantic_validates_parsed_record_via_aliases_and_date_coercion(
    extractor: UscgManufacturerDetailExtractor, details_html: bytes
) -> None:
    """Parsed dict (+ extractor-added lineage) validates; dates coerce; aliases map."""
    parsed = extractor._parse_details_page(details_html, "https://example/655")
    parsed["uscg_directory_id"] = 655
    parsed["detail_url"] = (
        "https://uscgboating.org/content/manufacturers-identification-detail.php?id=655"
    )
    record = UscgManufacturerDetailRecord.model_validate(parsed)
    assert record.source_recall_id == "AXY"  # via validation_alias "mic"
    assert record.company_name == "SOSA PERFORMANCE BOATS"  # via validation_alias "company"
    assert record.uscg_directory_id == 655
    assert record.date_modified == datetime(2026, 5, 29, tzinfo=UTC)
    assert record.in_business == datetime(2026, 5, 29, tzinfo=UTC)
    assert record.out_of_business is None


def test_pydantic_rejects_unknown_field_per_extra_forbid() -> None:
    """ConfigDict(extra='forbid') catches schema drift not caught by the parser fence."""
    from pydantic import ValidationError as PydanticValidationError

    sample = {
        "mic": "AXY",
        "detail_url": "https://example/x",
        "unexpected_new_column": "drift",
    }
    with pytest.raises(PydanticValidationError):
        UscgManufacturerDetailRecord.model_validate(sample)


def test_pydantic_requires_detail_url_and_mic() -> None:
    """source_recall_id (mic) and detail_url are required."""
    from pydantic import ValidationError as PydanticValidationError

    with pytest.raises(PydanticValidationError):
        UscgManufacturerDetailRecord.model_validate({"mic": "AXY"})  # missing detail_url
    with pytest.raises(PydanticValidationError):
        UscgManufacturerDetailRecord.model_validate({"detail_url": "https://x"})  # missing mic
