"""Unit tests for scripts/uscg/check_detail_drift_fence.py.

The canary runs the PRODUCTION parser
(``UscgManufacturerDetailExtractor._parse_details_page``) over cached HTML, so
these tests double as a contract check: if that method ever starts touching
``self``, ``parse_with_production_fence`` (which calls it unbound) breaks here.

The load-bearing test is ``test_long_bold_block_trips_fence``: the production
fence raises on ANY unknown bold label, whereas the exploratory probe ignores
labels over 40 chars. That gap is the whole reason this canary exists.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# scripts/ is not on sys.path by default; add the repo root so we can import the
# script as a regular module (mirrors tests/scripts/test_probe_mic_reassignment_rate.py).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.uscg.check_detail_drift_fence import (  # noqa: E402 — sys.path mutated above
    check_page,
    iter_cache_pages,
    parse_with_production_fence,
    run_check,
)
from src.extractors._base import TransientExtractionError  # noqa: E402

_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "uscg" / "sample_manufacturer_details_page.html"

# Minimal 5-cell row mirroring the live layout: [label][value][spacer][label][value].
_GOOD_HTML = (
    b"<table><tr>"
    b"<td><strong>MIC:</strong></td><td>AXY</td><td>&nbsp;</td>"
    b"<td><strong>Company:</strong></td><td>ACME BOATS</td>"
    b"</tr></table>"
)

# An unknown short label — the kind both the probe and the fence catch.
_UNKNOWN_SHORT_HTML = (
    b"<table><tr><td><strong>Warranty Contact:</strong></td><td>x</td></tr></table>"
)

# An unknown LONG (>40 char) bold block — the production fence raises on it, but
# the probe's `if len(norm) <= 40` heuristic would silently ignore it.
_LONG_LABEL = "This is a very long bolded disclaimer that exceeds forty characters"
_UNKNOWN_LONG_HTML = (
    b"<table><tr><td><strong>" + _LONG_LABEL.encode() + b"</strong></td><td>x</td></tr></table>"
)


class TestParseWithProductionFence:
    def test_parses_known_labels(self) -> None:
        # Confirms the unbound call into the real production parser works.
        fields = parse_with_production_fence(_GOOD_HTML, "url")
        assert fields["mic"] == "AXY"
        assert fields["company"] == "ACME BOATS"

    def test_real_fixture_page_is_clean(self) -> None:
        # The actual cached id=655 (AXY) page must pass the production fence and
        # surface its full label set.
        fields = parse_with_production_fence(_FIXTURE.read_bytes(), "url")
        assert fields["mic"] == "AXY"
        assert fields["state"] == "AZ"
        assert fields["zip"] == "86403"
        assert fields["date_modified"] == "5/29/2026"

    def test_unknown_short_label_raises(self) -> None:
        with pytest.raises(TransientExtractionError):
            parse_with_production_fence(_UNKNOWN_SHORT_HTML, "url")

    def test_unknown_long_label_raises(self) -> None:
        # The probe would NOT flag this (>40 chars); the production fence must.
        with pytest.raises(TransientExtractionError):
            parse_with_production_fence(_UNKNOWN_LONG_HTML, "url")


class TestCheckPage:
    def test_clean_page(self) -> None:
        result = check_page("123", _GOOD_HTML)
        assert result.ok is True
        assert result.field_count == 2
        assert result.message is None
        assert result.page_url.endswith("?id=123")

    def test_short_unknown_label_trips(self) -> None:
        result = check_page("9999", _UNKNOWN_SHORT_HTML)
        assert result.ok is False
        assert result.field_count == 0
        assert "warranty contact" in (result.message or "").lower()

    def test_long_bold_block_trips_fence(self) -> None:
        # The differentiator vs the probe: a >40-char bold block trips production.
        result = check_page("8888", _UNKNOWN_LONG_HTML)
        assert result.ok is False
        assert "forty characters" in (result.message or "")


class TestRunCheck:
    def test_aggregates_mixed_batch(self) -> None:
        summary = run_check([("1", _GOOD_HTML), ("2", _UNKNOWN_SHORT_HTML), ("3", _GOOD_HTML)])
        assert summary.total == 3
        assert summary.ok == 2
        assert len(summary.tripped) == 1
        assert summary.tripped[0].page_id == "2"
        assert summary.clean is False

    def test_all_clean_is_clean(self) -> None:
        summary = run_check([("1", _GOOD_HTML), ("2", _GOOD_HTML)])
        assert summary.ok == 2
        assert summary.clean is True

    def test_empty_batch_is_not_clean(self) -> None:
        # Zero pages can't be a clean pass — there's nothing to conclude from.
        summary = run_check([])
        assert summary.total == 0
        assert summary.clean is False


class TestIterCachePages:
    def test_reads_html_sorted_numerically_ignoring_non_html(self, tmp_path: Path) -> None:
        (tmp_path / "10.html").write_bytes(b"<html>10</html>")
        (tmp_path / "2.html").write_bytes(b"<html>2</html>")
        (tmp_path / "notes.txt").write_bytes(b"ignore me")
        pages = iter_cache_pages(tmp_path)
        assert [page_id for page_id, _ in pages] == ["2", "10"]
        assert pages[0][1] == b"<html>2</html>"

    def test_limit_takes_lowest_ids(self, tmp_path: Path) -> None:
        for rid in ("3", "1", "2"):
            (tmp_path / f"{rid}.html").write_bytes(b"<html></html>")
        pages = iter_cache_pages(tmp_path, limit=2)
        assert [page_id for page_id, _ in pages] == ["1", "2"]

    def test_empty_dir_yields_nothing(self, tmp_path: Path) -> None:
        assert iter_cache_pages(tmp_path) == []
