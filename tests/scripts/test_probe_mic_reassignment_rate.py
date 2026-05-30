"""Unit tests for scripts/uscg/probe_mic_reassignment_rate.py.

Pure-function coverage of the detail-page parser + reassignment helpers. The
fixtures mirror the real 5-cell row structure observed in the cached detail
HTML for ids 655 (AXY) and 1786 (COP):
``[left-label][left-value][&nbsp; spacer][right-label][right-value]``.

The load-bearing regression here is ``test_blank_field_does_not_bleed_next_label``:
the original parser skipped empty value cells and walked into the next label,
producing ``parent_company == "Parent MIC:"``. The value must be the label
cell's immediate next sibling, empty-or-not.
"""

from __future__ import annotations

import sys
from pathlib import Path

# scripts/ is not on sys.path by default; add the repo root so we can import
# the script as a regular module (mirrors tests/scripts/test_refresh_user_agents.py).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.uscg.probe_mic_reassignment_rate import (  # noqa: E402 — sys.path mutated above
    has_oob_marker,
    is_reassigned,
    oob_years,
    parse_detail,
)

# A reassigned record (AXY): one Past Company with an OOB year, a blank
# Parent Company immediately followed by the Parent MIC label (the bleed trap),
# and a blank right-column value (Status populated, Company Official populated).
_AXY_HTML = b"""
<html><body>
<table>
  <tr>
    <td><strong>MIC:</strong></td><td>AXY</td><td>&nbsp;</td>
    <td><strong>Status:</strong></td><td>
\tIn Business    </td>
  </tr>
  <tr>
    <td><strong>Company:</strong></td><td>SOSA PERFORMANCE BOATS</td><td>&nbsp;</td>
    <td><strong>Company Official:</strong></td><td>Ortega, Steve</td>
  </tr>
  <tr>
    <td><strong>DBA</strong></td><td>AXIOM OFFSHORE</td>
    <td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
  </tr>
  <tr>
    <td><strong>Parent Company</strong></td><td></td><td>&nbsp;</td>
    <td><strong>Parent MIC:</strong></td><td></td>
  </tr>
  <tr>
    <td><strong>Past Company 1:</strong></td>
    <td>ARMY SURPLUS OUTLET OF MEMPH (OOB 1978)</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
  </tr>
  <tr>
    <td><strong>Past Company 2:</strong></td><td></td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
  </tr>
  <tr>
    <td><strong>Date Modified:</strong></td><td>5/29/2026</td>
    <td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
  </tr>
</table>
</body></html>
"""

# A never-reassigned record: no Past Company entries at all.
_FRESH_HTML = b"""
<html><body>
<table>
  <tr>
    <td><strong>MIC:</strong></td><td>ZZZ</td><td>&nbsp;</td>
    <td><strong>Status:</strong></td><td>In Business</td>
  </tr>
  <tr>
    <td><strong>Company:</strong></td><td>NEW BOATS LLC</td><td>&nbsp;</td>
    <td><strong>Company Official:</strong></td><td>Doe, Jane</td>
  </tr>
  <tr>
    <td><strong>Past Company 1:</strong></td><td></td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
  </tr>
</table>
</body></html>
"""


class TestParseDetail:
    def test_extracts_populated_fields(self) -> None:
        fields, _ = parse_detail(_AXY_HTML)
        assert fields["mic"] == "AXY"
        assert fields["company"] == "SOSA PERFORMANCE BOATS"
        assert fields["dba"] == "AXIOM OFFSHORE"
        assert fields["company_official"] == "Ortega, Steve"
        assert fields["date_modified"] == "5/29/2026"
        # Right-column value with stray tabs/newlines collapses cleanly.
        assert fields["status"] == "In Business"

    def test_blank_field_does_not_bleed_next_label(self) -> None:
        # Regression: a blank value cell must yield "", NOT the next label.
        fields, _ = parse_detail(_AXY_HTML)
        assert fields["parent_company"] == ""
        assert fields["parent_mic"] == ""

    def test_past_company_parsed_verbatim(self) -> None:
        fields, _ = parse_detail(_AXY_HTML)
        assert fields["past_company_1"] == "ARMY SURPLUS OUTLET OF MEMPH (OOB 1978)"
        assert fields["past_company_2"] == ""

    def test_parses_out_of_business_top_level_field(self) -> None:
        # The current holder's own Out of Business date (distinct from a
        # Past Company OOB marker). Also checks an empty right-column value
        # stays "" rather than bleeding the next label.
        html = (
            b"<table><tr>"
            b"<td><strong>Out of Business:</strong></td><td>5/11/1994</td>"
            b"<td>&nbsp;</td><td><strong>Status:</strong></td><td></td>"
            b"</tr></table>"
        )
        fields, _ = parse_detail(html)
        assert fields["out_of_business"] == "5/11/1994"
        assert fields["status"] == ""

    def test_unknown_labels_reported_not_fatal(self) -> None:
        html = b"<table><tr><td><strong>Weird New Field:</strong></td><td>x</td></tr></table>"
        fields, unknown = parse_detail(html)
        assert "weird new field" in unknown
        # Recognized keys absent; parser does not raise on the unknown label.
        assert fields == {}


class TestIsReassigned:
    def test_true_when_past_company_present(self) -> None:
        fields, _ = parse_detail(_AXY_HTML)
        assert is_reassigned(fields) is True

    def test_false_when_no_past_company(self) -> None:
        fields, _ = parse_detail(_FRESH_HTML)
        assert is_reassigned(fields) is False


class TestHasOobMarker:
    def test_true_with_oob_year(self) -> None:
        assert has_oob_marker({"past_company_1": "ARMY SURPLUS OUTLET (OOB 1978)"}) is True

    def test_true_with_bare_oob_no_year(self) -> None:
        assert has_oob_marker({"past_company_2": "COPALIS BOAT SHOP (OOB)"}) is True

    def test_false_for_bare_predecessor_name(self) -> None:
        # FOUR WINNS' "SAF-T-MATE" has no (OOB) marker — possible rename, not recycle.
        assert has_oob_marker({"past_company_1": "SAF-T-MATE", "past_company_2": ""}) is False

    def test_false_when_no_past_company(self) -> None:
        assert (
            has_oob_marker({"past_company_1": "", "past_company_2": "", "past_company_3": ""})
            is False
        )


class TestOobYears:
    def test_extracts_four_digit_year(self) -> None:
        fields, _ = parse_detail(_AXY_HTML)
        assert oob_years(fields) == [1978]

    def test_oob_without_year_yields_nothing(self) -> None:
        # "(OOB)" with no year (the COPALIS case) contributes no year.
        fields = {"past_company_1": "COPALIS BOAT SHOP (OOB)", "past_company_2": ""}
        assert oob_years(fields) == []

    def test_multiple_past_companies_collect_each_year(self) -> None:
        fields = {
            "past_company_1": "FOO (OOB 2008)",
            "past_company_2": "BAR (OOB 1995)",
            "past_company_3": "",
        }
        assert oob_years(fields) == [2008, 1995]
