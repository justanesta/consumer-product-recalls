from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.fda.audit.probe_tier2_shorts import (  # noqa: E402  — sys.path mutated above
    _project_for_save,
    build_population_report,
    build_verdict_report,
    compare_short_to_full,
    get_field,
    probed_from_payload,
    rows_from_result,
)


class TestRowsFromResult:
    def test_list_shape(self) -> None:
        assert rows_from_result({"RESULT": [{"PRODUCTID": "1"}]}) == [{"PRODUCTID": "1"}]

    def test_columnar_shape(self) -> None:
        body = {"RESULT": {"COLUMNS": ["A", "B"], "DATA": [["x", "y"], ["p", "q"]]}}
        assert rows_from_result(body) == [{"A": "x", "B": "y"}, {"A": "p", "B": "q"}]

    def test_missing_result(self) -> None:
        assert rows_from_result({"STATUSCODE": 412}) == []

    def test_result_is_scalar(self) -> None:
        assert rows_from_result({"RESULT": "oops"}) == []

    def test_columnar_skips_non_list_rows(self) -> None:
        body = {"RESULT": {"COLUMNS": ["A"], "DATA": [["x"], "junk"]}}
        assert rows_from_result(body) == [{"A": "x"}]


class TestGetField:
    def test_case_insensitive(self) -> None:
        assert get_field({"PRODUCTID": "5"}, "productid") == "5"
        assert get_field({"productid": "5"}, "PRODUCTID") == "5"

    def test_missing_returns_none(self) -> None:
        assert get_field({"PRODUCTID": "5"}, "codeinfoshort") is None


class TestCompareShortToFull:
    def test_identical(self) -> None:
        out = compare_short_to_full("Choc bars", "Choc bars")
        assert out["verdict"] == "identical"
        assert out["is_clean_prefix"] is True

    def test_clean_prefix_midword(self) -> None:
        out = compare_short_to_full("Choco", "Chocolate bars")
        assert out["verdict"] == "clean_prefix_midword"
        assert out["is_clean_prefix"] is True
        assert out["cut_at_word_boundary"] is False
        assert out["short_len"] == 5
        assert out["full_len"] == 14

    def test_clean_prefix_word_boundary(self) -> None:
        # cut lands exactly at the space after a complete word
        out = compare_short_to_full("Chocolate", "Chocolate bars")
        assert out["verdict"] == "clean_prefix_word_boundary"
        assert out["cut_at_word_boundary"] is True

    def test_trailing_space_counts_as_boundary(self) -> None:
        out = compare_short_to_full("Chocolate ", "Chocolate bars")
        assert out["verdict"] == "clean_prefix_word_boundary"
        assert out["cut_at_word_boundary"] is True

    def test_ascii_ellipsis_truncation(self) -> None:
        out = compare_short_to_full("Chocolate...", "Chocolate bars")
        assert out["verdict"] == "ellipsis_truncation"
        assert out["has_trailing_ellipsis"] is True
        assert out["is_clean_prefix"] is True

    def test_unicode_ellipsis_truncation(self) -> None:
        out = compare_short_to_full("Chocolate…", "Chocolate bars")
        assert out["verdict"] == "ellipsis_truncation"
        assert out["has_trailing_ellipsis"] is True

    def test_not_a_prefix(self) -> None:
        out = compare_short_to_full("Vanilla", "Chocolate bars")
        assert out["verdict"] == "not_prefix"
        assert out["is_clean_prefix"] is False

    def test_short_longer_than_full_is_not_prefix(self) -> None:
        out = compare_short_to_full("Chocolate bars deluxe", "Chocolate")
        assert out["verdict"] == "not_prefix"

    def test_null_short(self) -> None:
        assert compare_short_to_full(None, "Chocolate")["verdict"] == "null"
        assert compare_short_to_full("", "Chocolate")["verdict"] == "null"

    def test_null_full(self) -> None:
        out = compare_short_to_full("Choc", None)
        assert out["verdict"] == "null"
        assert out["short_null"] is False
        assert out["full_null"] is True


class TestContentSubset:
    def test_clean_prefix_is_content_subset(self) -> None:
        assert compare_short_to_full("Chocolate", "Chocolate bars")["content_subset"] is True

    def test_newline_stripped_short_is_not_prefix_but_is_content_subset(self) -> None:
        # FDA strips the newline: "Molift\nProduct" -> "MoliftProduct"
        out = compare_short_to_full("Brand: MoliftProduct", "Brand: Molift\nProduct Name")
        assert out["verdict"] == "not_prefix"  # raw exact test fails on the missing \n
        assert out["content_subset"] is True  # but the content is the same, reflowed

    def test_tab_to_space_short_is_content_subset(self) -> None:
        out = compare_short_to_full("A B C", "A\tB C D")
        assert out["content_subset"] is True

    def test_genuinely_different_text_is_not_content_subset(self) -> None:
        out = compare_short_to_full("Vanilla wafers", "Chocolate bars")
        assert out["verdict"] == "not_prefix"
        assert out["content_subset"] is False

    def test_null_has_no_content_subset(self) -> None:
        assert compare_short_to_full(None, "Chocolate")["content_subset"] is None


class TestBuildVerdictReport:
    def test_empty(self) -> None:
        assert "No records fetched" in build_verdict_report([])

    def test_includes_verdicts_and_aggregate(self) -> None:
        record = {
            "PRODUCTID": "219875",
            "PRODUCTDESCRIPTIONSHORT": "Choco",
            "PRODUCTDESCRIPTIONTXT": "Chocolate bars",
            "RECALLREASONSHORT": "Undeclared milk",
            "PRODUCTSHORTREASONTXT": "Undeclared milk allergen",
            "CODEINFOSHORT": "LOT123",
            "CODEINFORMATION": "LOT123" + "X" * 1000,
            "PRODUCTDESCRIPTIONINDICATOR": "Y",
            "RECALLREASONINDICATOR": "N",
            "CODEINFOINDICATOR": "Y",
        }
        report = build_verdict_report([("219875", record)])
        assert "productdescriptionshort <-> productdescriptiontxt" in report
        assert "=== Aggregate ===" in report
        assert "clean_prefix_midword" in report  # Choco -> Chocolate (mid-word)
        assert "productdescriptionindicator='Y'" in report

    def test_large_full_field_is_not_dumped(self) -> None:
        record = {
            "CODEINFOSHORT": "LOT123",
            "CODEINFORMATION": "LOT123" + "X" * 5000,
        }
        report = build_verdict_report([("1", record)])
        # the 5000-char field must never reach stdout verbatim
        assert "X" * 200 not in report


def _record(**fields: object) -> dict[str, object]:
    return dict(fields)


class TestBuildPopulationReport:
    def test_empty(self) -> None:
        assert "No records fetched" in build_population_report([])

    def test_prevalence_and_denominator(self) -> None:
        probed = [
            # 1 of 3 has productdescriptionshort, and it is a clean prefix
            (
                "a",
                _record(
                    PRODUCTDESCRIPTIONSHORT="Chocolate", PRODUCTDESCRIPTIONTXT="Chocolate bars"
                ),
            ),
            ("b", _record(PRODUCTDESCRIPTIONTXT="Vanilla wafers")),
            ("c", _record(PRODUCTDESCRIPTIONTXT="Peanut snacks")),
        ]
        report = build_population_report(probed, total_products=134450)
        assert "sample size (successful GETs): 3" in report
        assert "denominator): 134450" in report
        assert "populated: 1/3 (33.3%)" in report
        assert "content ⊆ full (whitespace-blind): 1/1" in report
        assert "clean_prefix_word_boundary" in report

    def test_unknown_denominator(self) -> None:
        report = build_population_report([("a", _record(PRODUCTDESCRIPTIONTXT="x"))], None)
        assert "denominator): unknown" in report

    def test_whitespace_artifact_counts_as_content_subset_not_net_new(self) -> None:
        # raw not_prefix (newline stripped) but content is identical -> NOT net-new
        probed = [
            (
                "217593",
                _record(
                    PRODUCTDESCRIPTIONSHORT="Brand: MoliftProduct Name",
                    PRODUCTDESCRIPTIONTXT="Brand: Molift\nProduct Name and more detail",
                ),
            )
        ]
        report = build_population_report(probed)
        assert "content ⊆ full (whitespace-blind): 1/1" in report
        assert "net-new cases: NONE" in report

    def test_genuinely_net_new_case_shows_bounded_heads(self) -> None:
        probed = [
            (
                "208829",
                _record(
                    PRODUCTDESCRIPTIONSHORT="Totally unrelated curated summary",
                    PRODUCTDESCRIPTIONTXT="Completely different full description text",
                ),
            )
        ]
        report = build_population_report(probed)
        assert "GENUINELY net-new cases" in report
        assert "product 208829 productdescriptionshort:" in report
        assert "short head:" in report and "full  head:" in report

    def test_net_new_head_is_bounded(self) -> None:
        probed = [
            (
                "1",
                _record(
                    PRODUCTDESCRIPTIONSHORT="curated summary that is not a prefix",
                    PRODUCTDESCRIPTIONTXT="Y" * 5000,
                ),
            )
        ]
        report = build_population_report(probed)
        # _head caps at 100 chars, so the 5000-char full field never dumps verbatim
        assert "Y" * 200 not in report

    def test_indicator_crosstab_present(self) -> None:
        probed = [
            (
                "a",
                _record(
                    CODEINFOSHORT="LOT1 ", CODEINFORMATION="LOT1 more", CODEINFOINDICATOR="true"
                ),
            ),
            ("b", _record(CODEINFORMATION="", CODEINFOINDICATOR=None)),
        ]
        report = build_population_report(probed)
        assert "Indicator cross-tab" in report
        assert "codeinfoindicator:" in report
        assert "short_present" in report


class TestProjectForSave:
    def test_caps_long_full_field_and_records_length(self) -> None:
        record = {
            "PRODUCTID": "5",
            "CODEINFOSHORT": "LOT1",
            "CODEINFORMATION": "Z" * 9000,
        }
        projected = _project_for_save(record)
        assert len(projected["codeinformation"]) == 4000
        assert projected["_codeinformation_full_len"] == 9000
        assert projected["codeinfoshort"] == "LOT1"
        assert projected["productid"] == "5"

    def test_keeps_short_full_field_intact(self) -> None:
        record = {"CODEINFORMATION": "short text"}
        projected = _project_for_save(record)
        assert projected["codeinformation"] == "short text"
        assert "_codeinformation_full_len" not in projected


class TestProbedFromPayload:
    def test_roundtrip_records_dict(self) -> None:
        payload = {
            "records": {"219875": {"PRODUCTID": "219875"}, "100000": {"PRODUCTID": "100000"}}
        }
        probed = probed_from_payload(payload)
        assert ("219875", {"PRODUCTID": "219875"}) in probed
        assert len(probed) == 2

    def test_missing_records_key(self) -> None:
        assert probed_from_payload({"fetched_at": "x"}) == []

    def test_records_not_a_dict(self) -> None:
        assert probed_from_payload({"records": ["oops"]}) == []
