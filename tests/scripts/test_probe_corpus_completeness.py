from __future__ import annotations

import sys
from pathlib import Path

import pytest

# scripts/ is not on sys.path by default; add the repo root so we can import the
# probe as a regular module (PEP 420 namespace package) for testing its pure logic.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.fda.audit.probe_corpus_completeness import (  # noqa: E402  — sys.path mutated above
    _RACE_TOLERANCE,
    ProbeError,
    _recall_date_key,
    characterize_eventlmd_nulls,
    completeness_verdict,
    earliest_value,
    parse_resultcount,
)


class TestParseResultcount:
    def test_success_returns_count(self) -> None:
        assert parse_resultcount({"STATUSCODE": 400, "RESULTCOUNT": 133841}) == 133841

    def test_empty_window_returns_zero(self) -> None:
        assert parse_resultcount({"STATUSCODE": 412, "MESSAGE": "no records"}) == 0

    def test_bad_columns_raises(self) -> None:
        with pytest.raises(ProbeError, match="406"):
            parse_resultcount({"STATUSCODE": 406, "MESSAGE": "bad displaycolumns"})

    def test_missing_resultcount_raises(self) -> None:
        with pytest.raises(ProbeError, match="RESULTCOUNT"):
            parse_resultcount({"STATUSCODE": 400})

    def test_non_int_resultcount_raises(self) -> None:
        with pytest.raises(ProbeError, match="RESULTCOUNT"):
            parse_resultcount({"STATUSCODE": 400, "RESULTCOUNT": "lots"})


class TestEarliestValue:
    def test_returns_first_record_uppercase_key(self) -> None:
        body = {"RESULT": [{"EVENTLMD": "04/07/1975"}, {"EVENTLMD": "05/01/2000"}]}
        assert earliest_value(body, "eventlmd") == "04/07/1975"

    def test_empty_result_returns_none(self) -> None:
        assert earliest_value({"RESULT": []}, "eventlmd") is None

    def test_missing_result_key_returns_none(self) -> None:
        assert earliest_value({}, "eventlmd") is None

    def test_missing_field_returns_none(self) -> None:
        assert earliest_value({"RESULT": [{"PRODUCTID": "1"}]}, "eventlmd") is None

    def test_empty_string_value_returns_none(self) -> None:
        assert earliest_value({"RESULT": [{"EVENTLMD": ""}]}, "eventlmd") is None


class TestCompletenessVerdict:
    def test_equal_counts_complete(self) -> None:
        complete, gap = completeness_verdict(133841, 133841)
        assert complete is True
        assert gap == 0

    def test_small_race_gap_complete(self) -> None:
        complete, gap = completeness_verdict(133841, 133835)
        assert complete is True
        assert gap == 6

    def test_large_gap_incomplete(self) -> None:
        complete, gap = completeness_verdict(133841, 50000)
        assert complete is False
        assert gap == 83841

    def test_windowed_larger_is_complete(self) -> None:
        # Corpus grew between calls — negative gap, still complete.
        complete, gap = completeness_verdict(133841, 133850)
        assert complete is True
        assert gap == -9

    def test_tolerance_boundary(self) -> None:
        # Exactly at tolerance = complete; one past = incomplete.
        assert completeness_verdict(1000, 1000 - _RACE_TOLERANCE)[0] is True
        assert completeness_verdict(1000, 1000 - _RACE_TOLERANCE - 1)[0] is False


class TestRecallDateKey:
    def test_reorders_to_year_month_day(self) -> None:
        assert _recall_date_key("01/15/2026") == ("2026", "01", "15")

    def test_sorts_chronologically_not_lexically(self) -> None:
        # Lexical order would put 01/01/2026 before 12/31/2003; chronological must not.
        dates = ["01/01/2026", "12/31/2003", "06/15/2015"]
        assert sorted(dates, key=_recall_date_key) == ["12/31/2003", "06/15/2015", "01/01/2026"]

    def test_malformed_sorts_first(self) -> None:
        assert _recall_date_key("garbage") == ("", "", "")


class TestCharacterizeEventlmdNulls:
    def test_counts_and_dates_for_recent_nulls(self) -> None:
        records = [
            {"EVENTLMD": "05/01/2026", "RECALLINITIATIONDT": "04/30/2026"},  # edited
            {"EVENTLMD": None, "RECALLINITIATIONDT": "05/28/2026"},  # null, recent
            {"EVENTLMD": "", "RECALLINITIATIONDT": "05/20/2026"},  # empty == null, recent
        ]
        summary = characterize_eventlmd_nulls(records)
        assert summary["sampled"] == 3
        assert summary["null_eventlmd"] == 2
        assert summary["null_recall_date_earliest"] == "05/20/2026"
        assert summary["null_recall_date_latest"] == "05/28/2026"

    def test_no_nulls(self) -> None:
        records = [{"EVENTLMD": "05/01/2026", "RECALLINITIATIONDT": "04/30/2026"}]
        summary = characterize_eventlmd_nulls(records)
        assert summary["null_eventlmd"] == 0
        assert summary["null_recall_date_earliest"] is None
        assert summary["null_recall_date_latest"] is None

    def test_null_with_missing_recall_date_excluded_from_range(self) -> None:
        records = [{"EVENTLMD": None}]  # null eventlmd, no recall date
        summary = characterize_eventlmd_nulls(records)
        assert summary["null_eventlmd"] == 1
        assert summary["null_recall_date_earliest"] is None
