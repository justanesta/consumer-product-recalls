from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.fda.audit.probe_seed_query_shape import (  # noqa: E402  — sys.path mutated above
    ProbeError,
    chronological_ordered,
    lexical_ordered,
    numeric_ascending,
    result_records,
    straddle_overlap,
)


class TestResultRecords:
    def test_success_returns_rows(self) -> None:
        body = {"STATUSCODE": 400, "RESULT": [{"PRODUCTID": "1"}]}
        assert result_records(body) == [{"PRODUCTID": "1"}]

    def test_empty_window_returns_empty(self) -> None:
        assert result_records({"STATUSCODE": 412, "MESSAGE": "No results found"}) == []

    def test_missing_result_returns_empty_list(self) -> None:
        assert result_records({"STATUSCODE": 400}) == []

    def test_datagroup_406_raises(self) -> None:
        with pytest.raises(ProbeError, match="406"):
            result_records({"STATUSCODE": 406, "MESSAGE": "displaycolumns mismatch"})


class TestNumericAscending:
    def test_numeric_non_decreasing(self) -> None:
        assert numeric_ascending(["98000", "98001", "98010"]) is True

    def test_numeric_violation(self) -> None:
        assert numeric_ascending(["98010", "98001"]) is False

    def test_distinguishes_from_lexical(self) -> None:
        # "100" < "20" lexically but 100 > 20 numerically — a numeric-asc seq that
        # is NOT lexical-asc proves the server sorts numerically.
        seq = ["2", "20", "100"]
        assert numeric_ascending(seq) is True
        assert lexical_ordered(seq, ascending=True) is False


class TestLexicalOrdered:
    def test_ascending(self) -> None:
        assert lexical_ordered(["a", "b", "c"], ascending=True) is True
        assert lexical_ordered(["b", "a"], ascending=True) is False

    def test_descending(self) -> None:
        assert lexical_ordered(["c", "b", "a"], ascending=False) is True
        assert lexical_ordered(["a", "b"], ascending=False) is False


class TestChronologicalOrdered:
    def test_descending_chronological(self) -> None:
        # Genuinely chronological desc (newest first).
        assert (
            chronological_ordered(["05/28/2026", "01/15/2026", "12/31/2003"], ascending=False)
            is True
        )

    def test_lexical_desc_is_not_chronological(self) -> None:
        # The 12/31/2015 anomaly shape: lexical desc puts 12/.. before 01/.. of a newer year.
        lexically_desc = ["12/31/2015", "06/15/2020", "01/15/2026"]
        assert lexical_ordered(lexically_desc, ascending=False) is True
        assert chronological_ordered(lexically_desc, ascending=False) is False

    def test_ascending_chronological(self) -> None:
        assert (
            chronological_ordered(["01/01/2003", "06/15/2015", "05/28/2026"], ascending=True)
            is True
        )


class TestStraddleOverlap:
    def test_no_overlap(self) -> None:
        assert straddle_overlap(["1", "2", "3"], ["4", "5"]) == []

    def test_overlap_sorted(self) -> None:
        assert straddle_overlap(["1", "2", "3"], ["3", "2", "9"]) == ["2", "3"]
