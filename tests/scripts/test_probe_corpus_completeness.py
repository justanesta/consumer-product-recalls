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
    completeness_verdict,
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
        complete, gap = completeness_verdict(134450, 134253)
        assert complete is False
        assert gap == 197

    def test_windowed_larger_is_complete(self) -> None:
        # Corpus grew between calls — negative gap, still complete.
        complete, gap = completeness_verdict(133841, 133850)
        assert complete is True
        assert gap == -9

    def test_tolerance_boundary(self) -> None:
        # Exactly at tolerance = complete; one past = incomplete.
        assert completeness_verdict(1000, 1000 - _RACE_TOLERANCE)[0] is True
        assert completeness_verdict(1000, 1000 - _RACE_TOLERANCE - 1)[0] is False
