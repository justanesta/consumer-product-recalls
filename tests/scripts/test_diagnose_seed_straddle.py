from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# scripts/ is not on sys.path by default; add the repo root so we can import
# the script as a regular module for testing.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.fda.audit.diagnose_seed_straddle import (  # noqa: E402  — sys.path mutated above
    classify_group,
    coerce_productid,
    diagnose,
    render_report,
)


def _rec(productid: Any, event: Any = "E0") -> dict[str, Any]:
    return {"PRODUCTID": productid, "RECALLEVENTID": event}


# ---------------------------------------------------------------------------
# coerce_productid
# ---------------------------------------------------------------------------


def test_coerce_productid_normalizes_int_and_str() -> None:
    assert coerce_productid(12345) == "12345"
    assert coerce_productid("12345") == "12345"


# ---------------------------------------------------------------------------
# classify_group — the load-bearing classifier
# ---------------------------------------------------------------------------


def test_classify_in_page_when_copies_share_a_page() -> None:
    cls, gap, dist = classify_group([10, 11], page_size=2500)
    assert cls == "in_page"
    assert gap == 1
    assert dist == 0


def test_classify_straddle_across_a_page_boundary() -> None:
    # row 2499 is the last of page 0, row 2501 the second of page 1.
    cls, gap, dist = classify_group([2499, 2501], page_size=2500)
    assert cls == "straddle"
    assert gap == 2
    assert dist == 1  # nearest copy is 1 row from the boundary at 2500


def test_first_row_of_next_page_is_same_page_not_straddle() -> None:
    # 2500 // 2500 == 1 and 2501 // 2500 == 1 -> both page 1 -> genuine dup.
    cls, _, _ = classify_group([2500, 2501], page_size=2500)
    assert cls == "in_page"


def test_classify_scattered_when_copies_are_far_apart() -> None:
    cls, gap, _ = classify_group([10, 9000], page_size=2500)
    assert cls == "scattered"
    assert gap == 8990


# ---------------------------------------------------------------------------
# diagnose — full pass over fetched-order records (small page_size for clarity)
# ---------------------------------------------------------------------------


def test_diagnose_separates_straddle_from_in_page() -> None:
    # page_size 5 -> page 0: rows 0-4, page 1: rows 5-9, page 2: rows 10-11.
    records = [_rec(f"U{i}") for i in range(12)]
    # in-page duplicate "A" within page 0 (positions 1 and 3)
    records[1] = _rec("A", "EA")
    records[3] = _rec("A", "EA")
    # straddle duplicate "B" across the page0/page1 boundary (positions 4 and 5)
    records[4] = _rec("B", "EB")
    records[5] = _rec("B", "EB")

    d = diagnose(records, page_size=5)

    assert d.total_rows == 12
    assert d.duplicate_groups == 2
    assert d.duplicate_rows == 2  # one extra copy each
    assert d.straddle_groups == 1
    assert d.in_page_groups == 1
    assert d.scattered_groups == 0
    # straddle crosses the boundary that starts page 1
    assert d.boundary_histogram == {1: 1}
    # the dropped product's event is surfaced for targeted recovery
    assert d.affected_event_ids == ["EB"]
    # distinct = 12 positions - 2 extra copies = 10
    assert d.distinct_productids == 10


def test_diagnose_empty_payload() -> None:
    d = diagnose([], page_size=2500)
    assert d.total_rows == 0
    assert d.distinct_productids == 0
    assert d.duplicate_groups == 0
    assert d.straddle_groups == 0


def test_diagnose_all_unique_has_no_duplicates() -> None:
    records = [_rec(f"U{i}") for i in range(50)]
    d = diagnose(records, page_size=5)
    assert d.duplicate_groups == 0
    assert d.duplicate_rows == 0
    assert d.distinct_productids == 50


def test_diagnose_coerces_int_productids() -> None:
    # Same product served as int then str (schema notes PRODUCTID can be either).
    records = [_rec(1), _rec("1"), _rec(99)]
    d = diagnose(records, page_size=2500)
    assert d.distinct_productids == 2  # "1" and "99"
    assert d.duplicate_groups == 1


# ---------------------------------------------------------------------------
# render_report — verdict text smoke tests
# ---------------------------------------------------------------------------


def test_render_report_straddle_verdict() -> None:
    records = [_rec(f"U{i}") for i in range(12)]
    records[4] = _rec("B", "EB")
    records[5] = _rec("B", "EB")
    report = render_report(diagnose(records, page_size=5), 5)
    assert "STRADDLE" in report
    assert "INCOMPLETE" in report


def test_render_report_genuine_dup_verdict() -> None:
    records = [_rec(f"U{i}") for i in range(12)]
    records[1] = _rec("A", "EA")
    records[3] = _rec("A", "EA")
    report = render_report(diagnose(records, page_size=5), 5)
    assert "GENUINE SOURCE DUPLICATES" in report
    assert "COMPLETE" in report
