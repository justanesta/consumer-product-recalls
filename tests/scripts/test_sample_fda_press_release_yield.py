from __future__ import annotations

import sys
from pathlib import Path

import pytest

# scripts/ is not on sys.path by default; add the repo root so we can import
# the script as a regular module for testing (mirrors test_refresh_user_agents.py).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.sample_fda_press_release_yield import (  # noqa: E402 — sys.path mutated above
    _IRES_USER_AGENT,
    SampleRow,
    estimate_yield,
    interpret_pr_response,
    parse_sample_csv,
    report_to_dict,
    wilson_interval,
)

# ---------------------------------------------------------------------------
# anti-abuse User-Agent drift guard (finding N)
# ---------------------------------------------------------------------------


def test_user_agent_matches_extractor_to_evade_akamai() -> None:
    """The sampler MUST present the same Mozilla User-Agent as the production extractor.

    The default python-httpx UA trips FDA's Akamai anti-abuse layer on the FIRST request
    (302 → apology page) regardless of rate; drift between this constant and the extractor's
    IRES_USER_AGENT would silently re-break the sampler. Guards the 2026-06 regression where
    the sampler shipped without the UA and 302'd on event 1."""
    from src.extractors._fda_base import IRES_USER_AGENT

    assert _IRES_USER_AGENT == IRES_USER_AGENT


# ---------------------------------------------------------------------------
# parse_sample_csv
# ---------------------------------------------------------------------------


def test_parse_sample_csv_happy_path() -> None:
    rows = [
        {
            "recall_event_id": "76385",
            "stratum": "pre_2012",
            "stratum_size": "12000",
            "sample_pct": "1.0",
        },
        {
            "recall_event_id": "98815",
            "stratum": "post_2022_10_25",
            "stratum_size": "8000",
            "sample_pct": "1.0",
        },
    ]
    parsed = parse_sample_csv(rows)
    assert parsed == [
        SampleRow(recall_event_id=76385, stratum="pre_2012", stratum_size=12000),
        SampleRow(recall_event_id=98815, stratum="post_2022_10_25", stratum_size=8000),
    ]


def test_parse_sample_csv_empty() -> None:
    assert parse_sample_csv([]) == []


def test_parse_sample_csv_missing_column_raises() -> None:
    with pytest.raises(ValueError, match="row 1 malformed"):
        parse_sample_csv([{"recall_event_id": "1", "stratum": "x"}])  # no stratum_size


def test_parse_sample_csv_non_integer_id_raises() -> None:
    with pytest.raises(ValueError, match="row 1 malformed"):
        parse_sample_csv([{"recall_event_id": "not-an-int", "stratum": "x", "stratum_size": "5"}])


# ---------------------------------------------------------------------------
# interpret_pr_response — mirrors the extractor's RESULT-shape handling
# ---------------------------------------------------------------------------


def test_interpret_result_as_list() -> None:
    assert interpret_pr_response({"STATUSCODE": 400, "RESULT": [{"a": 1}, {"a": 2}]}) == 2


def test_interpret_result_columns_data_shape() -> None:
    body = {
        "STATUSCODE": 400,
        "RESULT": {"COLUMNS": ["RECALLEVENTID", "PRESSRELEASEURL"], "DATA": [[1, "u"], [1, "v"]]},
    }
    assert interpret_pr_response(body) == 2


def test_interpret_result_none_is_zero() -> None:
    assert interpret_pr_response({"STATUSCODE": 400, "RESULT": None}) == 0


def test_interpret_result_absent_is_zero() -> None:
    assert interpret_pr_response({"STATUSCODE": 400}) == 0


def test_interpret_bad_statuscode_raises() -> None:
    with pytest.raises(ValueError, match="STATUSCODE 406"):
        interpret_pr_response({"STATUSCODE": 406, "MESSAGE": "datagroup mismatch"})


# ---------------------------------------------------------------------------
# wilson_interval
# ---------------------------------------------------------------------------


def test_wilson_zero_n() -> None:
    assert wilson_interval(0, 0) == (0.0, 0.0)


def test_wilson_zero_successes_has_nonzero_upper() -> None:
    lo, hi = wilson_interval(0, 100)
    assert lo == 0.0
    assert 0.0 < hi < 0.1  # 0/100 still bounds the true rate above 0


def test_wilson_bounds_within_unit_interval() -> None:
    lo, hi = wilson_interval(1, 4)
    assert 0.0 <= lo <= hi <= 1.0


# ---------------------------------------------------------------------------
# estimate_yield — scaling + partial-run handling
# ---------------------------------------------------------------------------


def test_estimate_yield_scales_by_stratum_size() -> None:
    sample = [
        SampleRow(1, "A", stratum_size=1000),
        SampleRow(2, "A", stratum_size=1000),
        SampleRow(3, "A", stratum_size=1000),
        SampleRow(4, "A", stratum_size=1000),
    ]
    # 1 of 4 sampled events has a PR (with 3 PRs); observed event yield = 0.25.
    pr_counts = {1: 3, 2: 0, 3: 0, 4: 0}
    report = estimate_yield(sample, pr_counts)
    assert report.total_sampled_events == 4
    assert report.total_events_with_pr_sampled == 1
    assert report.total_press_releases_sampled == 3
    # 0.25 * 1000 = 250 events with PR; (3/4) * 1000 = 750 press releases.
    assert report.estimated_total_events_with_pr == pytest.approx(250.0)
    assert report.estimated_total_press_releases == pytest.approx(750.0)
    assert report.total_events_in_worklist == 1000


def test_estimate_yield_partial_run_excludes_unfetched() -> None:
    # 2 events drawn but only 1 fetched (throttle stopped the run). The unfetched event
    # must NOT inflate the denominator — yield computed over the 1 fetched event only.
    sample = [SampleRow(1, "A", 500), SampleRow(2, "A", 500)]
    pr_counts = {1: 2}  # event 2 never fetched
    report = estimate_yield(sample, pr_counts)
    assert report.total_sampled_events == 1
    assert report.strata[0].observed_event_yield == pytest.approx(1.0)
    # scaled to the full stratum size: 1.0 * 500 events, (2/1)*500 PRs.
    assert report.strata[0].estimated_events_with_pr == pytest.approx(500.0)
    assert report.strata[0].estimated_press_releases == pytest.approx(1000.0)


def test_estimate_yield_multi_stratum_sorted() -> None:
    sample = [
        SampleRow(10, "post_2022_10_25", 100),
        SampleRow(20, "pre_2012", 900),
    ]
    pr_counts = {10: 1, 20: 0}
    report = estimate_yield(sample, pr_counts)
    assert [s.stratum for s in report.strata] == ["post_2022_10_25", "pre_2012"]
    assert report.total_events_in_worklist == 1000


def test_estimate_yield_empty_sample() -> None:
    report = estimate_yield([], {})
    assert report.total_sampled_events == 0
    assert report.estimated_total_press_releases == 0.0
    assert report.strata == []


def test_report_to_dict_shape() -> None:
    report = estimate_yield([SampleRow(1, "A", 10)], {1: 1})
    d = report_to_dict(report)
    assert d["total_sampled_events"] == 1
    assert d["strata"][0]["stratum"] == "A"
    assert len(d["strata"][0]["wilson_ci_95"]) == 2
