"""Estimate the FDA press-release yield-by-stratum from a CHEAP paced sample.

Why this exists
---------------
The press-release endpoint is per-event (``GET /search/pressreleaseurls/{eventid}``),
the work-list is ~50,509 distinct events, and the sweep is pacing-bound at ~1 req/s →
~17 h for the full corpus. Before paying that, we want to know roughly HOW MANY press
releases exist and WHERE (by date band) they sit — so a ~1% stratified sample, paced the
same way, gives an unbiased point estimate of total PRs + per-stratum yield.

The deep-rescan CLI cannot do this: ``recalls deep-rescan fda_press_releases`` only
accepts ``--limit`` + ``--resume-after-event-id`` and sweeps a CONTIGUOUS ascending
``recall_event_id`` range (src/cli/main.py:344-360, src/extractors/fda_press_release.py:303-326).
There is no flag to pass an arbitrary set of ids, and a random sample is not contiguous.
So this is a standalone, READ-ONLY-on-the-API sampler: it does NOT write to bronze, R2, or
Postgres. It only reads a CSV of pre-drawn ids and writes a JSON estimate.

Input CSV is produced by
``scripts/sql/fda_press_releases/bronze/sample_worklist_stratified.sql`` (columns:
``recall_event_id, stratum, stratum_size, sample_pct``). This script GETs each sampled
event, counts press releases, and scales the per-stratum observed rate back up by
``stratum_size`` to estimate the total.

Faithful transport (mirrors src/extractors/fda_press_release.py so the sample paces and
errors exactly like the real sweep would):
  - same base URL + ``?signature=<unix_ts>`` query param
  - same ``Authorization-User`` / ``Authorization-Key`` headers (env, like the extractor)
  - same Mozilla ``User-Agent`` (finding N): the DEFAULT python-httpx UA trips FDA's Akamai
    anti-abuse layer on the FIRST request (302→apology page) regardless of rate — setting this
    UA is the fix for that, NOT request pacing. Must stay in sync with
    src/extractors/_fda_base.py::IRES_USER_AGENT (a test asserts it).
  - same anti-abuse handling: an HTML body (302→apology page) is a throttle → STOP, do not
    retry (retries deepen the throttle); 429 honours ``Retry-After``
  - same 1 req/s default pacing

Usage (user runs — needs FDA creds + network; no DB):
    set -a && . .env && set +a
    python scripts/sample_fda_press_release_yield.py \
        --in data/exploratory/fda_press_releases/pr_sample_event_ids.csv \
        --out data/exploratory/fda_press_releases/pr_yield_estimate.json \
        --sleep 1.0
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_PR_ENDPOINT = "/search/pressreleaseurls/"  # + {eventid}
_REQUEST_TIMEOUT_SECONDS = 60.0
_DEFAULT_SLEEP_SECONDS = 1.0
# Must match src/extractors/_fda_base.py::IRES_USER_AGENT. The default python-httpx UA trips
# FDA's Akamai anti-abuse throttle on the first request (finding N); this Mozilla UA is the fix.
# Hardcoded (not imported) to keep this probe standalone; a unit test guards against drift.
_IRES_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class ThrottleDetected(RuntimeError):
    """Raised when the iRES anti-abuse layer returns HTML instead of JSON."""


@dataclass
class SampleRow:
    """One pre-drawn work-list event from the stratified-sample CSV."""

    recall_event_id: int
    stratum: str
    stratum_size: int


@dataclass
class StratumEstimate:
    """Per-stratum yield estimate scaled back up to the stratum's true size."""

    stratum: str
    stratum_size: int
    sampled_events: int
    events_with_pr: int
    press_releases_seen: int
    observed_event_yield: float  # events_with_pr / sampled_events
    estimated_events_with_pr: float  # observed_event_yield * stratum_size
    estimated_press_releases: float  # (press_releases_seen / sampled_events) * stratum_size
    wilson_ci_95: tuple[float, float]  # 95% CI on observed_event_yield (proportion)


@dataclass
class YieldReport:
    """Top-level estimate written to JSON."""

    total_events_in_worklist: int
    total_sampled_events: int
    total_events_with_pr_sampled: int
    total_press_releases_sampled: int
    estimated_total_events_with_pr: float
    estimated_total_press_releases: float
    strata: list[StratumEstimate] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pure logic — no I/O (unit-tested)
# ---------------------------------------------------------------------------


def parse_sample_csv(rows: list[dict[str, str]]) -> list[SampleRow]:
    """Parse the stratified-sample CSV rows into typed ``SampleRow`` records.

    Tolerates the psql ``-A -F','`` output (header + data rows). Raises on a missing
    column or a non-integer id/size so a malformed sample fails fast rather than
    silently skewing the estimate.
    """
    parsed: list[SampleRow] = []
    for i, row in enumerate(rows, start=1):
        try:
            parsed.append(
                SampleRow(
                    recall_event_id=int(row["recall_event_id"]),
                    stratum=row["stratum"],
                    stratum_size=int(row["stratum_size"]),
                )
            )
        except (KeyError, ValueError) as exc:
            raise ValueError(f"sample CSV row {i} malformed ({exc}): {row!r}") from exc
    return parsed


def interpret_pr_response(body: dict[str, Any]) -> int:
    """Return the press-release row count from one event's iRES JSON body.

    Mirrors src/extractors/fda_press_release.py::_interpret_pr_response shape handling:
    RESULT may be a plain list, or a {COLUMNS, DATA} pair, or absent/None (zero rows).
    STATUSCODE 400 is FDA's success code; STATUSCODE 406 means the displaycolumns/datagroup
    mismatch (should not happen on this lookup endpoint, but surface it).
    """
    status = body.get("STATUSCODE")
    if status is not None and int(status) not in (400,):
        raise ValueError(f"FDA STATUSCODE {status}: {body.get('MESSAGE')!r}")
    result = body.get("RESULT")
    if result is None:
        return 0
    if isinstance(result, list):
        return len(result)
    if isinstance(result, dict):
        data = result.get("DATA")
        if isinstance(data, list):
            return len(data)
        return 0
    return 0


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score 95% CI for a binomial proportion. Robust at small n / 0 successes.

    Returns (0.0, 0.0) for n == 0. Used because per-stratum sampled_events is small and
    rare-yield strata often have 0 successes, where the normal approximation breaks.
    """
    if n == 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = (z / denom) * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (max(0.0, center - margin), min(1.0, center + margin))


def estimate_yield(sample: list[SampleRow], pr_counts: dict[int, int]) -> YieldReport:
    """Build the scaled yield report from the sample and per-event PR counts.

    ``pr_counts`` maps recall_event_id → number of press releases observed (0 for events
    that returned no rows). Events present in ``sample`` but absent from ``pr_counts`` were
    not fetched (e.g. the run stopped early on a throttle) and are EXCLUDED from their
    stratum's denominator so a partial run still yields an unbiased estimate of what it
    did see.
    """
    by_stratum: dict[str, list[SampleRow]] = {}
    stratum_size: dict[str, int] = {}
    for r in sample:
        by_stratum.setdefault(r.stratum, []).append(r)
        stratum_size[r.stratum] = r.stratum_size

    strata: list[StratumEstimate] = []
    total_sampled = 0
    total_with_pr = 0
    total_prs = 0
    est_total_events_with_pr = 0.0
    est_total_prs = 0.0
    worklist_total = 0
    for stratum in sorted(by_stratum):
        worklist_total += stratum_size[stratum]
        rows = by_stratum[stratum]
        fetched = [r for r in rows if r.recall_event_id in pr_counts]
        sampled = len(fetched)
        with_pr = sum(1 for r in fetched if pr_counts[r.recall_event_id] > 0)
        prs = sum(pr_counts[r.recall_event_id] for r in fetched)
        size = stratum_size[stratum]
        event_yield = with_pr / sampled if sampled else 0.0
        pr_per_event = prs / sampled if sampled else 0.0
        strata.append(
            StratumEstimate(
                stratum=stratum,
                stratum_size=size,
                sampled_events=sampled,
                events_with_pr=with_pr,
                press_releases_seen=prs,
                observed_event_yield=event_yield,
                estimated_events_with_pr=event_yield * size,
                estimated_press_releases=pr_per_event * size,
                wilson_ci_95=wilson_interval(with_pr, sampled),
            )
        )
        total_sampled += sampled
        total_with_pr += with_pr
        total_prs += prs
        est_total_events_with_pr += event_yield * size
        est_total_prs += pr_per_event * size

    return YieldReport(
        total_events_in_worklist=worklist_total,
        total_sampled_events=total_sampled,
        total_events_with_pr_sampled=total_with_pr,
        total_press_releases_sampled=total_prs,
        estimated_total_events_with_pr=est_total_events_with_pr,
        estimated_total_press_releases=est_total_prs,
        strata=strata,
    )


def report_to_dict(report: YieldReport) -> dict[str, Any]:
    """Serialise the report to a JSON-ready dict (rounds floats for readability)."""
    return {
        "total_events_in_worklist": report.total_events_in_worklist,
        "total_sampled_events": report.total_sampled_events,
        "total_events_with_pr_sampled": report.total_events_with_pr_sampled,
        "total_press_releases_sampled": report.total_press_releases_sampled,
        "estimated_total_events_with_pr": round(report.estimated_total_events_with_pr, 1),
        "estimated_total_press_releases": round(report.estimated_total_press_releases, 1),
        "strata": [
            {
                "stratum": s.stratum,
                "stratum_size": s.stratum_size,
                "sampled_events": s.sampled_events,
                "events_with_pr": s.events_with_pr,
                "press_releases_seen": s.press_releases_seen,
                "observed_event_yield": round(s.observed_event_yield, 4),
                "estimated_events_with_pr": round(s.estimated_events_with_pr, 1),
                "estimated_press_releases": round(s.estimated_press_releases, 1),
                "wilson_ci_95": [round(s.wilson_ci_95[0], 4), round(s.wilson_ci_95[1], 4)],
            }
            for s in report.strata
        ],
    }


# ---------------------------------------------------------------------------
# I/O — network + files (not unit-tested; kept thin)
# ---------------------------------------------------------------------------


def _auth_headers() -> dict[str, str]:
    user = os.environ.get("FDA_AUTHORIZATION_USER")
    key = os.environ.get("FDA_AUTHORIZATION_KEY")
    if not user or not key:
        raise RuntimeError(
            "FDA_AUTHORIZATION_USER / FDA_AUTHORIZATION_KEY must be set (source your .env first)."
        )
    return {"Authorization-User": user, "Authorization-Key": key}


def fetch_event_pr_count(client: httpx.Client, event_id: int) -> int:
    """GET one event's press releases and return the row count. Faithful to the extractor."""
    url = f"{_BASE_URL}{_PR_ENDPOINT}{event_id}?signature={int(time.time())}"
    response = client.get(url, headers=_auth_headers())
    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "60")
        raise ThrottleDetected(f"HTTP 429 rate limit (event {event_id}); Retry-After={retry_after}")
    if "text/html" in response.headers.get("Content-Type", ""):
        raise ThrottleDetected(
            f"anti-abuse throttle (HTTP {response.status_code}, HTML body; event {event_id}). "
            "Wait >=30 min before retrying."
        )
    response.raise_for_status()
    return interpret_pr_response(response.json())


def read_sample_csv(path: Path) -> list[SampleRow]:
    with path.open(newline="") as fh:
        return parse_sample_csv(list(csv.DictReader(fh)))


def run_sampler(in_path: Path, out_path: Path, sleep_seconds: float) -> YieldReport:
    """Drive the paced sample end-to-end and write the JSON estimate. Resumable-friendly:
    on a throttle it writes a PARTIAL estimate (only fetched events) and stops cleanly."""
    sample = read_sample_csv(in_path)
    pr_counts: dict[int, int] = {}
    stopped_early = False
    with httpx.Client(
        timeout=_REQUEST_TIMEOUT_SECONDS,
        follow_redirects=False,
        headers={"User-Agent": _IRES_USER_AGENT},
    ) as client:
        for idx, row in enumerate(sample):
            if idx > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            try:
                pr_counts[row.recall_event_id] = fetch_event_pr_count(client, row.recall_event_id)
            except ThrottleDetected as exc:
                print(f"STOPPING: {exc}")  # noqa: T201 — operator feedback for a manual probe
                stopped_early = True
                break
    report = estimate_yield(sample, pr_counts)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = report_to_dict(report)
    payload["partial_run"] = stopped_early
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in", dest="in_path", type=Path, required=True)
    parser.add_argument("--out", dest="out_path", type=Path, required=True)
    parser.add_argument("--sleep", dest="sleep_seconds", type=float, default=_DEFAULT_SLEEP_SECONDS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    report = run_sampler(args.in_path, args.out_path, args.sleep_seconds)
    print(  # noqa: T201 — operator summary for a manual probe
        f"sampled={report.total_sampled_events} "
        f"events_with_pr={report.total_events_with_pr_sampled} "
        f"est_total_prs={report.estimated_total_press_releases:.0f} "
        f"-> {args.out_path}"
    )


if __name__ == "__main__":
    main()
