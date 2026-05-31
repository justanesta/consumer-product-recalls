"""Probe FDA bulk POST /recalls/ for historical-seed completeness + floor.

Answers two questions empirically BEFORE the FDA historical seed:

1. **Completeness.** ``FdaDeepRescanLoader`` filters on ``eventlmdfrom``/``eventlmdto``.
   Per ``api_observations.md`` Finding H, the ``*lmd`` columns are null for
   un-edited records — so an eventlmd date-window may silently EXCLUDE them
   (NULL fails ``>=``). This compares the unfiltered corpus size (``filter: []``,
   whose ``RESULTCOUNT`` is the whole dataset per Finding E) against an
   ``eventlmdfrom=<floor>`` window. If they match, the windowed deep-rescan
   reaches everything; if the unfiltered count is materially larger, the window
   drops null-eventlmd rows and the seed strategy must change (no-filter full
   pagination).

2. **Floor / depth.** The earliest non-null ``eventlmd`` (the realized filter
   floor) and the earliest ``recallinitiationdt`` (true historical depth).

Three ``rows=1`` calls — ``RESULTCOUNT`` is the whole-dataset count regardless of
``rows`` (Finding E) — so this burns negligible probe budget. Does NOT land,
load, or touch the watermark. Auth via FDA_AUTHORIZATION_USER +
FDA_AUTHORIZATION_KEY (Settings).

HTTP stack mirrors ``src/extractors/fda.py`` and
``scripts/fda/audit/probe_displaycolumns.py`` (httpx + Mozilla UA + auth headers;
``follow_redirects``; cache-busting ``signature`` query param). The ``sort`` field
on every call appears in ``displaycolumns`` per Finding O (else FDA returns 204).

Usage:
    python scripts/fda/audit/probe_corpus_completeness.py
    python scripts/fda/audit/probe_corpus_completeness.py --floor 01/01/2000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.config.settings import Settings  # noqa: E402

_FDA_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_RECALLS_ENDPOINT = "/recalls/"
# Exact UA from src/extractors/fda.py — default python-httpx UA trips FDA's
# anti-abuse throttle (Finding N).
_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Every sort field used below must appear here (Finding O: sort ∉ displaycolumns
# → HTTP 204 silent edge block).
_COLUMNS = "productid,eventlmd,recallinitiationdt"

# A gap this small between the unfiltered and windowed counts is attributable to
# the corpus changing between sequential calls (~20 records/day), not to
# systematic null-eventlmd exclusion (which would be in the thousands).
_RACE_TOLERANCE = 25


class ProbeError(RuntimeError):
    """A probe call did not return a usable FDA success response."""


def parse_resultcount(body: dict[str, Any]) -> int:
    """RESULTCOUNT from a bulk-POST body; raise on non-success STATUSCODE.

    STATUSCODE semantics (api_observations.md Finding A/K): 400 success,
    412 empty window, 406 invalid displaycolumns, 401 auth.
    """
    status = body.get("STATUSCODE")
    if status == 412:
        return 0
    if status != 400:
        raise ProbeError(f"FDA STATUSCODE {status}: {body.get('MESSAGE')}")
    count = body.get("RESULTCOUNT")
    if not isinstance(count, int):
        raise ProbeError(f"RESULTCOUNT missing or non-int: {count!r}")
    return count


def earliest_value(body: dict[str, Any], field: str) -> str | None:
    """First record's value for ``field`` (uppercase API key), or None if absent/empty."""
    result = body.get("RESULT") or []
    if not result:
        return None
    value = result[0].get(field.upper())
    return value if value not in (None, "") else None


def completeness_verdict(
    unfiltered: int, windowed: int, tolerance: int = _RACE_TOLERANCE
) -> tuple[bool, int]:
    """Return (is_complete, gap). gap = unfiltered - windowed; complete iff gap <= tolerance.

    A gap within tolerance is call-to-call corpus drift; a large positive gap is
    the null-eventlmd exclusion signal. A negative gap (windowed > unfiltered) is
    also "complete" — it just means the corpus grew between calls.
    """
    gap = unfiltered - windowed
    return gap <= tolerance, gap


def _recall_date_key(date_str: str) -> tuple[str, str, str]:
    """Sort key turning MM/DD/YYYY into (YYYY, MM, DD) for chronological order.

    Lexical sort of MM/DD/YYYY is wrong (01/01/2026 < 12/31/2003); reorder first.
    Malformed values sort first (empty key) — fine for a probe.
    """
    parts = date_str.split("/")
    return (parts[2], parts[0], parts[1]) if len(parts) == 3 else ("", "", "")


def characterize_eventlmd_nulls(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize null-eventlmd rows in a sample (expected sorted recall-date DESC).

    Tests the "the gap is recent un-edited recalls" hypothesis: if the null-eventlmd
    rows cluster at the most-recent recall dates, they're new recalls not yet edited
    (eventlmd advances on edits only — Finding H), i.e. transient and self-healing.
    If their recall dates span old years, the gap is structural.
    """
    nulls = [r for r in records if not r.get("EVENTLMD")]
    null_recall_dates = sorted(
        (r["RECALLINITIATIONDT"] for r in nulls if r.get("RECALLINITIATIONDT")),
        key=_recall_date_key,
    )
    return {
        "sampled": len(records),
        "null_eventlmd": len(nulls),
        "null_recall_date_earliest": null_recall_dates[0] if null_recall_dates else None,
        "null_recall_date_latest": null_recall_dates[-1] if null_recall_dates else None,
    }


def _post(
    settings: Settings,
    *,
    filter_str: str,
    sort: str,
    sortorder: str,
    rows: int,
) -> dict[str, Any]:
    """One bulk POST to /recalls/; returns the parsed JSON body or raises ProbeError."""
    payload = {
        "displaycolumns": _COLUMNS,
        "filter": filter_str,
        "start": 1,
        "rows": rows,
        "sort": sort,
        "sortorder": sortorder,
    }
    url = f"{_FDA_BASE_URL}{_RECALLS_ENDPOINT}?signature={int(time.time())}"
    assert settings.fda_authorization_user is not None  # checked in main()
    assert settings.fda_authorization_key is not None
    try:
        with httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        ) as client:
            response = client.post(
                url,
                data={"payLoad": json.dumps(payload)},
                headers={
                    "Authorization-User": settings.fda_authorization_user.get_secret_value(),
                    "Authorization-Key": settings.fda_authorization_key.get_secret_value(),
                },
            )
    except httpx.TransportError as exc:
        raise ProbeError(f"network failure: {exc}") from exc

    if "text/html" in response.headers.get("Content-Type", ""):
        raise ProbeError("FDA anti-abuse throttle (HTML in place of JSON). Wait 30+ min and retry.")
    if response.status_code == 204:
        raise ProbeError("HTTP 204 — Akamai edge block (rate/budget). Wait an hour and retry.")
    if response.status_code != 200:
        raise ProbeError(f"HTTP {response.status_code}: {response.text[:300]}")
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--floor",
        default="01/01/1900",
        help="eventlmdfrom floor, MM/DD/YYYY (default 01/01/1900 — before any iRES record).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1,
        help="Page size; RESULTCOUNT is the whole-dataset count regardless (default 1).",
    )
    parser.add_argument(
        "--characterize",
        type=int,
        default=0,
        metavar="N",
        help="Also pull the N most-recent recalls (by recallinitiationdt) and report how "
        "many have null eventlmd + their recall-date range — tests the recent/new hypothesis.",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    if settings.fda_authorization_user is None or settings.fda_authorization_key is None:
        print(
            "ERROR: FDA_AUTHORIZATION_USER and FDA_AUTHORIZATION_KEY must be set.", file=sys.stderr
        )
        return 2

    window_filter = f"[{{'eventlmdfrom':'{args.floor}'}}]"
    try:
        unfiltered = parse_resultcount(
            _post(settings, filter_str="[]", sort="productid", sortorder="asc", rows=args.rows)
        )
        windowed_body = _post(
            settings, filter_str=window_filter, sort="eventlmd", sortorder="asc", rows=args.rows
        )
        windowed = parse_resultcount(windowed_body)
        earliest_eventlmd = earliest_value(windowed_body, "eventlmd")
        recall_body = _post(
            settings,
            filter_str=window_filter,
            sort="recallinitiationdt",
            sortorder="asc",
            rows=args.rows,
        )
        earliest_recall = earliest_value(recall_body, "recallinitiationdt")
    except ProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    complete, gap = completeness_verdict(unfiltered, windowed)

    print("=== FDA historical-seed completeness + floor probe ===")
    print(f"unfiltered corpus (filter []):       {unfiltered:>9,}")
    print(f"eventlmdfrom={args.floor} window:    {windowed:>9,}")
    print(f"gap (unfiltered - windowed):         {gap:>9,}")
    print(f"earliest non-null eventlmd:          {earliest_eventlmd}")
    print(f"earliest recallinitiationdt:         {earliest_recall}")
    print()
    if complete:
        print(
            f"VERDICT: COMPLETE (gap within race tolerance {_RACE_TOLERANCE}). The "
            "eventlmdfrom window reaches the whole corpus — `recalls deep-rescan fda` "
            f"with an early --start-date is safe. Realized floor: eventlmd={earliest_eventlmd}, "
            f"earliest recall={earliest_recall}."
        )
    else:
        print(
            f"VERDICT: INCOMPLETE — the eventlmd window MISSES ~{gap:,} records (almost "
            "certainly null-eventlmd un-edited rows). Do NOT seed with an eventlmdfrom "
            "window; it would silently drop them. FdaDeepRescanLoader needs a no-filter "
            "full-pagination mode (filter []) for the historical seed — flag this before seeding."
        )

    if args.characterize > 0:
        try:
            char_body = _post(
                settings,
                filter_str="[]",
                sort="recallinitiationdt",
                sortorder="desc",
                rows=args.characterize,
            )
        except ProbeError as exc:
            print(f"ERROR (characterize): {exc}", file=sys.stderr)
            return 2
        summary = characterize_eventlmd_nulls(char_body.get("RESULT", []))
        print()
        print(f"=== characterize: {summary['sampled']} most-recent recalls (by recall date) ===")
        print(f"null eventlmd among them:            {summary['null_eventlmd']}")
        print(
            "  null-rows recall-date range:       "
            f"{summary['null_recall_date_earliest']} -> {summary['null_recall_date_latest']}"
        )
        print(
            "  => null_eventlmd ~= the gap with all-recent dates: new un-edited recalls "
            "(transient). Old/spread dates: structural gap."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
