"""Pre-seed validation probes for the FDA deep-rescan historical_seed query.

Per the 2026-05-31 skeptical audit, the no-filter (Option A) seed —
``filter:"[]"``, ``sort=recalleventid asc``, ``rows=2500`` — must be validated
against the live API BEFORE committing the FdaDeepRescanLoader changes, because
two of its assumptions can abort or silently corrupt the sweep:

  * the full 32-col ``_DISPLAY_COLUMNS`` has never been sent as one payload — a
    STATUSCODE 406 on any column aborts page 1 (Finding K0);
  * ``recalleventid`` is non-unique (multi-product events share it; Finding F),
    so tie groups straddling a 2500-row page boundary could duplicate/drop rows.

Each probe is flag-gated so YOU control FDA API budget. ``--boundary`` and
``--pacing`` fetch multiple full 2500-row pages (large responses — codeinformation
can be ~200k chars/record). Auth via FDA_AUTHORIZATION_USER/KEY. Read-only: no R2
land, no bronze, no watermark. Every ``sort`` field is in ``displaycolumns`` per
Finding K0.2 (else a silent HTTP 204). HTTP stack mirrors src/extractors/fda.py.

Probes (audit §4):
  --datagroup   Probe 1: is the full 32-col _DISPLAY_COLUMNS a valid bulk-POST
                datagroup? (406 => names the offending column; fix before seeding)
  --sort-order  Probe 2: does the server order recalleventid numerically or
                lexically, and dates chronologically or lexically? Settles whether
                the floor/--characterize outputs of probe_corpus_completeness.py
                were lexically poisoned (the 12/31/2015 anomaly).
  --boundary    Probe 3: two adjacent 2500-row pages — page A full, NO productid
                straddle into page B, RESULTCOUNT ~= 134,450. Confirms the page cap
                + whether within_batch_dedup is load-bearing.
  --pacing      Probe 4: N-page sweep at a fixed inter-page sleep — confirms no
                throttle (Finding N) and sets the pacing floor for the 54-page seed.

No flag => runs the two cheap probes (--datagroup, --sort-order).

Usage:
    python scripts/fda/audit/probe_seed_query_shape.py
    python scripts/fda/audit/probe_seed_query_shape.py --boundary
    python scripts/fda/audit/probe_seed_query_shape.py --pacing --pacing-pages 5 --pacing-seconds 5
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
_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_SEED_PAGE_SIZE = 2500  # the codeinformation-capped page size the real seed uses


class ProbeError(RuntimeError):
    """A probe call did not return a usable FDA response (HTTP/throttle level)."""


# --- Pure helpers (unit-tested; no I/O) -------------------------------------


def result_records(body: dict[str, Any]) -> list[dict[str, Any]]:
    """RESULT rows from a 400-success body; [] for 412; raise on other STATUSCODE."""
    status = body.get("STATUSCODE")
    if status == 412:
        return []
    if status != 400:
        raise ProbeError(f"FDA STATUSCODE {status}: {body.get('MESSAGE')}")
    return body.get("RESULT") or []


def _date_key(date_str: str) -> tuple[str, str, str]:
    """MM/DD/YYYY -> (YYYY, MM, DD) for chronological comparison."""
    parts = date_str.split("/")
    return (parts[2], parts[0], parts[1]) if len(parts) == 3 else ("", "", "")


def numeric_ascending(ids: list[str]) -> bool:
    """True if the numeric values of the id strings are non-decreasing."""
    nums = [int(x) for x in ids]
    return all(nums[i] <= nums[i + 1] for i in range(len(nums) - 1))


def lexical_ordered(values: list[str], *, ascending: bool) -> bool:
    """True if the strings are in non-decreasing (asc) / non-increasing (desc) lexical order."""
    if ascending:
        return all(values[i] <= values[i + 1] for i in range(len(values) - 1))
    return all(values[i] >= values[i + 1] for i in range(len(values) - 1))


def chronological_ordered(dates: list[str], *, ascending: bool) -> bool:
    """True if MM/DD/YYYY dates are in chronological asc/desc order."""
    keys = [_date_key(d) for d in dates]
    if ascending:
        return all(keys[i] <= keys[i + 1] for i in range(len(keys) - 1))
    return all(keys[i] >= keys[i + 1] for i in range(len(keys) - 1))


def straddle_overlap(page_a_ids: list[str], page_b_ids: list[str]) -> list[str]:
    """productids appearing in BOTH adjacent pages (sorted). Non-empty => dup risk."""
    return sorted(set(page_a_ids) & set(page_b_ids))


# --- Network (one bulk POST) ------------------------------------------------


def _post(
    settings: Settings,
    *,
    columns: str,
    filter_str: str,
    start: int,
    rows: int,
    sort: str,
    sortorder: str,
) -> dict[str, Any]:
    """One bulk POST to /recalls/; returns the parsed body or raises ProbeError."""
    payload = {
        "displaycolumns": columns,
        "filter": filter_str,
        "start": start,
        "rows": rows,
        "sort": sort,
        "sortorder": sortorder,
    }
    url = f"{_FDA_BASE_URL}{_RECALLS_ENDPOINT}?signature={int(time.time())}"
    assert settings.fda_authorization_user is not None  # checked in main()
    assert settings.fda_authorization_key is not None
    try:
        with httpx.Client(
            timeout=120.0,
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
        raise ProbeError(
            "HTTP 204 — Akamai edge block (sort∉displaycolumns or rate). Wait and retry."
        )
    if response.status_code != 200:
        raise ProbeError(f"HTTP {response.status_code}: {response.text[:300]}")
    return response.json()


# --- Probes -----------------------------------------------------------------


def probe_datagroup(settings: Settings, columns: str) -> int:
    """Probe 1: is the full displaycolumns set a valid bulk-POST datagroup?"""
    print("=== Probe 1: 32-col datagroup validity (filter [], rows=1) ===")
    body = _post(
        settings,
        columns=columns,
        filter_str="[]",
        start=1,
        rows=1,
        sort="recalleventid",
        sortorder="asc",
    )
    status = body.get("STATUSCODE")
    if status == 400:
        print(f"  PASS: STATUSCODE 400, RESULTCOUNT={body.get('RESULTCOUNT'):,}. Datagroup valid.")
        return 0
    if status == 406:
        print(f"  FAIL: STATUSCODE 406 — {body.get('MESSAGE')}")
        print("  => a requested column is not in the bulk-POST datagroup. Remove/fix it in")
        print("     _DISPLAY_COLUMNS before seeding (the seed would abort on page 1).")
        return 1
    print(f"  INCONCLUSIVE: STATUSCODE {status} — {body.get('MESSAGE')}")
    return 2


def probe_sort_order(settings: Settings) -> int:
    """Probe 2: numeric-vs-lexical recalleventid order + chronological-vs-lexical date order."""
    print("=== Probe 2: sort order-type ===")
    rc = result_records(
        _post(
            settings,
            columns="recalleventid,productid",
            filter_str="[]",
            start=1,
            rows=10,
            sort="recalleventid",
            sortorder="asc",
        )
    )
    ids = [str(r["RECALLEVENTID"]) for r in rc]
    print(f"  recalleventid asc (10): {ids}")
    if ids:
        num = numeric_ascending(ids)
        lex = lexical_ordered(ids, ascending=True)
        kind = "numeric" if (num and not lex) else "lexical" if (lex and not num) else "ambiguous"
        print(f"    numeric_asc={num}  lexical_asc={lex}  => {kind} order")
        print("    (either is a STABLE total order, so paginated completeness holds regardless)")

    dr = result_records(
        _post(
            settings,
            columns="productid,recallinitiationdt",
            filter_str="[]",
            start=1,
            rows=10,
            sort="recallinitiationdt",
            sortorder="desc",
        )
    )
    dates = [r["RECALLINITIATIONDT"] for r in dr if r.get("RECALLINITIATIONDT")]
    print(f"  recallinitiationdt desc (10): {dates}")
    if dates:
        chrono = chronological_ordered(dates, ascending=False)
        lex = lexical_ordered(dates, ascending=False)
        kind = (
            "chronological"
            if (chrono and not lex)
            else "lexical"
            if (lex and not chrono)
            else "ambiguous"
        )
        print(f"    chronological_desc={chrono}  lexical_desc={lex}  => {kind} date sort")
        if kind == "lexical":
            print("    => CONFIRMS Bug #1: the floor + --characterize outputs of")
            print("       probe_corpus_completeness.py are lexically poisoned (meaningless).")
            print("       Characterize the 197 post-seed via SQL on bronze (sort-immune).")
    return 0


def probe_boundary(settings: Settings, columns: str) -> int:
    """Probe 3: two adjacent 2500-row pages — full page A, no productid straddle into B."""
    print(f"=== Probe 3: page cap + boundary integrity (rows={_SEED_PAGE_SIZE}) ===")
    page_a = result_records(
        _post(
            settings,
            columns=columns,
            filter_str="[]",
            start=1,
            rows=_SEED_PAGE_SIZE,
            sort="recalleventid",
            sortorder="asc",
        )
    )
    page_b = result_records(
        _post(
            settings,
            columns=columns,
            filter_str="[]",
            start=_SEED_PAGE_SIZE + 1,
            rows=_SEED_PAGE_SIZE,
            sort="recalleventid",
            sortorder="asc",
        )
    )
    a_ids = [str(r["PRODUCTID"]) for r in page_a]
    b_ids = [str(r["PRODUCTID"]) for r in page_b]
    overlap = straddle_overlap(a_ids, b_ids)
    print(f"  page A rows={len(page_a)} (expect {_SEED_PAGE_SIZE}); page B rows={len(page_b)}")
    print(f"  productid straddle (A ∩ B): {len(overlap)}")
    rc = 0
    if len(page_a) != _SEED_PAGE_SIZE:
        print(f"  FAIL (truncation risk): page A returned {len(page_a)} != {_SEED_PAGE_SIZE}.")
        print(
            "     The len(page)<rows terminator could stop the seed early — re-key off RESULTCOUNT."
        )
        rc = 1
    if overlap:
        print(f"  FAIL (straddle): {len(overlap)} productid(s) in both pages, e.g. {overlap[:5]}.")
        print("     within_batch_dedup=True is MANDATORY in the seed path.")
        rc = 1
    if rc == 0:
        print("  PASS: page A full, no straddle. Pagination is gap/dup-free at this boundary.")
        print("  (Re-run to confirm determinism across two invocations.)")
    return rc


def probe_pacing(settings: Settings, columns: str, pages: int, sleep_s: float) -> int:
    """Probe 4: N-page sweep at a fixed inter-page sleep — confirm no throttle."""
    print(f"=== Probe 4: pacing floor ({pages} pages, {sleep_s}s/page, rows={_SEED_PAGE_SIZE}) ===")
    for i in range(pages):
        start = 1 + i * _SEED_PAGE_SIZE
        try:
            recs = result_records(
                _post(
                    settings,
                    columns=columns,
                    filter_str="[]",
                    start=start,
                    rows=_SEED_PAGE_SIZE,
                    sort="recalleventid",
                    sortorder="asc",
                )
            )
        except ProbeError as exc:
            print(f"  FAIL on page {i + 1} (start={start}): {exc}")
            print(f"  => {sleep_s}s/page is too fast; increase the inter-page sleep for the seed.")
            return 1
        print(f"  page {i + 1} (start={start}): {len(recs)} rows OK")
        if i < pages - 1:
            time.sleep(sleep_s)
    print(f"  PASS: {pages} pages at {sleep_s}s/page with no throttle — validated pacing floor.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--datagroup", action="store_true", help="Probe 1 only.")
    parser.add_argument("--sort-order", action="store_true", help="Probe 2 only.")
    parser.add_argument("--boundary", action="store_true", help="Probe 3 (fetches 2x2500 rows).")
    parser.add_argument("--pacing", action="store_true", help="Probe 4 (fetches N x 2500 rows).")
    parser.add_argument(
        "--pacing-pages", type=int, default=5, help="Pages for --pacing (default 5)."
    )
    parser.add_argument(
        "--pacing-seconds",
        type=float,
        default=5.0,
        help="Inter-page sleep for --pacing (default 5).",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    if settings.fda_authorization_user is None or settings.fda_authorization_key is None:
        print(
            "ERROR: FDA_AUTHORIZATION_USER and FDA_AUTHORIZATION_KEY must be set.", file=sys.stderr
        )
        return 2

    from src.extractors.fda import _DISPLAY_COLUMNS  # the exact production column set

    # Default (no flag): the two cheap probes.
    run_datagroup = args.datagroup or not (args.sort_order or args.boundary or args.pacing)
    run_sort = args.sort_order or not (args.datagroup or args.boundary or args.pacing)

    rc = 0
    try:
        if run_datagroup:
            rc = max(rc, probe_datagroup(settings, _DISPLAY_COLUMNS))
        if run_sort:
            rc = max(rc, probe_sort_order(settings))
        if args.boundary:
            rc = max(rc, probe_boundary(settings, _DISPLAY_COLUMNS))
        if args.pacing:
            rc = max(
                rc, probe_pacing(settings, _DISPLAY_COLUMNS, args.pacing_pages, args.pacing_seconds)
            )
    except ProbeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return rc


if __name__ == "__main__":
    sys.exit(main())
