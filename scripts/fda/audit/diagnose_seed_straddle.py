"""Diagnose the FDA historical-seed "captured but not loaded" gap (2026-06-01).

The 2026-06-01 full-corpus seed fetched 134,450 rows (= RESULTCOUNT) but loaded
only 134,181 (134,205 distinct after recovering 24 invariant rejects). The
245-row gap is `within_batch_dedup` collapsing duplicate `(PRODUCTID, hash)` rows.
This script settles WHY those duplicates exist — and therefore whether the seed is
complete — WITHOUT re-hitting the FDA server.

THE METHOD (uses only the raw landed payload, already in R2):
`land_raw` runs BEFORE validate/invariants/dedup (`_base.py` 5-step lifecycle), so
the landed `.json.gz` is the full 134,450-row response in FETCH ORDER — a flat JSON
array (`fda.py` `land_raw`). The full-corpus path paginates `start += _PAGE_SIZE`
with `_PAGE_SIZE = 2500` and sorts on the NON-UNIQUE `recalleventid`, so page
boundaries sit at exact multiples of 2,500.

A `PRODUCTID` can appear twice for exactly two reasons:
  * SAME page  -> a genuine duplicate row in FDA's data. No product is dropped.
  * ACROSS an adjacent page boundary -> an offset-pagination tie-boundary STRADDLE
    re-read (the tie group reshuffles between the two page requests, so one row is
    served at the end of page k AND the start of page k+1). Because fetched
    (134,450) == RESULTCOUNT, every straddle re-read is matched by exactly one
    DROPPED distinct product. So: #straddle-dups == #dropped products.

VERDICT: if the duplicates straddle page boundaries (cluster at the ~53 multiples
of 2,500), the seed dropped ~that many distinct products -> re-seed sorted on the
UNIQUE `productid` (no ties -> no straddle). If they are same-page, FDA genuinely
serves duplicate rows and the seed is complete at 134,205.

This does NOT load, validate, or mutate anything — it only GETs one R2 object and
counts. Run it yourself (needs R2 credentials via Settings):

    python scripts/fda/audit/diagnose_seed_straddle.py
    python scripts/fda/audit/diagnose_seed_straddle.py --landing-path fda/2026-06-01/<uuid>.json.gz
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.config.settings import Settings  # noqa: E402  — sys.path mutated above
from src.landing.r2 import R2LandingClient  # noqa: E402

# Must match src/extractors/fda.py `_PAGE_SIZE` (the codeinformation page cap).
_PAGE_SIZE = 2_500
_PRODUCTID_KEY = "PRODUCTID"
_EVENT_KEY = "RECALLEVENTID"
# The 2026-06-01 seed's raw_landing_path (from the run log / extraction_runs).
_DEFAULT_LANDING_PATH = "fda/2026-06-01/47c5124a-6c71-4953-9f56-f248cf0da449.json.gz"


@dataclass(frozen=True)
class DupGroup:
    """One PRODUCTID that appears more than once in the fetched payload."""

    productid: str
    positions: tuple[int, ...]
    pages: tuple[int, ...]
    classification: str  # "in_page" | "straddle" | "scattered"
    position_gap: int
    boundary_distance: int  # min distance of any copy to the crossed boundary (straddle only)
    event_ids: tuple[str, ...]


@dataclass
class Diagnosis:
    total_rows: int
    distinct_productids: int
    duplicate_groups: int
    duplicate_rows: int  # sum over groups of (copies - 1) — the count within_batch_dedup collapses
    straddle_groups: int  # == inferred dropped distinct products
    in_page_groups: int
    scattered_groups: int
    # boundary page-index -> count of straddle dups crossing it
    boundary_histogram: dict[int, int] = field(default_factory=dict)
    affected_event_ids: list[str] = field(default_factory=list)
    groups: list[DupGroup] = field(default_factory=list)


def coerce_productid(value: Any) -> str:
    """PRODUCTID may arrive as int or str (schema note); normalize for grouping."""
    return str(value)


def classify_group(positions: list[int], page_size: int) -> tuple[str, int, int]:
    """Classify a duplicate group from its fetch-order positions.

    Returns (classification, position_gap, boundary_distance).

    - "in_page": all copies in one page -> genuine duplicate row, no drop implied.
    - "straddle": copies span pages and sit within one page_size of each other
      (a tie-boundary re-read) -> implies one dropped product.
    - "scattered": copies span non-adjacent pages / far apart -> anomalous, inspect.

    boundary_distance is the smallest distance from any copy to a crossed page
    boundary (multiple of page_size); 0 for in_page.
    """
    ordered = sorted(positions)
    gap = ordered[-1] - ordered[0]
    pages = {p // page_size for p in ordered}
    if len(pages) == 1:
        return "in_page", gap, 0

    # Boundaries (multiples of page_size) strictly between the first and last copy.
    first, last = ordered[0], ordered[-1]
    crossed = [
        b for b in range(((first // page_size) + 1) * page_size, last + 1, page_size) if b > first
    ]
    boundary_distance = min(min(abs(pos - b) for pos in ordered) for b in crossed)

    # A straddle re-read keeps the two copies close (within the tie group), so the
    # whole group spans <= one page_size. Anything wider is not a boundary artifact.
    if gap <= page_size:
        return "straddle", gap, boundary_distance
    return "scattered", gap, boundary_distance


def diagnose(
    records: list[dict[str, Any]],
    *,
    page_size: int = _PAGE_SIZE,
    productid_key: str = _PRODUCTID_KEY,
    event_key: str = _EVENT_KEY,
) -> Diagnosis:
    """Pure analysis over the fetched-order raw records. No I/O."""
    positions_by_pid: dict[str, list[int]] = defaultdict(list)
    events_by_pid: dict[str, list[str]] = defaultdict(list)
    for i, rec in enumerate(records):
        pid = coerce_productid(rec.get(productid_key))
        positions_by_pid[pid].append(i)
        events_by_pid[pid].append(str(rec.get(event_key)))

    groups: list[DupGroup] = []
    boundary_hist: Counter[int] = Counter()
    affected_events: list[str] = []
    straddle = in_page = scattered = 0
    duplicate_rows = 0

    for pid, positions in positions_by_pid.items():
        if len(positions) < 2:
            continue
        duplicate_rows += len(positions) - 1
        classification, gap, boundary_distance = classify_group(positions, page_size)
        pages = tuple(sorted({p // page_size for p in positions}))
        group = DupGroup(
            productid=pid,
            positions=tuple(sorted(positions)),
            pages=pages,
            classification=classification,
            position_gap=gap,
            boundary_distance=boundary_distance,
            event_ids=tuple(events_by_pid[pid]),
        )
        groups.append(group)
        if classification == "straddle":
            straddle += 1
            # The boundary this group crosses (the lower page's index + 1).
            boundary_hist[pages[0] + 1] += 1
            affected_events.extend(events_by_pid[pid])
        elif classification == "in_page":
            in_page += 1
        else:
            scattered += 1

    return Diagnosis(
        total_rows=len(records),
        distinct_productids=len(positions_by_pid),
        duplicate_groups=len(groups),
        duplicate_rows=duplicate_rows,
        straddle_groups=straddle,
        in_page_groups=in_page,
        scattered_groups=scattered,
        boundary_histogram=dict(sorted(boundary_hist.items())),
        affected_event_ids=sorted(set(affected_events)),
        groups=groups,
    )


def render_report(d: Diagnosis, page_size: int) -> str:
    """Human-readable verdict from a Diagnosis."""
    lines: list[str] = []
    lines.append("=== FDA seed straddle diagnosis (raw R2 payload, fetch order) ===")
    lines.append(f"total fetched rows:        {d.total_rows:>9,}")
    lines.append(f"distinct PRODUCTIDs:       {d.distinct_productids:>9,}")
    lines.append(f"duplicate groups:          {d.duplicate_groups:>9,}")
    lines.append(f"duplicate rows (collapsed by within_batch_dedup): {d.duplicate_rows:>9,}")
    lines.append("")
    lines.append(f"  STRADDLE (cross page boundary) -> inferred DROPS: {d.straddle_groups:>6,}")
    lines.append(f"  in-page (genuine source dup, no drop):           {d.in_page_groups:>6,}")
    lines.append(f"  scattered (anomalous — inspect):                 {d.scattered_groups:>6,}")
    lines.append("")
    if d.boundary_histogram:
        n_boundaries = len(d.boundary_histogram)
        lines.append(f"straddle dups span {n_boundaries} page boundaries (of ~53):")
        for boundary_page, count in d.boundary_histogram.items():
            row = boundary_page * page_size
            lines.append(f"  boundary at row {row:>7,} (page {boundary_page}): {count}")
        lines.append("")
    lines.append("VERDICT:")
    if d.straddle_groups > d.in_page_groups and d.straddle_groups > 0:
        lines.append(
            f"  TIE-BOUNDARY STRADDLE confirmed — the seed dropped ~{d.straddle_groups} "
            "distinct products (one per straddle dup, since fetched == RESULTCOUNT), so bronze "
            "is INCOMPLETE. Fix: re-seed sorted on the UNIQUE `productid` (no ties -> no "
            f"straddle). The {len(d.affected_event_ids)} affected recalleventids each lost a "
            "product (targeted re-fetch can recover just those)."
        )
    elif d.in_page_groups > 0 and d.straddle_groups == 0:
        lines.append(
            "  GENUINE SOURCE DUPLICATES — every duplicate sits within a single page, so FDA "
            "serves duplicate rows and NO product was dropped. The seed is COMPLETE at "
            f"{d.distinct_productids:,} distinct products; within_batch_dedup did the right thing."
        )
    else:
        lines.append(
            f"  MIXED/INCONCLUSIVE — {d.straddle_groups} straddle vs {d.in_page_groups} in-page "
            f"vs {d.scattered_groups} scattered. Inspect the per-group detail (use --show-groups)."
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--landing-path",
        default=_DEFAULT_LANDING_PATH,
        help="R2 key of the seed's raw payload (default: the 2026-06-01 seed).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=_PAGE_SIZE,
        help=f"Pagination page size; must match the seed run (default {_PAGE_SIZE}).",
    )
    parser.add_argument(
        "--show-groups",
        type=int,
        default=0,
        metavar="N",
        help="Also print the first N straddle/scattered duplicate groups for manual inspection.",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    client = R2LandingClient(settings)
    raw = client.get_raw(args.landing_path)
    records = json.loads(raw)
    if not isinstance(records, list):
        print(
            f"ERROR: landed payload is not a JSON array (got {type(records).__name__}).",
            file=sys.stderr,
        )
        return 2

    d = diagnose(records, page_size=args.page_size)
    print(render_report(d, args.page_size))

    if args.show_groups:
        flagged = [g for g in d.groups if g.classification != "in_page"]
        print(f"\n--- first {min(args.show_groups, len(flagged))} non-in-page groups ---")
        for g in flagged[: args.show_groups]:
            print(
                f"  PRODUCTID={g.productid} {g.classification} positions={g.positions} "
                f"pages={g.pages} gap={g.position_gap} boundary_dist={g.boundary_distance} "
                f"events={g.event_ids}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
