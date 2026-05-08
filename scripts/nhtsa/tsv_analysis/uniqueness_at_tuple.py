"""Report row-uniqueness of a chosen tuple of fields against a NHTSA TSV.

Mirrors the bronze SQL diagnostics:
- ``verify_six_tuple_identity.sql`` Q1b (total rows vs distinct N-tuples)
- ``investigate_tire_collision.sql`` Q3 (7-tuple uniqueness measure)
- ``verify_natural_key_candidate.sql`` Q-C (semantic-composite fallback)

Useful when you want to test a specific tuple shape without going through
``identity_search.py``'s full iterative widening — e.g., "is the 6-tuple
without rcl_cmpt_id row-unique?", or "what about (campno, rcl_cmpt_id)
alone?". Reports total rows, distinct tuples, single-row groups, and
collision groups, with a sample of the worst collisions.

Usage:
    python scripts/nhtsa/tsv_analysis/uniqueness_at_tuple.py \\
        --zip data/exploratory/nhtsa/may7-bronze.zip \\
        --tuple campno,maketxt,modeltxt,yeartxt,compname,rcl_cmpt_id,mfr_comp_ptno
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _lib import (  # noqa: E402
    group_by_tuple,
    iter_tsv_rows,
    parse_tuple_arg,
    print_zip_header,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zip", required=True, type=Path)
    parser.add_argument(
        "--tuple",
        required=True,
        help='Comma-separated lowercase field names, e.g. "campno,maketxt,modeltxt".',
    )
    parser.add_argument(
        "--top-collisions",
        type=int,
        default=10,
        help="How many of the largest collision groups to display (default: 10).",
    )
    args = parser.parse_args()

    if not args.zip.exists():
        print(f"ERROR: {args.zip} not found", file=sys.stderr)
        return 2

    try:
        tuple_indices = parse_tuple_arg(args.tuple)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print_zip_header(args.zip)

    print(f"Tuple: {args.tuple.split(',')}")
    print(f"  ({len(tuple_indices)} fields)")
    print()

    all_rows = list(iter_tsv_rows(args.zip))
    groups = group_by_tuple(all_rows, tuple_indices)

    total_rows = len(all_rows)
    distinct_tuples = len(groups)
    single_row = sum(1 for v in groups.values() if len(v) == 1)
    collision = distinct_tuples - single_row
    excess_rows = total_rows - distinct_tuples

    print(f"Total rows:                  {total_rows}")
    print(f"Distinct tuples:             {distinct_tuples}")
    print(f"Single-row groups:           {single_row}")
    print(f"Multi-row collision groups:  {collision}")
    print(f"Excess rows from collisions: {excess_rows}")
    print(f"Row-unique:                  {'yes' if collision == 0 else 'no'}")
    print()

    if collision > 0 and args.top_collisions > 0:
        print(f"Top {args.top_collisions} collision groups by row count:")
        sorted_collisions = sorted(
            ((key, members) for key, members in groups.items() if len(members) > 1),
            key=lambda kv: -len(kv[1]),
        )
        for key, members in sorted_collisions[: args.top_collisions]:
            preview = " | ".join(v if len(v) <= 40 else v[:40] + "…" for v in key)
            print(f"  {len(members):>5} rows  |  {preview}")
        print()

    return 0 if collision == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
