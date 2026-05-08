"""Test whether a chosen identity tuple is stable across two NHTSA TSV captures.

Complements ``uniqueness_at_tuple.py`` (within-corpus row-uniqueness) and
the bronze-level ``assert_eleven_tuple_identity_stable.sql`` /
``assert_nine_tuple_identity_stable.sql`` SQL assertions (cross-run drift
on bronze data). This script answers the third question:

    Across two TSV captures from different days, does the same logical
    row keep the same tuple value? Or did NHTSA edit one of the tuple's
    fields for an existing recall?

Why this is needed in addition to the bronze SQL assertions: the bronze
versions only see rows that landed in bronze, which today is a
``--since=2023-12-01`` filtered slice. This script operates directly on
full-corpus TSV captures (PRE_2010, full POST_2010) and avoids spending
bronze inserts on Neon free tier.

Strategy: for each non-campno field X in the candidate tuple, group the
combined-corpus rows (tagged with their source ZIP) by the OTHER (k-1)
fields. Within each group, compute the set of X values seen in ZIP A
and the set seen in ZIP B. If A_set != B_set, that's drift — at least
one X value appears uniquely in one ZIP, indicating NHTSA's data shape
changed across the two captures.

This test is STRICTER than the bronze SQL ``count(distinct
raw_landing_path) > 1`` filter: it eliminates the Ferrari false-positive
class (multi-X groups where both ZIPs report both X values are not
flagged here).

Usage:
    python scripts/nhtsa/tsv_analysis/cross_corpus_stability.py \\
        --zip-a data/exploratory/nhtsa/may7-bronze.zip \\
        --zip-b data/exploratory/nhtsa/FLAT_RCL_POST_2010.zip \\
        --tuple campno,maketxt,modeltxt,yeartxt,compname,rcl_cmpt_id,\\
mfr_comp_ptno,mfr_comp_desc,mfr_comp_name

The first field of ``--tuple`` is treated as the "anchor" — it stays in
the GROUP BY for every per-field drift check. Defaults to ``campno``
(the natural anchor for NHTSA recalls).
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _lib import (  # noqa: E402
    iter_tsv_rows,
    parse_tuple_arg,
    print_zip_header,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--zip-a", required=True, type=Path, help="First TSV capture (e.g. yesterday)."
    )
    parser.add_argument(
        "--zip-b", required=True, type=Path, help="Second TSV capture (e.g. today)."
    )
    parser.add_argument(
        "--tuple",
        required=True,
        help=(
            "Comma-separated lowercase field names. "
            "First field is the GROUP BY anchor (kept in every check)."
        ),
    )
    parser.add_argument(
        "--samples-per-field",
        type=int,
        default=5,
        help="Drift-group samples to print per drifting field (default: 5).",
    )
    args = parser.parse_args()

    for label, path in (("--zip-a", args.zip_a), ("--zip-b", args.zip_b)):
        if not path.exists():
            print(f"ERROR: {label} {path} not found", file=sys.stderr)
            return 2

    try:
        tuple_indices = parse_tuple_arg(args.tuple)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if len(tuple_indices) < 2:
        print("ERROR: --tuple must have at least 2 fields (anchor + one to test)", file=sys.stderr)
        return 2

    tuple_field_names = [n.strip() for n in args.tuple.split(",")]

    print_zip_header(args.zip_a)
    print_zip_header(args.zip_b)
    print(f"Tuple under test: {tuple_field_names}  ({len(tuple_indices)} fields)")
    print(f"Anchor field (always grouped):     {tuple_field_names[0]}")
    print(f"Fields tested for cross-zip drift: {tuple_field_names[1:]}")
    print()

    # Each row: (source_label, field_values_tuple)
    rows_a = [
        tuple(r[i] if i < len(r) else "" for i in tuple_indices) for r in iter_tsv_rows(args.zip_a)
    ]
    rows_b = [
        tuple(r[i] if i < len(r) else "" for i in tuple_indices) for r in iter_tsv_rows(args.zip_b)
    ]
    print(f"ZIP A rows: {len(rows_a)}")
    print(f"ZIP B rows: {len(rows_b)}")
    print()

    n_fields = len(tuple_indices)
    drift_summary: list[tuple[str, int]] = []
    drift_samples_by_field: dict[str, list[tuple[tuple[str, ...], set[str], set[str]]]] = {}

    # Iterate "drop one field at a time" except the anchor (field 0).
    for drop_idx in range(1, n_fields):
        dropped_field = tuple_field_names[drop_idx]
        kept_positions = [i for i in range(n_fields) if i != drop_idx]

        # group_key = values of all kept tuple positions
        # cell = (set of dropped values seen in A, set in B)
        cells: dict[tuple[str, ...], tuple[set[str], set[str]]] = defaultdict(
            lambda: (set(), set())
        )
        for row in rows_a:
            key = tuple(row[i] for i in kept_positions)
            a_set, b_set = cells[key]
            a_set.add(row[drop_idx])
        for row in rows_b:
            key = tuple(row[i] for i in kept_positions)
            a_set, b_set = cells[key]
            b_set.add(row[drop_idx])

        drift_groups = [(key, a, b) for key, (a, b) in cells.items() if a and b and a != b]
        # The `a and b` filter requires the group to exist in both ZIPs at all —
        # otherwise a brand-new recall in B would falsely look like drift on every field.

        drift_summary.append((dropped_field, len(drift_groups)))
        drift_samples_by_field[dropped_field] = drift_groups[: args.samples_per_field]

    total = sum(n for _, n in drift_summary)
    print("=== Q1: per-field cross-corpus drift-group counts ===")
    print(f"Headline: TOTAL = {total} (0 means the tuple is stable across the two captures)")
    print()
    print(f"{'drifting_field':<20} {'drift_groups':>13}")
    print(f"{'-' * 20} {'-' * 13}")
    for field, n in sorted(drift_summary, key=lambda kv: (-kv[1], kv[0])):
        print(f"{field:<20} {n:>13}")
    print(f"{'TOTAL':<20} {total:>13}")
    print()

    if total == 0:
        return 0

    print("=== Q2: drift samples per field ===")
    print("For each group: kept fields | A-only values | B-only values | shared values")
    print()
    for field, samples in drift_samples_by_field.items():
        if not samples:
            continue
        print(f"--- {field} drift samples ---")
        for kept_values, a_set, b_set in samples:
            kept_preview = " | ".join(v if len(v) <= 30 else v[:30] + "…" for v in kept_values)
            a_only = sorted(a_set - b_set)
            b_only = sorted(b_set - a_set)
            shared = sorted(a_set & b_set)

            def _fmt(vs: list[str]) -> str:
                if not vs:
                    return "(none)"
                return ", ".join(repr(v) if v else "''" for v in vs[:5]) + (
                    f" +{len(vs) - 5}" if len(vs) > 5 else ""
                )

            print(f"  group: {kept_preview}")
            print(f"    A-only : {_fmt(a_only)}")
            print(f"    B-only : {_fmt(b_only)}")
            print(f"    shared : {_fmt(shared)}")
        print()

    return 1


if __name__ == "__main__":
    sys.exit(main())
