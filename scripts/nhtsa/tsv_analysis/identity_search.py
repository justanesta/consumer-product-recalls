"""Iteratively search for the minimum row-unique identity tuple in an NHTSA TSV.

Starts from the ADR 0030 7-tuple ``(campno, maketxt, modeltxt, yeartxt,
compname, rcl_cmpt_id, mfr_comp_ptno)``. At each iteration:

1. Group all TSV rows by the current tuple's field values.
2. For every multi-row group, classify it as either:
   - Byte-identical (rows differ only in RECORD_ID; safe to dedup via
     within_batch_dedup), or
   - Anomaly (rows differ on a non-RECORD_ID field; would crash
     ``WithinBatchIdentityCollisionError`` at extract time).
3. If 0 anomalies → the current tuple is row-unique. Done.
4. Otherwise, identify the TSV field that varies in the most anomaly
   groups (and isn't already in the tuple). Add it. Loop.

The output is the iteration history plus the final row-unique tuple.
This is the architectural answer for the BronzeLoader ``identity_fields``
config — what tuple of fields, when used as the bronze identity, makes
NHTSA's TSV's same-logical-row guarantee match the bronze loader's
dedup contract.

Per ADR 0030, the ``--zip`` should be a TSV that the production loader
will see — typically ``FLAT_RCL_POST_2010.zip`` for the daily incremental
path, plus optionally ``FLAT_RCL_PRE_2010.zip`` for the historical-seed
path. Run against both and confirm the same minimum tuple works for both.

Usage:
    python scripts/nhtsa/tsv_analysis/identity_search.py \\
        --zip data/exploratory/nhtsa/may7-bronze.zip
    python scripts/nhtsa/tsv_analysis/identity_search.py \\
        --zip data/exploratory/nhtsa/FLAT_RCL_PRE_2010.zip
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

# Make `_lib` importable when running this file directly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _lib import (  # noqa: E402 — sys.path manipulation must come first
    NAME_TO_INDEX,
    differing_fields_in_group,
    group_by_tuple,
    iter_tsv_rows,
    print_zip_header,
)

# ADR 0030's starting tuple (lowercase field names).
INITIAL_TUPLE = (
    "campno",
    "maketxt",
    "modeltxt",
    "yeartxt",
    "compname",
    "rcl_cmpt_id",
    "mfr_comp_ptno",
)


def _classify_group(
    members: list[list[str]],
) -> tuple[bool, bool]:
    """Return (is_collision, is_anomaly).

    is_collision: more than one row in the group.
    is_anomaly:   collision AND rows differ on a non-RECORD_ID field.
    """
    if len(members) < 2:
        return False, False
    distinct_stripped = {"\t".join(f[1:]) for f in members}
    return True, len(distinct_stripped) > 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--zip",
        required=True,
        type=Path,
        help="Path to a NHTSA flat-file ZIP (e.g. FLAT_RCL_POST_2010.zip).",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=10,
        help="Stop the iterative widening after this many rounds (default: 10).",
    )
    args = parser.parse_args()

    if not args.zip.exists():
        print(f"ERROR: {args.zip} not found", file=sys.stderr)
        return 2

    print_zip_header(args.zip)

    print("Loading TSV rows into memory...", flush=True)
    all_rows = list(iter_tsv_rows(args.zip))
    print(f"  {len(all_rows)} rows loaded.")
    print()

    tuple_names: list[str] = list(INITIAL_TUPLE)

    for iteration in range(args.max_iterations + 1):
        try:
            tuple_indices = tuple(NAME_TO_INDEX[n] for n in tuple_names)
        except KeyError as exc:
            print(f"ERROR: invalid field name in tuple: {exc}", file=sys.stderr)
            return 2

        groups = group_by_tuple(all_rows, tuple_indices)

        single_row_groups = 0
        collision_groups = 0
        byte_identical_groups = 0
        anomaly_groups: list[list[list[str]]] = []
        for members in groups.values():
            is_collision, is_anomaly = _classify_group(members)
            if not is_collision:
                single_row_groups += 1
                continue
            collision_groups += 1
            if is_anomaly:
                anomaly_groups.append(members)
            else:
                byte_identical_groups += 1

        anomaly_count = len(anomaly_groups)

        print(f"Iteration {iteration}: tuple = {tuple_names}")
        print(f"  Distinct tuples:         {len(groups)}")
        print(f"  Single-row groups:       {single_row_groups}")
        print(f"  Multi-row collisions:    {collision_groups}")
        print(f"    Byte-identical:        {byte_identical_groups}")
        print(f"    Anomaly:               {anomaly_count}")
        print()

        if anomaly_count == 0:
            print("=" * 64)
            print("ROW-UNIQUE TUPLE FOUND")
            print("=" * 64)
            print(f"Tuple ({len(tuple_names)} fields):")
            for name in tuple_names:
                print(f"  - {name}")
            print()
            print(f"{byte_identical_groups} byte-duplicate group(s) will dedup")
            print("via BronzeLoader.within_batch_dedup at extract time.")
            print()
            print("Suggested BronzeLoader.identity_fields:")
            print(f"  identity_fields=({', '.join(repr(n) for n in tuple_names)})")
            return 0

        # Tally which TSV fields vary across the anomaly residue.
        differing_field_counts: dict[str, int] = defaultdict(int)
        for members in anomaly_groups:
            for diff_name in differing_fields_in_group(members):
                differing_field_counts[diff_name.lower()] += 1

        # Pick the most-frequently-differing field that isn't already in our tuple.
        in_tuple = set(tuple_names)
        candidates = sorted(
            (
                (name, count)
                for name, count in differing_field_counts.items()
                if name not in in_tuple
            ),
            key=lambda kv: -kv[1],
        )

        if not candidates:
            # All fields that differ are already in the tuple. Genuine
            # data anomaly — anomaly groups have rows that differ on
            # source_recall_id alone (impossible by construction since
            # we strip RECORD_ID before checking), or some pathological
            # case. Surface clearly.
            print("=" * 64)
            print("WIDENING FAILED")
            print("=" * 64)
            print(f"Anomaly count: {anomaly_count}")
            print("All TSV fields that vary in anomaly groups are already")
            print("in the identity tuple. Inspect the anomaly groups manually")
            print("via find_differentiator.py with the same tuple.")
            return 1

        next_field, next_count = candidates[0]
        print(f"  → Adding '{next_field}' to tuple")
        print(f"    (varies in {next_count} of {anomaly_count} anomaly groups)")
        if len(candidates) > 1:
            print("    Other candidates:")
            for name, count in candidates[1:5]:
                print(f"      {name}: {count} groups")
        print()
        tuple_names.append(next_field)

    print(f"⚠ max_iterations ({args.max_iterations}) exceeded; tuple = {tuple_names}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
