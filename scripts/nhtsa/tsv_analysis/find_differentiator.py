"""Identify which TSV fields vary within multi-row collision groups.

Mirrors the bronze SQL diagnostics:
- ``find_row_differentiator.sql`` Q1/Q2 (distinct-count per column across
  duplicate sets)
- ``investigate_residual_collisions.sql`` Q1/Q2 (NISSAN/ACHILLES anomaly
  diagnostics)

Given a tuple of grouping fields, this script:
1. Groups rows by the tuple's values.
2. Filters to multi-row collision groups (size >= 2).
3. For each collision group, lists the TSV fields whose values differ
   across the rows after stripping field 1 (RECORD_ID).
4. Reports a population-wide tally — "field X varies in N anomaly groups"
   — so you can see which fields would most disambiguate the residue if
   added to the tuple.
5. Optionally dumps full per-variant values for the top-N anomaly groups
   so you can character-diff them.

Optional filter via ``--filter field=value``: restrict the analysis to
rows where the named field has a specific value (e.g.
``--filter mfr_comp_ptno=""`` to focus on empty-ptno collisions). Filter
field name is lowercase; value can be empty string.

Usage:
    python scripts/nhtsa/tsv_analysis/find_differentiator.py \\
        --zip data/exploratory/nhtsa/may7-bronze.zip \\
        --tuple campno,maketxt,modeltxt,yeartxt,compname,rcl_cmpt_id,mfr_comp_ptno

    python scripts/nhtsa/tsv_analysis/find_differentiator.py \\
        --zip data/exploratory/nhtsa/may7-bronze.zip \\
        --tuple campno,maketxt,modeltxt,yeartxt,compname,rcl_cmpt_id \\
        --filter mfr_comp_ptno=
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _lib import (  # noqa: E402
    FIELD_NAMES,
    NAME_TO_INDEX,
    differing_fields_in_group,
    group_by_tuple,
    iter_tsv_rows,
    parse_tuple_arg,
    print_zip_header,
)


def _parse_filter(spec: str | None) -> tuple[int, str] | None:
    """Parse "fieldname=value" → (0-indexed position, value). Empty value allowed."""
    if spec is None:
        return None
    if "=" not in spec:
        raise ValueError(f"--filter must be field=value, got {spec!r}")
    name, _, value = spec.partition("=")
    name = name.strip().lower()
    if name not in NAME_TO_INDEX:
        known = ", ".join(sorted(NAME_TO_INDEX))
        raise ValueError(f"Unknown filter field {name!r}. Known: {known}")
    return NAME_TO_INDEX[name], value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zip", required=True, type=Path)
    parser.add_argument(
        "--tuple",
        required=True,
        help="Comma-separated lowercase field names defining the group key.",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help='Optional row filter as "field=value". Lowercase field; empty value allowed.',
    )
    parser.add_argument(
        "--show-anomalies",
        type=int,
        default=5,
        help="How many sample anomaly groups to dump in detail (default: 5).",
    )
    parser.add_argument(
        "--max-value-preview",
        type=int,
        default=120,
        help="Truncate per-variant value previews to this many chars (default: 120).",
    )
    args = parser.parse_args()

    if not args.zip.exists():
        print(f"ERROR: {args.zip} not found", file=sys.stderr)
        return 2

    try:
        tuple_indices = parse_tuple_arg(args.tuple)
        filter_spec = _parse_filter(args.filter)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print_zip_header(args.zip)
    print(f"Tuple: {args.tuple.split(',')} ({len(tuple_indices)} fields)")
    if filter_spec is not None:
        filter_idx, filter_value = filter_spec
        print(f"Filter: {FIELD_NAMES[filter_idx]} == {filter_value!r}")
    print()

    rows: list[list[str]] = []
    for fields in iter_tsv_rows(args.zip):
        if filter_spec is not None:
            filter_idx, filter_value = filter_spec
            if (fields[filter_idx] if filter_idx < len(fields) else "") != filter_value:
                continue
        rows.append(fields)

    print(f"Rows in scope: {len(rows)}")
    print()

    groups = group_by_tuple(rows, tuple_indices)

    single_row = sum(1 for v in groups.values() if len(v) == 1)
    collision_groups = [v for v in groups.values() if len(v) > 1]
    byte_identical = 0
    anomaly_groups: list[list[list[str]]] = []
    for members in collision_groups:
        distinct = {"\t".join(f[1:]) for f in members}
        if len(distinct) == 1:
            byte_identical += 1
        else:
            anomaly_groups.append(members)

    print(f"Distinct group keys:           {len(groups)}")
    print(f"  Single-row groups:           {single_row}")
    print(f"  Multi-row groups:            {len(collision_groups)}")
    print(f"    Byte-identical:            {byte_identical}")
    print(f"    Anomaly:                   {len(anomaly_groups)}")
    print()

    if not anomaly_groups:
        print("No anomalies — every collision group's rows are byte-identical")
        print("(modulo RECORD_ID). within_batch_dedup would handle this cleanly.")
        return 0

    # Population-wide tally of which TSV fields vary in anomaly groups.
    differing_field_counts: dict[str, int] = defaultdict(int)
    for members in anomaly_groups:
        for diff_name in differing_fields_in_group(members):
            differing_field_counts[diff_name] += 1

    print("Differing-field frequency across all anomaly groups:")
    print("(adding any of these to the tuple would disambiguate that many groups.)")
    for name, count in sorted(differing_field_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {name}: {count} groups")
    print()

    if args.show_anomalies <= 0:
        return 1

    # Sort anomaly groups largest-first so the dump emphasizes the worst cases.
    anomaly_groups.sort(key=lambda members: -len(members))

    print(f"Sample anomaly groups (top {min(args.show_anomalies, len(anomaly_groups))}):")
    print()
    for shown_idx, members in enumerate(anomaly_groups[: args.show_anomalies], 1):
        # The group key is constant within `members`; pull from the first row.
        key_values = tuple(members[0][i] if i < len(members[0]) else "" for i in tuple_indices)
        diffing = differing_fields_in_group(members)
        distinct_lines = sorted({"\t".join(f[1:]) for f in members})

        print(f"Group {shown_idx}:")
        print(f"  Group key: {key_values}")
        print(f"  Total rows:                {len(members)}")
        print(f"  Distinct after RECORD_ID:  {len(distinct_lines)}")
        print(f"  Differing field(s):        {', '.join(diffing) if diffing else '(none)'}")
        print("  Per-variant values for differing fields:")
        for variant_idx, distinct_line in enumerate(distinct_lines, 1):
            stripped_fields = distinct_line.split("\t")
            value_pairs: list[str] = []
            for diff_name in diffing:
                tsv_idx = FIELD_NAMES.index(diff_name)
                stripped_idx = tsv_idx - 1  # subtract 1 for stripped RECORD_ID
                v = (
                    stripped_fields[stripped_idx]
                    if 0 <= stripped_idx < len(stripped_fields)
                    else ""
                )
                if len(v) > args.max_value_preview:
                    v = v[: args.max_value_preview] + "…"
                value_pairs.append(f"{diff_name}={v!r}")
            print(f"    variant {variant_idx}: {' | '.join(value_pairs)}")
        print()

    if len(anomaly_groups) > args.show_anomalies:
        print(
            f"... ({len(anomaly_groups) - args.show_anomalies} more anomaly groups not shown) ..."
        )

    return 1


if __name__ == "__main__":
    sys.exit(main())
