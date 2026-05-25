"""
Fetch raw R2 landings for a specific (source_recall_id, langcode) pair and
diff selected fields across snapshots.

Used to confirm whether a field change observed in bronze is upstream-FSIS
(present in the landed JSON) or a bronze-side transform (present in bronze
but absent in the landed JSON). The bronze Pydantic schema preserves '' for
Optional[str] fields verbatim per ADR 0027, so a bronze '' is in principle
what FSIS sent — this script makes that claim falsifiable per-recall.

Default target: PHA-04302026-01 / English — first observed field_establishment
and field_company_media_contact populated-to-empty transition (snap 1 → snap 2,
2026-05-01 → 2026-05-02). See `documentation/usda/bilingual_and_lmd_findings.md`
PHA-04302026-01 subsection for the bronze-side picture this script complements.

Usage:
    python scripts/usda_recalls/inspect_raw_landing_for_recall.py
    python scripts/usda_recalls/inspect_raw_landing_for_recall.py \
        --recall-id PHA-04302026-01 --langcode English

Output sections:
    1. Snapshot inventory (extraction_timestamp, raw_landing_path, content_hash).
    2. Per-field per-snapshot value table for the FIELDS_TO_DIFF set
       (truncated for readability — full payload is the R2 object).
    3. Populated/empty/null summary for the erasure-candidate fields
       (field_establishment, field_company_media_contact) — the headline
       table that confirms or refutes the upstream-erasure hypothesis.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import sqlalchemy as sa

from src.config.settings import Settings
from src.landing.r2 import R2LandingClient

FIELDS_TO_DIFF: list[str] = [
    "field_summary",
    "field_establishment",
    "field_company_media_contact",
    "field_last_modified_date",
]

ERASURE_FIELDS: list[str] = [
    "field_establishment",
    "field_company_media_contact",
]


def _value_label(v: Any) -> str:
    if v is None:
        return "null"
    if v == "":
        return "empty string"
    return "populated"


def _value_repr(v: Any, max_len: int) -> str:
    if v is None:
        return "<null>"
    if v == "":
        return "<empty string>"
    s = repr(v)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recall-id", default="PHA-04302026-01")
    parser.add_argument("--langcode", default="English")
    parser.add_argument(
        "--snippet-len",
        type=int,
        default=160,
        help="Max chars per field value in the diff table (default: 160).",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    engine = sa.create_engine(settings.neon_database_url.get_secret_value())
    r2 = R2LandingClient(settings)

    query = sa.text(
        """
        select extraction_timestamp, raw_landing_path, content_hash
        from usda_fsis_recalls_bronze
        where trim(source_recall_id) = :rid
          and langcode = :lang
        order by extraction_timestamp
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, {"rid": args.recall_id, "lang": args.langcode}).all()

    if not rows:
        print(f"No bronze rows for ({args.recall_id!r}, {args.langcode!r}).")
        return 1

    print(f"=== Snapshots in bronze for ({args.recall_id}, {args.langcode}) ===\n")
    for i, row in enumerate(rows, start=1):
        print(
            f"  snap {i}: extraction={row.extraction_timestamp} "
            f"hash={row.content_hash[:8]}... key={row.raw_landing_path}"
        )

    snapshots: list[dict[str, Any]] = []
    print("\n=== Fetching raw landings from R2 ===\n")
    for i, row in enumerate(rows, start=1):
        print(f"  snap {i}: {row.raw_landing_path}")
        raw_bytes = r2.get_raw(row.raw_landing_path)
        payload = json.loads(raw_bytes)
        if not isinstance(payload, list):
            print(f"    unexpected payload shape: {type(payload).__name__} — skipping")
            continue
        match = next(
            (
                rec
                for rec in payload
                if rec.get("field_recall_number", "").strip() == args.recall_id
                and rec.get("langcode") == args.langcode
            ),
            None,
        )
        if match is None:
            print("    target recall not present in this landing — skipping")
            continue
        snapshots.append(
            {
                "snap_n": i,
                "extraction": row.extraction_timestamp,
                "key": row.raw_landing_path,
                "record": match,
            }
        )

    if not snapshots:
        print("\nNo matching records in any landing payload. Nothing to diff.")
        return 1

    print(f"\n=== Per-field values across {len(snapshots)} snapshot(s) ===\n")
    for field in FIELDS_TO_DIFF:
        print(f"--- {field} ---")
        for s in snapshots:
            v = s["record"].get(field)
            print(f"  snap {s['snap_n']} ({s['extraction']}): {_value_repr(v, args.snippet_len)}")
        print()

    print("=== Populated / empty / null summary for erasure-candidate fields ===\n")
    print("  Confirms upstream-FSIS erasure (vs bronze transform). ADR 0027 means")
    print("  bronze preserves '' verbatim, so any 'empty string' below is what FSIS sent.\n")
    header_left = "snap (extraction)"
    col_width = max(35, max(len(f) for f in ERASURE_FIELDS) + 4)
    print(f"  {header_left:<32s}  " + "  ".join(f"{f:<{col_width}s}" for f in ERASURE_FIELDS))
    for s in snapshots:
        row_label = f"snap {s['snap_n']} ({s['extraction']})"
        cells = [_value_label(s["record"].get(f)) for f in ERASURE_FIELDS]
        print(f"  {row_label:<32s}  " + "  ".join(f"{c:<{col_width}s}" for c in cells))

    return 0


if __name__ == "__main__":
    sys.exit(main())
