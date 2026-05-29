"""Inspect USDA recall payloads landed in R2 across one or more sources of input.

Three mutually-exclusive source modes (same shape as FDA's inspect_landed_payloads.py):

  --raw-landing-path <r2-key>      Fetch one R2 object, cache locally, inspect.
  --local-path <file>              Inspect an already-cached / locally-stored JSON.
  --date YYYY-MM-DD [YYYY-MM-DD]   One or more dates; queries extraction_runs for
                                   USDA raw_landing_paths and inspects each.

Plus a USDA-specific filter:

  --langcode English | Spanish     Restrict the per-field summary to one
                                   bilingual sibling family. Default: both.
                                   English-only matches what staging filters to.

Output is human-readable per-field statistics (N, NULL%, distinct, length,
value distribution or samples) for each USDA field present in the records loaded.

Used by the Phase 6a foundation audit
(``documentation/audit/methodology.md`` + ``documentation/usda/field_audit_2026_w22.md``)
to validate cassette/PDF-derived assumptions against the broader R2 corpus.

Caching: R2 downloads are cached locally under ``data/exploratory/usda_recalls/``
(gitignored) keyed by the R2 basename with the ``.gz`` suffix stripped.
Repeated invocations against the same R2 key skip the R2 round-trip.

USDA payload shape: the recall API returns a flat JSON array (no envelope).
Bronze loader writes that array verbatim to R2. Each record is a dict with
``field_`` prefixed keys plus ``langcode`` and ``field_has_spanish``.

Usage:

    # All USDA runs across three dates (DB resolves the keys)
    python scripts/usda_recalls/audit/inspect_landed_payloads.py \\
        --date 2026-01-15 2026-03-15 2026-05-15

    # English-only — matches staging's filter
    python scripts/usda_recalls/audit/inspect_landed_payloads.py \\
        --date 2026-05-15 --langcode English

    # Restrict to specific fields (the §4 lift candidates)
    python scripts/usda_recalls/audit/inspect_landed_payloads.py \\
        --date 2026-05-15 --langcode English \\
        --field field_recall_reason field_risk_level field_states \\
                field_related_to_outbreak field_processing

    # Offline (after a previous run cached the payload)
    python scripts/usda_recalls/audit/inspect_landed_payloads.py \\
        --local-path data/exploratory/usda_recalls/<uuid>.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date as _date
from itertools import groupby
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import DEFAULT_CACHE_DIR, resolve_cached_payload, summarize_records  # noqa: E402


def _resolve_via_extraction_runs(
    dates: list[_date],
    limit_per_date: int | None = None,
) -> list[str]:
    """Query ``extraction_runs`` for USDA raw_landing_paths matching given dates.

    If ``limit_per_date`` is set, returns only the N most recent runs per date.
    USDA's recall API returns the full ~2000-record corpus on every fetch
    (Findings A-G in recall_api_observations.md), so multiple same-date runs
    yield duplicate data; pass ``limit_per_date=1`` for the canonical snapshot
    per date.
    """
    import sqlalchemy as sa  # noqa: PLC0415

    from src.config.settings import Settings  # noqa: PLC0415

    settings = Settings()  # type: ignore[call-arg]
    engine = sa.create_engine(settings.neon_database_url.get_secret_value())
    # `raw_landing_path <> ''` filter: some USDA runs land with extracted=0 /
    # inserted=0 and an empty-string raw_landing_path (rather than NULL — e.g.,
    # an aborted or no-op run). Path("").name is "" which would make
    # resolve_cached_payload return the cache directory itself, crashing the
    # subsequent read_bytes() call. Exclude these at the source.
    query = sa.text(
        """
        select started_at, raw_landing_path, records_extracted, records_inserted
        from extraction_runs
        where source = 'usda'
          and started_at::date in :dates
          and raw_landing_path is not null
          and raw_landing_path <> ''
        order by started_at
        """
    ).bindparams(sa.bindparam("dates", expanding=True))
    with engine.connect() as conn:
        rows = list(conn.execute(query, {"dates": dates}).all())
    total = len(rows)

    if limit_per_date is not None:
        kept: list[Any] = []
        # rows are ordered by started_at ASC, so same-date rows are consecutive
        for _, group in groupby(rows, key=lambda r: r.started_at.date()):
            group_list = sorted(group, key=lambda r: r.started_at, reverse=True)
            kept.extend(group_list[:limit_per_date])
        rows = sorted(kept, key=lambda r: r.started_at)
        print(
            f"# extraction_runs returned {total} USDA run(s) across {len(dates)} date(s); "
            f"--limit-per-date={limit_per_date} kept {len(rows)}",
            file=sys.stderr,
        )
    else:
        print(
            f"# extraction_runs returned {total} USDA run(s) across {len(dates)} date(s)",
            file=sys.stderr,
        )

    for row in rows:
        print(
            f"#   {row.started_at}: extracted={row.records_extracted}, "
            f"inserted={row.records_inserted}, key={row.raw_landing_path}",
            file=sys.stderr,
        )
    return [row.raw_landing_path for row in rows]


def _load_records_from_path(path: Path) -> list[dict[str, Any]]:
    """Parse a local USDA JSON payload — expect a flat array of dicts."""
    raw_bytes = path.read_bytes()
    payload = json.loads(raw_bytes)
    if not isinstance(payload, list):
        print(
            f"# Skipping {path.name}: unexpected payload type {type(payload).__name__}",
            file=sys.stderr,
        )
        return []
    return [r for r in payload if isinstance(r, dict)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--raw-landing-path",
        help="Single R2 object key (e.g., 'usda/2026-05-15/<uuid>.json.gz').",
    )
    source_group.add_argument(
        "--local-path",
        type=Path,
        help="Local path to an already-cached USDA recall JSON payload.",
    )
    source_group.add_argument(
        "--date",
        nargs="+",
        help="One or more ISO dates (YYYY-MM-DD); DB-resolves R2 keys via extraction_runs.",
    )
    parser.add_argument(
        "--langcode",
        choices=["English", "Spanish"],
        default=None,
        help="Restrict to one langcode (default: both — mirrors raw R2 payload).",
    )
    parser.add_argument(
        "--limit-per-date",
        type=int,
        default=None,
        help=(
            "When multiple USDA runs share a date, take only the N most recent. "
            "USDA returns the full ~2000-record corpus on every fetch, so "
            "N=1 captures the canonical snapshot per date with no information "
            "loss. Default: process all runs (which may multiply duplicate "
            "data N× for analysis purposes — proportions stay correct but raw "
            "counts inflate)."
        ),
    )
    parser.add_argument(
        "--field",
        nargs="*",
        help="Restrict summary to these field names (default: union of all keys observed).",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Cache directory (default: {DEFAULT_CACHE_DIR}). Gitignored.",
    )
    args = parser.parse_args()

    all_records: list[dict[str, Any]] = []

    if args.local_path:
        if not args.local_path.exists():
            print(f"ERROR: {args.local_path} not found", file=sys.stderr)
            return 2
        all_records.extend(_load_records_from_path(args.local_path))
    elif args.raw_landing_path:
        cached = resolve_cached_payload(args.raw_landing_path, args.cache_dir)
        all_records.extend(_load_records_from_path(cached))
    else:
        try:
            dates: list[_date] = [_date.fromisoformat(d) for d in args.date]
        except ValueError as exc:
            print(f"ERROR: bad date format ({exc})", file=sys.stderr)
            return 2
        keys = _resolve_via_extraction_runs(dates, limit_per_date=args.limit_per_date)
        if not keys:
            print("# No USDA runs with raw_landing_path on those dates.", file=sys.stderr)
            return 1
        for key in keys:
            cached = resolve_cached_payload(key, args.cache_dir)
            all_records.extend(_load_records_from_path(cached))

    if not all_records:
        print("# No records loaded — nothing to summarize.", file=sys.stderr)
        return 1

    pre_filter_n = len(all_records)
    if args.langcode is not None:
        all_records = [r for r in all_records if r.get("langcode") == args.langcode]
        print(
            f"# Filtered by langcode={args.langcode}: {pre_filter_n} → {len(all_records)} records",
            file=sys.stderr,
        )

    if not all_records:
        print(
            f"# No records after langcode={args.langcode} filter.",
            file=sys.stderr,
        )
        return 1

    print(f"# Loaded {len(all_records)} total records across all sources.")
    print()
    print(summarize_records(all_records, fields=args.field))
    return 0


if __name__ == "__main__":
    sys.exit(main())
