"""Inspect USDA FSIS Establishment Listing payloads landed in R2.

Three mutually-exclusive source modes (same shape as the FDA/USDA-recall
inspect scripts):

  --raw-landing-path <r2-key>      Fetch one R2 object, cache locally, inspect.
  --local-path <file>              Inspect an already-cached / locally-stored JSON.
  --date YYYY-MM-DD [YYYY-MM-DD]   One or more dates; queries extraction_runs for
                                   USDA-establishments raw_landing_paths.

Output is human-readable per-field statistics (N, NULL%, distinct, length,
value distribution or samples) for each Establishment Listing field present.

The Establishment Listing API ("MPI Directory") returns the full ~7,945-record
dump on every extraction (no pagination, no ETag, no incremental cursor — see
``documentation/usda/establishment_api_observations.md`` Findings A-G). So
a single date's payload is the full corpus snapshot.

Used by the Phase 6a foundation audit
(``documentation/audit/methodology.md`` + ``documentation/usda/field_audit_2026_w22.md``)
to validate PDF-derived assumptions against the broader R2 corpus, especially
the firm-relationship question (§6 of the audit doc) — populate rates for
``establishment_number``, ``dbas``, address fields, ``geolocation``/``county``
``false`` sentinels, and the activities/dbas JSON-array shapes.

Caching: R2 downloads are cached locally under
``data/exploratory/usda_establishments/`` (gitignored) keyed by the R2 basename
with the ``.gz`` suffix stripped.

Payload shape: flat JSON array of records. Each record has snake_case keys
(no ``field_`` prefix — different from the recall API). Note the JSON-boolean
``false`` sentinel on ``geolocation``/``county`` (Finding C); ``_lib._is_null``
treats it as null so NULL-rate distributions reflect real missingness.

Usage:

    # One full corpus snapshot (one date = full ~7,945 records)
    python scripts/usda_establishments/audit/inspect_landed_payloads.py \\
        --date 2026-05-15

    # Restrict to firm-relationship-relevant fields
    python scripts/usda_establishments/audit/inspect_landed_payloads.py \\
        --date 2026-05-15 \\
        --field establishment_number establishment_name dbas address city \\
                state zip phone duns_number size activities

    # Spread of dates to see if the MPI corpus changes (it usually doesn't fast)
    python scripts/usda_establishments/audit/inspect_landed_payloads.py \\
        --date 2026-04-01 2026-04-15 2026-05-01 2026-05-15

    # Offline
    python scripts/usda_establishments/audit/inspect_landed_payloads.py \\
        --local-path data/exploratory/usda_establishments/<uuid>.json
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
    """Query ``extraction_runs`` for USDA-establishment raw_landing_paths.

    If ``limit_per_date`` is set, returns only the N most recent runs per date.
    The MPI Directory API returns the full ~7,945-record corpus on every fetch
    (Findings A-G in establishment_api_observations.md), so multiple same-date
    runs yield duplicate data; pass ``limit_per_date=1`` for the canonical
    snapshot per date.
    """
    import sqlalchemy as sa  # noqa: PLC0415

    from src.config.settings import Settings  # noqa: PLC0415

    settings = Settings()  # type: ignore[call-arg]
    engine = sa.create_engine(settings.neon_database_url.get_secret_value())
    # `raw_landing_path <> ''` filter: some runs land with extracted=0 and an
    # empty-string raw_landing_path (rather than NULL). Path("").name is ""
    # which would make resolve_cached_payload return the cache directory itself,
    # crashing the subsequent read_bytes() call. Exclude at the source.
    query = sa.text(
        """
        select started_at, raw_landing_path, records_extracted, records_inserted
        from extraction_runs
        where source = 'usda_establishments'
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
        for _, group in groupby(rows, key=lambda r: r.started_at.date()):
            group_list = sorted(group, key=lambda r: r.started_at, reverse=True)
            kept.extend(group_list[:limit_per_date])
        rows = sorted(kept, key=lambda r: r.started_at)
        print(
            f"# extraction_runs returned {total} USDA-establishment run(s) across "
            f"{len(dates)} date(s); --limit-per-date={limit_per_date} kept {len(rows)}",
            file=sys.stderr,
        )
    else:
        print(
            f"# extraction_runs returned {total} USDA-establishment run(s) across "
            f"{len(dates)} date(s)",
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
    """Parse a local USDA-establishment JSON payload — expect a flat array of dicts."""
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
        help="Single R2 object key (e.g., 'usda_establishments/2026-05-15/<uuid>.json.gz').",
    )
    source_group.add_argument(
        "--local-path",
        type=Path,
        help="Local path to an already-cached USDA-establishment JSON payload.",
    )
    source_group.add_argument(
        "--date",
        nargs="+",
        help="One or more ISO dates (YYYY-MM-DD); DB-resolves R2 keys via extraction_runs.",
    )
    parser.add_argument(
        "--field",
        nargs="*",
        help="Restrict summary to these field names (default: union of all keys observed).",
    )
    parser.add_argument(
        "--limit-per-date",
        type=int,
        default=None,
        help=(
            "When multiple establishment-listing runs share a date, take only "
            "the N most recent. The MPI Directory API returns the full ~7,945 "
            "record corpus on every fetch, so N=1 captures the canonical "
            "snapshot per date with no information loss. Default: process all "
            "runs (raw counts inflate N× per date for analysis purposes; "
            "proportions stay correct)."
        ),
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
            print(
                "# No USDA-establishment runs with raw_landing_path on those dates.",
                file=sys.stderr,
            )
            return 1
        for key in keys:
            cached = resolve_cached_payload(key, args.cache_dir)
            all_records.extend(_load_records_from_path(cached))

    if not all_records:
        print("# No records loaded — nothing to summarize.", file=sys.stderr)
        return 1

    print(f"# Loaded {len(all_records)} total records across all sources.")
    print()
    print(summarize_records(all_records, fields=args.field))
    return 0


if __name__ == "__main__":
    sys.exit(main())
