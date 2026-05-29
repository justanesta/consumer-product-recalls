"""Inspect FDA payloads landed in R2 across one or more sources of input.

Three mutually-exclusive source modes:

  --raw-landing-path <r2-key>      Fetch one R2 object, cache locally, inspect.
  --local-path <file>              Inspect an already-cached / locally-stored JSON.
  --date YYYY-MM-DD [YYYY-MM-DD]   One or more dates; queries extraction_runs for
                                   FDA raw_landing_paths and inspects each.

Output is human-readable per-field statistics (N, NULL%, distinct, length, value
distribution or samples) for each FDA field present in the records loaded.

Used by the Phase 6a foundation audit
(``documentation/audit/methodology.md`` + ``documentation/fda/field_audit_2026_w22.md``)
to validate cassette-derived assumptions against the broader R2 corpus.

Caching: R2 downloads are cached locally under ``data/exploratory/fda/`` (the
project's gitignored exploratory directory) keyed by the R2 basename with the
``.gz`` suffix stripped. Repeated invocations against the same R2 key skip the
R2 round-trip. Override the cache directory via ``--cache-dir`` if needed.

Usage:

    # One R2 key
    python scripts/fda/audit/inspect_landed_payloads.py \\
        --raw-landing-path fda/2026-05-15/<uuid>.json.gz

    # All FDA runs across three dates (DB resolves the keys)
    python scripts/fda/audit/inspect_landed_payloads.py \\
        --date 2026-01-15 2026-03-15 2026-05-15

    # Restrict the per-field summary to specific columns
    python scripts/fda/audit/inspect_landed_payloads.py \\
        --date 2026-05-15 --field PHASETXT CENTERCD VOLUNTARYTYPETXT

    # Offline (after a previous run cached the payload)
    python scripts/fda/audit/inspect_landed_payloads.py \\
        --local-path data/exploratory/fda/<uuid>.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date as _date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import DEFAULT_CACHE_DIR, resolve_cached_payload, summarize_records  # noqa: E402


def _resolve_via_extraction_runs(dates: list[_date]) -> list[str]:
    """Query ``extraction_runs`` for FDA raw_landing_paths matching given dates.

    Returns the list of R2 keys; caller resolves each via the cache."""
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
        where source = 'fda'
          and started_at::date in :dates
          and raw_landing_path is not null
          and raw_landing_path <> ''
        order by started_at
        """
    ).bindparams(sa.bindparam("dates", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(query, {"dates": dates}).all()
    print(
        f"# extraction_runs returned {len(rows)} FDA run(s) across {len(dates)} date(s)",
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
    """Parse a local FDA JSON payload — expect a list of dicts."""
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
        help="Single R2 object key (e.g., 'fda/2026-05-15/<uuid>.json.gz').",
    )
    source_group.add_argument(
        "--local-path",
        type=Path,
        help="Local path to an already-cached FDA JSON payload.",
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
        keys = _resolve_via_extraction_runs(dates)
        if not keys:
            print("# No FDA runs with raw_landing_path on those dates.", file=sys.stderr)
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
