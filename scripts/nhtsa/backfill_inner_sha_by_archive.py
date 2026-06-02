"""Backfill extraction_runs.response_inner_content_sha256_by_archive from R2 manifests.

Migration 0021 added a JSONB ``{archive_url: inner_sha256}`` column to ``extraction_runs`` so
the NHTSA deep-rescan short-circuit (W6) can compare BOTH archives' inner-content SHAs to the
prior run. Pre-existing NHTSA deep-rescan runs predate the column but recorded both SHAs in
their R2 deep-rescan manifest (``NhtsaDeepRescanLoader.land_raw``). This one-off reads each
such run's manifest and populates the new column so the short-circuit has a baseline from day
one.

Idempotent: it targets only NHTSA runs whose landing path is a ``.json`` manifest and whose
by-archive column is still NULL, so re-running is a no-op once applied.

Run (operator, against the target Neon branch):
    python scripts/nhtsa/backfill_inner_sha_by_archive.py [--dry-run]

The pure transform (``manifest_to_sha_map``) is unit-tested in
``tests/scripts/test_backfill_inner_sha_by_archive.py``; DB/R2 I/O is kept behind functions so
the parse layer stays testable without network/DB (mirrors the extractor ``_parse_*`` split).
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import sqlalchemy as sa

from src.config.db import make_engine
from src.config.settings import Settings
from src.extractors._tables import extraction_runs
from src.landing.r2 import R2LandingClient

_NHTSA_SOURCE = "nhtsa"


def manifest_to_sha_map(manifest: dict[str, Any]) -> dict[str, str]:
    """Extract ``{archive_url: inner_content_sha256}`` from a NHTSA deep-rescan manifest.

    Manifest shape (``NhtsaDeepRescanLoader.land_raw``)::

        {"deep_rescan": true,
         "sources": [{"url": ..., "inner_content_sha256": ...}, ...]}

    Only entries carrying both a ``url`` and a non-empty ``inner_content_sha256`` are kept.
    """
    sources = manifest.get("sources", [])
    return {
        source["url"]: source["inner_content_sha256"]
        for source in sources
        if source.get("url") and source.get("inner_content_sha256")
    }


def _manifest_runs(conn: sa.Connection) -> list[tuple[int, str]]:
    """``(id, raw_landing_path)`` for NHTSA runs whose landing path is a JSON manifest and
    whose by-archive column is still NULL (so re-runs skip already-backfilled rows)."""
    rows = conn.execute(
        sa.select(extraction_runs.c.id, extraction_runs.c.raw_landing_path).where(
            extraction_runs.c.source == _NHTSA_SOURCE,
            extraction_runs.c.raw_landing_path.like("%.json%"),
            extraction_runs.c.response_inner_content_sha256_by_archive.is_(None),
        )
    ).all()
    return [(int(r[0]), str(r[1])) for r in rows]


def backfill(engine: sa.Engine, r2_client: R2LandingClient, *, dry_run: bool) -> int:
    """Populate the by-archive column for eligible NHTSA runs. Returns the count updated."""
    with engine.connect() as conn:
        runs = _manifest_runs(conn)

    updated = 0
    for run_id, landing_path in runs:
        try:
            manifest = json.loads(r2_client.get_raw(landing_path))
        except Exception as exc:  # noqa: BLE001 — skip one unreadable manifest, keep going
            print(f"  run {run_id}: SKIP — cannot read manifest {landing_path}: {exc}")
            continue
        sha_map = manifest_to_sha_map(manifest)
        if not sha_map:
            print(f"  run {run_id}: SKIP — no inner SHAs in manifest {landing_path}")
            continue
        print(f"  run {run_id}: {sha_map}")
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    sa.update(extraction_runs)
                    .where(extraction_runs.c.id == run_id)
                    .values(response_inner_content_sha256_by_archive=sha_map)
                )
        updated += 1
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill NHTSA by-archive inner SHAs.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Report what would change; make no writes."
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    engine = make_engine(settings.neon_database_url.get_secret_value())
    r2_client = R2LandingClient(settings)

    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print(f"NHTSA inner-SHA-by-archive backfill [{mode}]")
    updated = backfill(engine, r2_client, dry_run=args.dry_run)
    verb = "would update" if args.dry_run else "updated"
    print(f"Done — {verb} {updated} run(s).")


if __name__ == "__main__":
    main()
