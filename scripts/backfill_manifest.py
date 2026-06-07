"""Backfill the USDA presence manifest for historical runs (ADR 0028 Mechanism C).

The presence manifest (``extraction_run_identities``, migration 0027) records, per successful
USDA run, the set of ``field_recall_number`` ids actually returned — so ``recall_lifecycle`` can
derive ``is_currently_active`` / ``was_ever_retracted`` (a retraction produces zero new bronze
rows, indistinguishable from "unchanged", so bronze alone cannot signal it). The live extractor
writes it on every run from Phase 6c forward; this one-shot job reconstructs it for runs that
completed *before* the table existed, by replaying each run's landed R2 payload through the
current schema and stamping the rows with that run's ORIGINAL ``run_id``.

USDA-only by construction — it is the only source that (1) full-dumps the corpus on its daily
cadence, (2) has a stable recall key to roster on, and (3) actually retracts (see
``DedupContract.default_track_presence``). NHTSA is full-dump but era-partial daily + has a
regen-unstable ``source_recall_id``; CPSC/FDA are watermark-partial.

**Census-first.** Default mode is read-only: it prints how many successful USDA runs carry a
usable ``run_id`` (the backfillable universe), how many are NULL-``run_id`` (run_id was nullable
from the baseline → those are permanently un-backfillable: no FK target), and the manifest
**floor** (earliest backfillable run). Pass ``--apply`` to perform the inserts. Idempotent:
candidates exclude runs that already have manifest rows, and the insert is
``ON CONFLICT DO NOTHING`` on ``uq_eri_identity``.

Run (census):  ``python scripts/backfill_manifest.py``
Run (apply):   ``python scripts/backfill_manifest.py --apply``
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.bronze.manifest import build_presence_manifest_rows
from src.bronze.reingest import replay_to_passing
from src.config.db import make_engine
from src.config.logging import configure_logging
from src.config.settings import Settings
from src.config.source_loader import load_source_config
from src.config.source_registry import EXTRACTOR_BY_SOURCE_NAME, build_extractor_kwargs
from src.extractors._tables import extraction_run_identities, extraction_runs
from src.landing.r2 import R2LandingClient

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from typing import Any

    from sqlalchemy import Connection, Engine

    from src.extractors._base import Extractor

logger = structlog.get_logger()

_SOURCE = "usda"
_LANGCODE_FIELD = "langcode"
_ERI_CONSTRAINT = "uq_eri_identity"  # composite UNIQUE from migration 0027


# --------------------------------------------------------------------------------------
# Pure helpers (unit-tested)
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CensusReport:
    """Read-only summary of how much USDA presence history is recoverable."""

    total_payload_runs: int
    with_run_id: int
    null_run_id: int
    floor: datetime | None
    backfillable: int


@dataclass(frozen=True)
class BackfillResult:
    """Outcome of an ``--apply`` run."""

    runs_processed: int
    manifest_rows_submitted: int


def format_census_report(report: CensusReport) -> str:
    """Render the census for the operator (the basis for the census-first go/no-go decision)."""
    floor = report.floor.date().isoformat() if report.floor is not None else "n/a"
    return "\n".join(
        [
            "USDA presence-manifest backfill census (ADR 0028 Mechanism C):",
            f"  successful runs with a landed payload : {report.total_payload_runs}",
            f"  with usable run_id (backfillable set)  : {report.with_run_id}",
            f"  NULL run_id (permanently un-backfillable): {report.null_run_id}",
            f"  manifest floor (earliest recoverable)  : {floor}",
            f"  not-yet-backfilled (--apply will insert): {report.backfillable}",
        ]
    )


# --------------------------------------------------------------------------------------
# I/O (read-only census + the apply path; mock-tested)
# --------------------------------------------------------------------------------------


def _usda_payload_run_conditions() -> list[sa.ColumnElement[bool]]:
    """Where-conditions for 'a successful USDA run with a real landed payload' (shared)."""
    return [
        extraction_runs.c.source == _SOURCE,
        extraction_runs.c.status == "success",
        extraction_runs.c.raw_landing_path.is_not(None),
        extraction_runs.c.raw_landing_path != "",
    ]


def select_backfillable_runs(conn: Connection) -> list[tuple[str, str]]:
    """``(run_id, raw_landing_path)`` for USDA runs with a usable run_id and no manifest rows yet.

    NULL ``run_id`` runs are excluded (no FK target — un-backfillable). The ``NOT EXISTS`` makes a
    re-run process only runs not already backfilled (idempotent at the run grain).
    """
    already_has_manifest = (
        sa.select(extraction_run_identities.c.id)
        .where(extraction_run_identities.c.run_id == extraction_runs.c.run_id)
        .exists()
    )
    rows = conn.execute(
        sa.select(extraction_runs.c.run_id, extraction_runs.c.raw_landing_path)
        .where(
            *_usda_payload_run_conditions(),
            extraction_runs.c.run_id.is_not(None),
            ~already_has_manifest,
        )
        .order_by(extraction_runs.c.started_at)
    ).all()
    return [(row.run_id, row.raw_landing_path) for row in rows]


def census(engine: Engine) -> CensusReport:
    """Read-only: size the backfillable set, the un-backfillable NULL-run_id tail, and the floor."""
    with engine.connect() as conn:
        agg = conn.execute(
            sa.select(
                sa.func.count().label("total"),
                sa.func.count(extraction_runs.c.run_id).label("with_run_id"),
                sa.func.min(extraction_runs.c.started_at)
                .filter(extraction_runs.c.run_id.is_not(None))
                .label("floor"),
            ).where(*_usda_payload_run_conditions())
        ).one()
        backfillable = len(select_backfillable_runs(conn))
    return CensusReport(
        total_payload_runs=agg.total,
        with_run_id=agg.with_run_id,
        null_run_id=agg.total - agg.with_run_id,
        floor=agg.floor,
        backfillable=backfillable,
    )


def _insert_manifest_rows(conn: Connection, rows: Sequence[dict[str, object]]) -> None:
    """Insert presence rows with ``ON CONFLICT DO NOTHING`` on the composite UNIQUE (idempotent)."""
    stmt = pg_insert(extraction_run_identities).on_conflict_do_nothing(constraint=_ERI_CONSTRAINT)
    conn.execute(stmt, list(rows))


def backfill_usda(
    engine: Engine, r2: R2LandingClient, ext: Extractor[Any], *, dry_run: bool = False
) -> BackfillResult:
    """Replay each backfillable USDA run's payload and write its presence roster under the run's
    ORIGINAL run_id. The original run_id (not a fresh one) is load-bearing: it is the FK target and
    what ``recall_lifecycle`` counts enumerating runs by.

    ``dry_run`` replays + counts (reading R2, re-validating under the current schema) but inserts
    nothing — so ``manifest_rows_submitted`` is the *would-be* count, the per-run preview of what
    ``--apply`` will write. A low count vs the run's corpus size flags records the current schema
    now rejects.
    """
    with engine.connect() as conn:
        candidates = select_backfillable_runs(conn)

    processed = 0
    submitted = 0
    for original_run_id, landing_path in candidates:
        passing = replay_to_passing(ext, r2, landing_path)
        rows = build_presence_manifest_rows(
            passing, run_id=original_run_id, source=_SOURCE, langcode_field=_LANGCODE_FIELD
        )
        if rows and not dry_run:
            with engine.begin() as conn:
                _insert_manifest_rows(conn, rows)
        submitted += len(rows)
        processed += 1
        logger.info(
            "backfill_manifest.run",
            run_id=original_run_id,
            manifest_rows=len(rows),
            dry_run=dry_run,
        )
    return BackfillResult(runs_processed=processed, manifest_rows_submitted=submitted)


def _build_usda_extractor(settings: Settings) -> Extractor[Any]:
    config = load_source_config(_SOURCE)
    extractor_cls = EXTRACTOR_BY_SOURCE_NAME[_SOURCE]
    kwargs = build_extractor_kwargs(config, extractor_cls, settings)
    return extractor_cls(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backfill the USDA presence manifest (ADR 0028 Mechanism C). "
        "Census-only by default; --dry-run previews per-run row counts; --apply inserts."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Replay the backfillable runs and report how many manifest rows WOULD be inserted, "
        "without writing (reads R2 + re-validates under the current schema).",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Replay the backfillable runs and INSERT their presence rosters. Default is "
        "census-only (read-only, pure SQL).",
    )
    args = parser.parse_args(argv)

    configure_logging()
    settings = Settings()  # type: ignore[call-arg]  # reads from env vars
    engine = make_engine(settings.neon_database_url.get_secret_value())

    report = census(engine)
    print(format_census_report(report))

    if not args.apply and not args.dry_run:
        print("\n(census only — --dry-run to preview per-run row counts, --apply to insert.)")
        return 0
    if report.backfillable == 0:
        print("\nNothing to backfill.")
        return 0

    r2 = R2LandingClient(settings)
    ext = _build_usda_extractor(settings)
    result = backfill_usda(engine, r2, ext, dry_run=args.dry_run)
    if args.dry_run:
        print(
            f"\n[dry-run] would backfill: runs={result.runs_processed} "
            f"manifest_rows={result.manifest_rows_submitted} — no write performed. "
            "(A count well below the run's corpus size means the current schema rejects old rows.)"
        )
        return 0
    print(
        f"\nBackfilled: runs_processed={result.runs_processed} "
        f"manifest_rows_submitted={result.manifest_rows_submitted}"
    )
    print("(ON CONFLICT DO NOTHING — idempotent; a re-run backfills only newly-eligible runs.)")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
