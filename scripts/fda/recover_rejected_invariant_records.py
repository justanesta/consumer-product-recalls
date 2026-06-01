"""Recover FDA recalls quarantined by the >70-years date-sanity invariant — no re-fetch.

Context: the 2026-06-01 full-corpus historical seed quarantined 24 product rows (14
recall events) into ``fda_recalls_rejected``. Investigation
(``scripts/sql/fda/bronze/explore_seed_rejections.sql``) PROVED these are genuine recent
recalls (2007/2012/2013) whose source ``recall_initiation_dt`` carries a dropped-century
typo (``2013 -> 0013``, one transposition ``2012 -> 0212``). The shared date-sanity
invariant (``src/bronze/invariants.py::check_date_sanity``, ``_MAX_RECALL_AGE_DAYS =
70*365``) correctly fired on the insane year, but the consequence is 14 real events
dropped from bronze over one corrupted field. Every one has intact, modern
``center_classification_dt`` / ``termination_dt`` / ``posted_internet_dt`` / ``event_lmd``.

This recovers them WITHOUT re-fetching the ~134k-row corpus: the full, already-validated
payloads live in ``fda_recalls_rejected.raw_record`` (the ``model_dump(mode="json")`` of
each ``FdaRecord``). We reconstruct the records and push them through the SAME
``BronzeLoader`` the extractor uses (content-hash dedup, ``rid`` excluded) — so the
recovered rows are byte-identical to how the seed would have written them.

Deliberate design choices:
- Calls ``BronzeLoader.load()`` DIRECTLY, NOT ``FdaExtractor.load_bronze()``. The latter
  also advances the ``eventlmd`` watermark, and these records' ``event_lmd`` (2018-2020)
  is BELOW the post-seed watermark — routing them through ``load_bronze`` would regress
  the watermark and trigger a needless incremental re-fetch. Recovery must not touch it.
- Reuses the seed's ``raw_landing_path`` + ``extraction_timestamp`` so the 24 share the
  exact lineage of their 134,181 siblings (they came from the same landing payload).
- INSERT-only (non-destructive): the source rows are LEFT in ``fda_recalls_rejected`` as
  an audit record of the original quarantine. Content-hash dedup makes re-running a
  no-op (already-present rows are skipped), so this is safe to run more than once.

This does NOT change the invariant — a future full-corpus rescan would re-quarantine the
same source typo. Whether to move date-sanity off the hard gate (permissive bronze, ADR
0014) is a separate architectural decision deferred to ``feature/silver-field-remap``.

Usage (run yourself; touches the DB — read, then one transactional insert):
    python scripts/fda/recover_rejected_invariant_records.py --dry-run
    python scripts/fda/recover_rejected_invariant_records.py
    python scripts/fda/recover_rejected_invariant_records.py \\
        --landing-path fda/<date>/<uuid>.json.gz
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import sqlalchemy as sa

# scripts/fda/ is on sys.path[0] when run directly; add the repo root so ``src`` resolves
# regardless of install state (mirrors scripts/fda/audit/*).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.bronze.loader import BronzeLoader  # noqa: E402  — after sys.path shim
from src.config.settings import Settings  # noqa: E402
from src.extractors.fda import _fda_bronze, _fda_rejected  # noqa: E402  — canonical Table defs
from src.schemas.fda import FdaRecord  # noqa: E402

# A rejected row is in-scope iff it failed the date-sanity invariant this way. Excludes
# null-source-id invariant failures and any schema (validate_records) rejects.
_RECOVERABLE_REASON_SUBSTR = "more than 70 years in the past"

# FdaRecord date fields. ``model_dump(mode="json")`` serializes these to ISO 8601, but
# FdaRecord's date BeforeValidator (``_parse_fda_date``) only accepts ``MM/DD/YYYY`` or a
# ``datetime`` — it RAISES on ISO strings. So we convert these back to ``datetime`` (which
# the validator passes through) before re-validating. The round-trip test is the guard:
# a fully date-populated record fails to reconstruct if this set omits a field.
_FDA_DATE_FIELDS = frozenset(
    {
        "event_lmd",
        "recall_initiation_dt",
        "center_classification_dt",
        "termination_dt",
        "enforcement_report_dt",
        "determination_dt",
        "posted_internet_dt",
    }
)


# --------------------------------------------------------------------------------------
# Pure logic (unit-tested in tests/scripts/test_recover_rejected_invariant_records.py)
# --------------------------------------------------------------------------------------


def is_recoverable_invariant_rejection(
    failure_stage: str | None, failure_reason: str | None
) -> bool:
    """True iff a rejected row is a >70-years date-sanity invariant failure.

    This is the confirmed source year-typo class (see explore_seed_rejections.sql).
    Anything else — null-source-id invariant failures, schema validate rejects — is
    deliberately excluded so recovery touches only the rows we have characterized.
    """
    return (
        failure_stage == "invariants"
        and failure_reason is not None
        and _RECOVERABLE_REASON_SUBSTR in failure_reason
    )


def coerce_dumped_dates(raw_record: dict[str, Any]) -> dict[str, Any]:
    """Convert ``model_dump(mode="json")`` ISO date strings back to ``datetime`` objects.

    FdaRecord's date validator accepts ``datetime`` (passes it through) but raises on ISO
    strings, so a naive ``model_validate`` of the dumped payload would fail. None/empty
    values and non-date fields are left untouched. Returns a new dict (input unmutated).
    """
    coerced = dict(raw_record)
    for field in _FDA_DATE_FIELDS:
        value = coerced.get(field)
        if isinstance(value, str) and value:
            coerced[field] = datetime.fromisoformat(value)
    return coerced


def reconstruct_record(raw_record: dict[str, Any]) -> FdaRecord:
    """Rebuild a validated FdaRecord from its stored ``model_dump(mode="json")`` payload.

    Round-trips back to the original ``model_dump`` (so the loader computes an identical
    content_hash), and re-runs full validation as a safety net before any DB write.
    """
    return FdaRecord.model_validate(coerce_dumped_dates(raw_record))


# --------------------------------------------------------------------------------------
# I/O (side effects — not unit-tested; the pure layer above is)
# --------------------------------------------------------------------------------------


def build_engine(settings: Settings) -> sa.Engine:
    return sa.create_engine(
        settings.neon_database_url.get_secret_value(),
        pool_pre_ping=True,
    )


def latest_rejection_landing_path(conn: sa.Connection) -> str | None:
    """The ``raw_landing_path`` of the most recent rejection — the run we scope to."""
    row = conn.execute(
        sa.select(_fda_rejected.c.raw_landing_path)
        .order_by(_fda_rejected.c.rejected_at.desc())
        .limit(1)
    ).first()
    return row[0] if row is not None else None


def fetch_recoverable_rows(
    conn: sa.Connection, landing_path: str
) -> list[tuple[int, dict[str, Any]]]:
    """Return ``(id, raw_record)`` for every recoverable rejection at ``landing_path``."""
    rows = conn.execute(
        sa.select(
            _fda_rejected.c.id,
            _fda_rejected.c.raw_record,
            _fda_rejected.c.failure_stage,
            _fda_rejected.c.failure_reason,
        ).where(_fda_rejected.c.raw_landing_path == landing_path)
    ).all()
    return [
        (row.id, row.raw_record)
        for row in rows
        if is_recoverable_invariant_rejection(row.failure_stage, row.failure_reason)
    ]


def seed_extraction_timestamp(conn: sa.Connection, landing_path: str) -> datetime | None:
    """The seed's bronze load timestamp for this landing file (one atomic load → one ts).

    Reused so recovered rows share their 134k siblings' lineage. None if no bronze rows
    carry this path (then BronzeLoader.load defaults to now(UTC)).
    """
    row = conn.execute(
        sa.select(sa.func.min(_fda_bronze.c.extraction_timestamp)).where(
            _fda_bronze.c.raw_landing_path == landing_path
        )
    ).first()
    return row[0] if row is not None and row[0] is not None else None


def _print_plan(records: list[FdaRecord], rows: list[tuple[int, dict[str, Any]]]) -> None:
    for (rejected_id, _), record in zip(rows, records, strict=True):
        init = record.recall_initiation_dt.isoformat() if record.recall_initiation_dt else None
        print(
            f"  id={rejected_id} product={record.source_recall_id} "
            f"event={record.recall_event_id} init={init} firm={record.firm_legal_nam!r}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--landing-path",
        default=None,
        help="raw_landing_path to scope to (default: the most recent rejection's path).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Reconstruct + print the recovery plan, then exit without writing.",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]  # reads from env vars
    engine = build_engine(settings)

    # --- Read phase (no writes) ---
    with engine.connect() as conn:
        landing_path = args.landing_path or latest_rejection_landing_path(conn)
        if landing_path is None:
            print("fda_recalls_rejected is empty — nothing to recover.")
            return 0
        rows = fetch_recoverable_rows(conn, landing_path)
        timestamp = seed_extraction_timestamp(conn, landing_path)

    if not rows:
        print(f"No recoverable (>70yr date-sanity) rejections at {landing_path}.")
        return 0

    # Reconstruct BEFORE opening a write transaction: a validation failure must abort
    # before any DB mutation.
    records = [reconstruct_record(raw_record) for _, raw_record in rows]
    print(f"Found {len(records)} recoverable record(s) at landing_path={landing_path}:")
    _print_plan(records, rows)

    if args.dry_run:
        print(f"[dry-run] would insert {len(records)} record(s) into fda_recalls_bronze; no write.")
        return 0

    # --- Write phase (one transaction) ---
    loader = BronzeLoader(
        bronze_table=_fda_bronze,
        rejected_table=_fda_rejected,
        hash_exclude_fields=frozenset({"rid"}),  # mirrors FdaExtractor.load_bronze
    )
    with engine.begin() as conn:
        # type: ignore[arg-type] — list invariance (FdaRecord vs BaseModel), as in
        # FdaExtractor.load_bronze; the loader only needs model_dump(), which FdaRecord has.
        inserted = loader.load(conn, records, [], landing_path, extraction_timestamp=timestamp)  # type: ignore[arg-type]

    print(
        f"Recovered {inserted} record(s) into fda_recalls_bronze "
        f"(landing_path={landing_path}, extraction_timestamp={timestamp})."
    )
    if inserted < len(records):
        print(
            f"  ({len(records) - inserted} already present — content-hash dedup skipped "
            f"them; this run is idempotent.)"
        )
    print(
        f"The {len(rows)} source row(s) remain in fda_recalls_rejected as an audit record "
        f"of the original quarantine."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
