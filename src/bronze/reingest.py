"""R2 replay (re-ingest) — reprocess landed payloads through the current schema.

ADR 0014 / ADR 0028 Mechanism B. When the *source's* response was correct but *our*
processing of it was wrong — a Pydantic normalizer bug, a new bronze field that parses an
already-present raw key, or an ADR 0027 hashing-helper change — re-ingest reprocesses the
raw payloads already in R2 **without contacting the source**. This is the only recovery path
for that class: ``recover-rejected`` (``src/bronze/recovery.py``) only un-quarantines
false-positive invariant rejects, and ``deep-rescan`` re-hits the live source.

Per-payload flow (the middle of the 5-step lifecycle, ``src/extractors/_base.py``), reusing
the source's own ``parse_landed_payload`` / ``land_raw`` / ``validate_records`` /
``check_invariants`` but **not** its ``run()`` or ``load_bronze``:

    raw_bytes  = r2.get_raw(original_raw_landing_path)
    raw_records= ext.parse_landed_payload(raw_bytes)   # json.loads (RestApiExtractor)
    new_key    = ext.land_raw(raw_records)             # fresh uuid key; sets _current_landing_path
    passing,q  = validate_and_check(ext, raw_records)
    inserted   = config.loader.load(conn, passing, q, new_key)   # content-hash dedup -> idempotent
    INSERT extraction_runs(run_id=<new>, change_type=<rebaseline>, raw_landing_path=new_key, …)

**Why not route through ``run()`` / ``load_bronze`` (two would-be silver bugs):**
- ``run()`` → ``_record_run`` writes the USDA presence manifest (``default_track_presence``),
  and a replay of a *historical* payload (``started_at=now()``) would become
  ``usda_latest_run`` and flip ``recall_lifecycle.is_currently_active``/``was_ever_retracted``
  corpus-wide. So re-ingest hand-rolls the ``extraction_runs`` insert and writes **no** manifest.
- the source's ``load_bronze`` advances the freshness watermark (USDA:
  ``_fsis_base._update_watermark_state``), so a replay would mark the live incremental "freshly
  run." So re-ingest writes via the contract-built ``BronzeLoader`` primitive directly — the
  same incremental-mode oracle the recovery path uses.

**Why re-land to a fresh key (never reuse the original):** the new bronze rows must join (via
``raw_landing_path``) to a run whose ``change_type`` is a rebaseline value, or
``recall_event_history`` synthesizes a false edit for every re-coerced record. A fresh uuid key
(R2's ``land`` always mints one) under a ``schema_rebaseline`` run is the history-safe shape —
``recall_event_history`` keeps the rebaseline snapshot in the LAG sequence but excludes it from
edit detection. Reusing the original key would let the ``distinct on (raw_landing_path) ... order
by started_at desc`` runs-CTE reassign the *original* routine snapshot's change_type. Content-hash
dedup means a no-op replay (schema unchanged) inserts zero bronze rows.

**Scope: the 5 JSON REST sources** (cpsc, fda, usda, usda_establishments, fda_press_releases) —
their ``land_raw`` writes ``json.dumps(raw_records)``, so ``parse_landed_payload`` is a clean
``json.loads`` round-trip. NHTSA (flat file) and USCG (HTML) are not JSON arrays of records and
are cheaply re-fetchable via ``deep-rescan``; their ``parse_landed_payload`` raises (and they are
absent from ``REINGEST_CONFIG_BY_SOURCE_NAME``), so the CLI rejects them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import sqlalchemy as sa
import structlog

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME
from src.bronze.loader import BronzeLoader

# Module-level Table objects, importable without instantiating an extractor (mirrors
# src/bronze/recovery.py). The 5 JSON REST sources only — NHTSA/USCG are out of scope.
from src.extractors._tables import extraction_runs
from src.extractors.cpsc import _cpsc_bronze, _cpsc_rejected
from src.extractors.fda import _fda_bronze, _fda_rejected
from src.extractors.fda_press_release import _press_releases_bronze, _press_releases_rejected
from src.extractors.usda import _usda_bronze, _usda_rejected
from src.extractors.usda_establishment import _establishments_bronze, _establishments_rejected

if TYPE_CHECKING:
    from datetime import date

    from pydantic import BaseModel
    from sqlalchemy import Connection, Engine

    from src.extractors._base import Extractor, QuarantineRecord
    from src.landing.r2 import R2LandingClient

logger = structlog.get_logger()

# Re-ingest is a re-baseline operation by definition; routine/historical_seed/etag_audit are
# all wrong (they would let recall_event_history treat the re-coerced wave as real edits).
REINGEST_VALID_CHANGE_TYPES = frozenset({"schema_rebaseline", "hash_helper_rebaseline"})


@dataclass(frozen=True)
class ReingestConfig:
    """Per-source bronze + rejected tables and the contract-built loader (zero watermark side
    effects — unlike the source's own ``load_bronze``)."""

    bronze_table: sa.Table
    rejected_table: sa.Table
    loader: BronzeLoader


@dataclass(frozen=True)
class ReingestResult:
    """Outcome of a re-ingest run over a date window for one source."""

    source: str
    from_date: date
    to_date: date
    payloads_found: int
    payloads_replayed: int
    rows_inserted: int
    dry_run: bool = False


# Each loader is built from the source's dedup contract (the SSOT the incremental + deep-rescan
# + recovery paths also consume), in incremental mode — re-ingest replays one historical payload
# exactly as an incremental re-load of it would.
REINGEST_CONFIG_BY_SOURCE_NAME: dict[str, ReingestConfig] = {
    "cpsc": ReingestConfig(
        _cpsc_bronze,
        _cpsc_rejected,
        BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME["cpsc"],
            bronze_table=_cpsc_bronze,
            rejected_table=_cpsc_rejected,
        ),
    ),
    "fda": ReingestConfig(
        _fda_bronze,
        _fda_rejected,
        BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME["fda"],
            bronze_table=_fda_bronze,
            rejected_table=_fda_rejected,
        ),
    ),
    "usda": ReingestConfig(
        _usda_bronze,
        _usda_rejected,
        BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME["usda"],
            bronze_table=_usda_bronze,
            rejected_table=_usda_rejected,
        ),
    ),
    "usda_establishments": ReingestConfig(
        _establishments_bronze,
        _establishments_rejected,
        BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME["usda_establishments"],
            bronze_table=_establishments_bronze,
            rejected_table=_establishments_rejected,
        ),
    ),
    "fda_press_releases": ReingestConfig(
        _press_releases_bronze,
        _press_releases_rejected,
        BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME["fda_press_releases"],
            bronze_table=_press_releases_bronze,
            rejected_table=_press_releases_rejected,
        ),
    ),
}


# --------------------------------------------------------------------------------------
# Pure-ish helpers (the orchestration below is mock-tested; these compose lifecycle calls)
# --------------------------------------------------------------------------------------


def validate_and_check(
    ext: Extractor[Any], raw_records: list[dict[str, Any]]
) -> tuple[list[BaseModel], list[QuarantineRecord]]:
    """Run the source's ``validate_records`` → ``check_invariants`` on replayed records.

    Returns ``(passing_records, all_quarantined)`` — the same split the lifecycle produces, ready
    for ``BronzeLoader.load``. Shared with ``scripts/backfill_manifest.py`` (which uses only the
    passing records).
    """
    valid, schema_rejects = ext.validate_records(raw_records)
    passing, invariant_rejects = ext.check_invariants(valid)
    return passing, [*schema_rejects, *invariant_rejects]


def replay_to_passing(
    ext: Extractor[Any], r2: R2LandingClient, original_landing_path: str
) -> list[BaseModel]:
    """Read a landed payload from R2 and return its passing records (no write, no re-land).

    The reusable ``R2 bytes → passing records`` substrate: ``get_raw`` → ``parse_landed_payload``
    → ``validate_records`` → ``check_invariants``. Used by ``backfill_manifest`` to rebuild a
    historical run's presence roster.
    """
    raw_records = ext.parse_landed_payload(r2.get_raw(original_landing_path))
    passing, _quarantined = validate_and_check(ext, raw_records)
    return passing


def select_candidate_runs(
    conn: Connection, source: str, from_date: date, to_date: date, *, force: bool = False
) -> list[tuple[str, str]]:
    """Successful runs in ``[from_date, to_date]`` with a real landed payload to replay.

    Returns ``(run_id, raw_landing_path)`` ordered by ``started_at``. Excludes failed/aborted runs
    (NULL ``raw_landing_path``), 304 no-op runs (``raw_landing_path = ''``), prior rebaselines (so a
    re-run doesn't replay its own output), and — since the lineage column (migration 0029) — runs
    with NULL ``run_id`` (a replay must record its origin's run_id). Unless ``force``, also skips
    originals already replayed (a rebaseline run with ``replayed_from_run_id = this run_id``), so a
    re-run is a true no-op instead of piling up duplicate rebaseline runs.
    """
    replay = sa.alias(extraction_runs, name="replay")
    already_replayed = (
        sa.select(replay.c.id)
        .where(replay.c.replayed_from_run_id == extraction_runs.c.run_id)
        .exists()
    )
    conditions = [
        extraction_runs.c.source == source,
        extraction_runs.c.status == "success",
        extraction_runs.c.raw_landing_path.is_not(None),
        extraction_runs.c.raw_landing_path != "",
        extraction_runs.c.run_id.is_not(None),
        sa.func.date(extraction_runs.c.started_at).between(from_date, to_date),
        sa.func.coalesce(extraction_runs.c.change_type, "routine").not_in(
            tuple(REINGEST_VALID_CHANGE_TYPES)
        ),
    ]
    if not force:
        conditions.append(~already_replayed)
    rows = conn.execute(
        sa.select(extraction_runs.c.run_id, extraction_runs.c.raw_landing_path)
        .where(*conditions)
        .order_by(extraction_runs.c.started_at)
    ).all()
    return [(row.run_id, row.raw_landing_path) for row in rows]


def _original_extraction_timestamp(
    conn: Connection, bronze_table: sa.Table, landing_path: str
) -> datetime | None:
    """The bronze load timestamp of the ORIGINAL payload (one atomic load → one ts).

    Re-ingest must re-coerce a payload back at its ORIGINAL temporal position, not ``now()``:
    stamping a replayed (older) payload with ``now()`` would make its content the newest version
    for each identity and leapfrog any genuinely-newer data — e.g. a later re-baseline — corrupting
    silver's ``max(extraction_timestamp)`` pick. Mirrors ``recovery.seed_extraction_timestamp``.
    ``None`` (no bronze rows for the original key) → ``BronzeLoader.load`` falls back to now().
    """
    row = conn.execute(
        sa.select(sa.func.min(bronze_table.c.extraction_timestamp)).where(
            bronze_table.c.raw_landing_path == landing_path
        )
    ).first()
    return row[0] if row is not None and row[0] is not None else None


def _record_reingest_run(
    conn: Connection,
    *,
    source: str,
    run_id: str,
    started_at: datetime,
    change_type: str,
    raw_landing_path: str,
    replayed_from_run_id: str,
    fetched: int,
    inserted: int,
    rejected: int,
) -> None:
    """Hand-rolled ``extraction_runs`` insert for one replayed payload.

    Deliberately does NOT write a presence manifest (unlike ``Extractor._record_run``) — a replay
    must never assert presence for a historical roster (it would corrupt ``recall_lifecycle``).
    Records ``replayed_from_run_id`` (the original run's id) so the replay is unambiguously a
    re-ingest, not an extract-path rebaseline (migration 0029).
    """
    conn.execute(
        extraction_runs.insert().values(
            source=source,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            status="success",
            records_extracted=fetched,
            records_inserted=inserted,
            records_rejected=rejected,
            run_id=run_id,
            raw_landing_path=raw_landing_path,
            change_type=change_type,
            replayed_from_run_id=replayed_from_run_id,
        )
    )


def reingest_window(
    engine: Engine,
    r2: R2LandingClient,
    ext: Extractor[Any],
    *,
    source: str,
    config: ReingestConfig,
    from_date: date,
    to_date: date,
    change_type: str,
    dry_run: bool = False,
    force: bool = False,
) -> ReingestResult:
    """Replay every landed payload for ``source`` in ``[from_date, to_date]`` through the current
    schema, marking each replay run ``change_type`` so history excludes the wave.

    Idempotent at the bronze grain (content-hash dedup). With the lineage column (migration 0029)
    a re-run is also idempotent at the run grain: already-replayed originals are skipped unless
    ``force=True`` (which replays them again, minting fresh rebaseline runs).
    """
    with engine.connect() as conn:
        candidates = select_candidate_runs(conn, source, from_date, to_date, force=force)

    if dry_run or not candidates:
        return ReingestResult(source, from_date, to_date, len(candidates), 0, 0, dry_run=dry_run)

    replayed = 0
    total_inserted = 0
    for original_run_id, original_key in candidates:
        raw_records = ext.parse_landed_payload(r2.get_raw(original_key))
        new_key = ext.land_raw(raw_records)  # fresh uuid key; sets _current_landing_path = new_key
        passing, quarantined = validate_and_check(ext, raw_records)
        new_run_id = str(uuid4())
        with engine.begin() as conn:
            # Re-coerce at the payload's ORIGINAL timestamp, never now() — see the helper: a
            # replayed older payload must not leapfrog newer bronze (e.g. a later re-baseline).
            original_ts = _original_extraction_timestamp(conn, config.bronze_table, original_key)
            inserted = config.loader.load(
                conn, passing, quarantined, new_key, extraction_timestamp=original_ts
            )
            _record_reingest_run(
                conn,
                source=source,
                run_id=new_run_id,
                started_at=datetime.now(UTC),
                change_type=change_type,
                raw_landing_path=new_key,
                replayed_from_run_id=original_run_id,
                fetched=len(raw_records),
                inserted=inserted,
                rejected=len(quarantined),
            )
        replayed += 1
        total_inserted += inserted
        logger.info(
            "reingest.payload.replayed",
            source=source,
            original_landing_path=original_key,
            new_landing_path=new_key,
            change_type=change_type,
            rows_inserted=inserted,
        )

    return ReingestResult(
        source, from_date, to_date, len(candidates), replayed, total_inserted, dry_run=False
    )
