"""Source-agnostic recovery of quarantined-but-valid bronze records.

Records that fail ``check_invariants()`` are routed to ``<source>_rejected`` (ADR 0013)
with their full ``model_dump(mode="json")`` payload in ``raw_record``. Usually that is
correct — the record really is malformed. But some invariant failures are *false
positives*: e.g. the 2026-06-01 FDA seed quarantined 24 genuine recalls whose source
``recall_initiation_dt`` carried a dropped-century typo (``2013 -> 0013``), tripping the
shared ``check_date_sanity`` invariant. This module recovers such records by reading the
rejected table, reconstructing the Pydantic model from the stored payload, and pushing it
through the SAME ``BronzeLoader.load`` the extractor uses — no API re-fetch, no watermark
mutation (``BronzeLoader`` never touches ``source_watermarks``), content-hash idempotent.

It generalizes the FDA one-off (``scripts/fda/recover_rejected_invariant_records.py``) into
a source-parameterized core driven by ``recalls recover-rejected <source>``.

Design (validated 2026-06-01, workflow ``wp27oa1gt`` — see the plan §0):
- **Explicit ``RECOVERY_CONFIG_BY_SOURCE_NAME`` map, NOT a ``make_bronze_loader`` refactor.**
  Reading a loader config off an extractor would require instantiating it, which fires
  ``model_post_init`` (``create_engine`` + ``R2LandingClient`` — heavy, env-requiring side
  effects). And NHTSA cannot be represented by one accessor (its incremental and deep-rescan
  loaders differ fundamentally). So each source's ``BronzeLoader`` args are copied **verbatim**
  here, keyed by source name. The module-level ``_<source>_bronze`` / ``_<source>_rejected``
  Table objects import with zero instantiation.
  NOTE: this duplicates each extractor's loader args (the accepted MVP trade-off). If a
  source's identity/hash config changes, update the matching entry here. Citations point at
  the canonical site so the two stay in sync by inspection.
- **Scope: the 5 sources that call ``check_date_sanity``** (fda, cpsc, usda recall, nhtsa,
  uscg recall). The three non-recall sources (uscg manufacturers/details, usda establishments)
  only call ``check_null_source_id`` and cannot produce the date-typo class — they are out of
  scope (an unlisted source → "not implemented" error in the CLI).

Recovery is a deliberate, human-in-the-loop operation (census first, narrow predicate,
``--dry-run``, non-destructive — rejected rows are left as an audit record). It does NOT
change the invariant. It is complementary to the planned ADR 0014 re-ingest (which re-runs
``check_invariants`` and would re-reject these rows).
"""

from __future__ import annotations

import types
import typing
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import sqlalchemy as sa

from src.bronze.loader import BronzeLoader

# Module-level Table objects + record models — all importable without instantiating an
# extractor (no model_post_init, no engine/R2 side effects).
from src.extractors.cpsc import _cpsc_bronze, _cpsc_rejected
from src.extractors.fda import _fda_bronze, _fda_rejected
from src.extractors.nhtsa import _nhtsa_bronze, _nhtsa_rejected
from src.extractors.uscg import _uscg_bronze, _uscg_rejected
from src.extractors.usda import _usda_bronze, _usda_rejected
from src.schemas.cpsc import CpscRecord
from src.schemas.fda import FdaRecord
from src.schemas.nhtsa import NhtsaRecord
from src.schemas.uscg import UscgRecallRecord
from src.schemas.usda import UsdaFsisRecord

if typing.TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import BaseModel
    from sqlalchemy import Connection, Engine

# The confirmed-recoverable failure class: the shared check_date_sanity >70yr branch
# (src/bronze/invariants.py). The dropped-century source typo that motivated this tool.
_PAST_DATE_SANITY_SUBSTR = "more than 70 years in the past"


# --------------------------------------------------------------------------------------
# Pure logic (unit-tested)
# --------------------------------------------------------------------------------------


def datetime_field_names(model: type[BaseModel]) -> frozenset[str]:
    """Names of ``datetime`` / ``datetime | None`` fields on a bronze model.

    Pydantic 2 stores the unwrapped annotation, so an ``Annotated[datetime | None, ...]``
    field presents as ``datetime | None`` — a ``types.UnionType`` (the ``X | Y`` form), NOT
    ``typing.Union``. Both union kinds MUST be checked; a ``typing.Union``-only check returns
    the empty set for every date field on every source (verified 2026-06-01).
    """
    names: set[str] = set()
    for name, field in model.model_fields.items():
        annotation = field.annotation
        if annotation is datetime:
            names.add(name)
            continue
        origin = typing.get_origin(annotation)
        if origin is typing.Union or origin is types.UnionType:
            non_none = [arg for arg in typing.get_args(annotation) if arg is not type(None)]
            if len(non_none) == 1 and non_none[0] is datetime:
                names.add(name)
    return frozenset(names)


def coerce_dumped_datetimes(
    raw_record: dict[str, Any], date_fields: frozenset[str]
) -> dict[str, Any]:
    """ISO date strings (``model_dump(mode="json")`` output) → ``datetime`` for ``date_fields``.

    Every source's date ``BeforeValidator`` parses its native wire format (MM/DD/YYYY,
    YYYYMMDD, …) and raises on ISO strings, but all of them pass a ``datetime`` straight
    through. So converting the dumped ISO strings back to ``datetime`` first lets re-validation
    succeed uniformly. None / '' / non-date values are left untouched; input is not mutated.
    """
    coerced = dict(raw_record)
    for field in date_fields:
        value = coerced.get(field)
        if isinstance(value, str) and value:
            coerced[field] = datetime.fromisoformat(value)
    return coerced


def reconstruct(model: type[BaseModel], raw_record: dict[str, Any]) -> BaseModel:
    """Rebuild a validated bronze record from its stored ``model_dump(mode="json")`` payload.

    Round-trips back to the same ``model_dump`` (so ``BronzeLoader`` computes an identical
    content_hash) and re-runs full validation as a safety net before any write.
    """
    return model.model_validate(coerce_dumped_datetimes(raw_record, datetime_field_names(model)))


def recoverable_past_date_sanity(failure_stage: str | None, failure_reason: str | None) -> bool:
    """Default predicate: the confirmed >70-years date-sanity invariant class.

    Gates on BOTH ``failure_stage == 'invariants'`` AND the reason substring — a reason-only
    check would also match a validate-stage Pydantic error whose message happened to contain
    the substring.
    """
    return (
        failure_stage == "invariants"
        and failure_reason is not None
        and _PAST_DATE_SANITY_SUBSTR in failure_reason
    )


def reason_contains(substr: str) -> Callable[[str | None, str | None], bool]:
    """Build a predicate matching invariant-stage rejections whose reason contains ``substr``.

    Still gates on ``failure_stage == 'invariants'`` so a broadened ``--reason-contains`` can
    never pull in schema (validate) rejections.
    """

    def predicate(failure_stage: str | None, failure_reason: str | None) -> bool:
        return (
            failure_stage == "invariants"
            and failure_reason is not None
            and substr in failure_reason
        )

    return predicate


# --------------------------------------------------------------------------------------
# Per-source config (explicit map — the loader args are copied verbatim from each extractor)
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RecoveryConfig:
    """Everything recovery needs for one source, with zero extractor instantiation."""

    record_model: type[BaseModel]
    bronze_table: sa.Table
    rejected_table: sa.Table
    loader: BronzeLoader


@dataclass(frozen=True)
class RecoveryResult:
    """Outcome of a recovery run for one source/landing-path."""

    source: str
    landing_path: str | None
    candidates: int
    inserted: int
    dry_run: bool = False


# Loader args MUST match each source's INCREMENTAL load_bronze exactly (that is the canonical
# dedup contract; content_hash depends on hash_exclude_fields). NHTSA uses its incremental
# 11-tuple config, NOT the deep-rescan single-column loader (the latter is a latent bug).
RECOVERY_CONFIG_BY_SOURCE_NAME: dict[str, RecoveryConfig] = {
    "fda": RecoveryConfig(
        record_model=FdaRecord,
        bronze_table=_fda_bronze,
        rejected_table=_fda_rejected,
        # fda.py:304-308 — RID excluded (query-position counter, finding F).
        loader=BronzeLoader(
            bronze_table=_fda_bronze,
            rejected_table=_fda_rejected,
            hash_exclude_fields=frozenset({"rid"}),
        ),
    ),
    "cpsc": RecoveryConfig(
        record_model=CpscRecord,
        bronze_table=_cpsc_bronze,
        rejected_table=_cpsc_rejected,
        # cpsc.py:244 — defaults only.
        loader=BronzeLoader(bronze_table=_cpsc_bronze, rejected_table=_cpsc_rejected),
    ),
    "nhtsa": RecoveryConfig(
        record_model=NhtsaRecord,
        bronze_table=_nhtsa_bronze,
        rejected_table=_nhtsa_rejected,
        # nhtsa.py:461-479 — 11-tuple identity; RECORD_ID (source_recall_id) excluded from
        # the hash (regen-unstable, finding K); within-batch dedup + null identity per ADR 0030.
        loader=BronzeLoader(
            bronze_table=_nhtsa_bronze,
            rejected_table=_nhtsa_rejected,
            identity_fields=(
                "campno",
                "maketxt",
                "modeltxt",
                "yeartxt",
                "compname",
                "rcl_cmpt_id",
                "mfr_comp_ptno",
                "mfr_comp_desc",
                "mfr_comp_name",
                "endman",
                "bgman",
            ),
            hash_exclude_fields=frozenset({"source_recall_id"}),
            within_batch_dedup=True,
            allow_null_identity=True,
        ),
    ),
    "usda": RecoveryConfig(
        record_model=UsdaFsisRecord,
        bronze_table=_usda_bronze,
        rejected_table=_usda_rejected,
        # usda.py:309-319 — bilingual composite identity; press-release bodies excluded.
        loader=BronzeLoader(
            bronze_table=_usda_bronze,
            rejected_table=_usda_rejected,
            identity_fields=("source_recall_id", "langcode"),
            hash_exclude_fields=frozenset({"en_press_release", "press_release"}),
        ),
    ),
    "uscg": RecoveryConfig(
        record_model=UscgRecallRecord,
        bronze_table=_uscg_bronze,
        rejected_table=_uscg_rejected,
        # uscg.py:466-471 — detail URL excluded (volatile, not record content).
        loader=BronzeLoader(
            bronze_table=_uscg_bronze,
            rejected_table=_uscg_rejected,
            hash_exclude_fields=frozenset({"details_url"}),
        ),
    ),
}


# --------------------------------------------------------------------------------------
# I/O (orchestration; the pure layer above is unit-tested, this is mock-tested)
# --------------------------------------------------------------------------------------


def latest_rejection_landing_path(conn: Connection, rejected_table: sa.Table) -> str | None:
    """The ``raw_landing_path`` of the most recent rejection in this source's rejected table."""
    row = conn.execute(
        sa.select(rejected_table.c.raw_landing_path)
        .order_by(rejected_table.c.rejected_at.desc())
        .limit(1)
    ).first()
    return row[0] if row is not None else None


def fetch_recoverable_rows(
    conn: Connection,
    rejected_table: sa.Table,
    landing_path: str,
    is_recoverable: Callable[[str | None, str | None], bool],
) -> list[tuple[int, dict[str, Any]]]:
    """Return ``(id, raw_record)`` for rejections at ``landing_path`` matching the predicate.

    Dialect-agnostic JSON access only — ``raw_record`` is ``postgresql.JSONB`` on cpsc/fda but
    ``sa.JSON`` on the others; both deserialize to a dict on read.
    """
    rows = conn.execute(
        sa.select(
            rejected_table.c.id,
            rejected_table.c.raw_record,
            rejected_table.c.failure_stage,
            rejected_table.c.failure_reason,
        ).where(rejected_table.c.raw_landing_path == landing_path)
    ).all()
    return [
        (row.id, row.raw_record)
        for row in rows
        if is_recoverable(row.failure_stage, row.failure_reason)
    ]


def seed_extraction_timestamp(
    conn: Connection, bronze_table: sa.Table, landing_path: str
) -> datetime | None:
    """The bronze load timestamp for this landing file (one atomic load → one ts).

    Reused so recovered rows share the lineage of their siblings from the same landing
    payload. None if no bronze rows carry this path (then BronzeLoader.load defaults to now).
    """
    row = conn.execute(
        sa.select(sa.func.min(bronze_table.c.extraction_timestamp)).where(
            bronze_table.c.raw_landing_path == landing_path
        )
    ).first()
    return row[0] if row is not None and row[0] is not None else None


def recover_quarantined(
    engine: Engine,
    *,
    source: str,
    config: RecoveryConfig,
    is_recoverable: Callable[[str | None, str | None], bool],
    landing_path: str | None = None,
    dry_run: bool = False,
) -> RecoveryResult:
    """Recover quarantined-but-valid records for one source.

    Reads (landing-path discovery, candidate fetch, seed-timestamp lookup) run in a read-only
    ``engine.connect()``; the insert runs in a separate ``engine.begin()`` — mirroring the FDA
    one-off so a large candidate read does not hold an open write transaction. Reconstruction
    happens between the two so a validation failure aborts before any write.
    """
    rejected = config.rejected_table
    with engine.connect() as conn:
        path = landing_path or latest_rejection_landing_path(conn, rejected)
        if path is None:
            return RecoveryResult(source, None, 0, 0, dry_run)
        rows = fetch_recoverable_rows(conn, rejected, path, is_recoverable)
        timestamp = seed_extraction_timestamp(conn, config.bronze_table, path)

    if not rows:
        return RecoveryResult(source, path, 0, 0, dry_run)

    # Reconstruct before opening the write transaction — a validation failure must abort
    # before any DB mutation.
    records = [reconstruct(config.record_model, raw_record) for _, raw_record in rows]

    if dry_run:
        return RecoveryResult(source, path, len(records), 0, dry_run=True)

    with engine.begin() as conn:
        inserted = config.loader.load(conn, records, [], path, extraction_timestamp=timestamp)

    return RecoveryResult(source, path, len(records), inserted, dry_run=False)
