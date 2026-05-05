"""NHTSA flat-file recall extractor (Phase 5c Step 2).

Architecture per ``documentation/nhtsa/flat_file_observations.md``
(Option A — TSV-only):

- **Incremental path** (``NhtsaExtractor``): downloads
  ``FLAT_RCL_POST_2010.zip`` (~14 MB compressed, ~240,126 records).
  Daily-cron-friendly; ADR 0007 content-hash dedup absorbs no-op days.
- **Historical / deep-rescan path** (``NhtsaDeepRescanLoader``):
  downloads BOTH ``FLAT_RCL_PRE_2010.zip`` and ``FLAT_RCL_POST_2010.zip``
  (~322k total rows, dating back to 1966-01-19 by RCDATE). No count
  guard, no watermark advance. Used for one-time historical seeding
  (``--change-type=historical_seed``) and weekly defense-in-depth.

Watermark surfaces (``Last-Modified``, ``x-amz-version-id``) are
disqualified per Findings B and C — re-stamped daily regardless of
content. The extractor uses a plain GET every run and relies on bronze
content-hash dedup. ZIP wrapper bytes are non-deterministic across
re-archives (Finding J), so the dedup oracle is the SHA-256 of the
**decompressed inner TSV content**, captured to
``extraction_runs.response_inner_content_sha256`` (migration 0011).
Day-over-day diffs on that column close Finding H Q1 (cadence) over
~7 days as a free side-effect of production runs.

Identity: ``source_recall_id`` maps to TSV field 1 (RECORD_ID),
NHTSA's stable per-row natural key per RCL.txt. CAMPNO (the public
recall ID) is captured in its own indexed bronze column for
analytical grouping but is not unique per row — one campaign produces
multiple TSV rows, one per affected make × model × year.

Quarantine routing: field-count drift (a row with !=29 fields per
Finding F's history of right-edge column additions) is caught per row
in ``validate_records`` via a marker dict from ``extract``; the row is
routed to ``nhtsa_recalls_rejected`` with ``failure_stage="extract"``
without abandoning the rest of the file. Pydantic ``ValidationError``
on a parseable-but-malformed row routes to the same table with
``failure_stage="validate_records"``.

NHTSA serves directly from S3 with no public CDN cache layer
(Finding G), so the bot-management workarounds that USDA and FDA need
do not apply here. Plain ``httpx.Client`` defaults are sufficient.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

import sqlalchemy as sa
import structlog
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.invariants import check_date_sanity, check_null_source_id
from src.bronze.loader import BronzeLoader
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    ExtractionResult,
    QuarantineRecord,
    TransientExtractionError,
)
from src.extractors._flat_file import FlatFileExtractor, FlatFileFieldCountError
from src.landing.r2 import R2LandingClient
from src.schemas.nhtsa import NhtsaRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_nhtsa_bronze = sa.Table(
    "nhtsa_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("campno", sa.Text),
    sa.Column("maketxt", sa.Text),
    sa.Column("modeltxt", sa.Text),
    sa.Column("yeartxt", sa.Text),
    sa.Column("mfgcampno", sa.Text),
    sa.Column("compname", sa.Text),
    sa.Column("mfgname", sa.Text),
    sa.Column("bgman", sa.TIMESTAMP(timezone=True)),
    sa.Column("endman", sa.TIMESTAMP(timezone=True)),
    sa.Column("rcltype", sa.Text),
    sa.Column("potaff", sa.Text),
    sa.Column("odate", sa.TIMESTAMP(timezone=True)),
    sa.Column("influenced_by", sa.Text),
    sa.Column("mfgtxt", sa.Text),
    sa.Column("rcdate", sa.TIMESTAMP(timezone=True)),
    sa.Column("datea", sa.TIMESTAMP(timezone=True)),
    sa.Column("rpno", sa.Text),
    sa.Column("fmvss", sa.String(length=3)),
    sa.Column("desc_defect", sa.Text),
    sa.Column("conequence_defect", sa.Text),
    sa.Column("corrective_action", sa.Text),
    sa.Column("notes", sa.Text),
    sa.Column("rcl_cmpt_id", sa.Text),
    sa.Column("mfr_comp_name", sa.Text),
    sa.Column("mfr_comp_desc", sa.Text),
    sa.Column("mfr_comp_ptno", sa.Text),
    sa.Column("do_not_drive", sa.Boolean),
    sa.Column("park_outside", sa.Boolean),
)

_nhtsa_rejected = sa.Table(
    "nhtsa_recalls_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

_source_watermarks = sa.Table(
    "source_watermarks",
    _metadata,
    sa.Column("source", sa.Text, primary_key=True),
    sa.Column("last_cursor", sa.Text),
    sa.Column("last_etag", sa.Text),
    sa.Column("last_successful_extract_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),
)

_extraction_runs = sa.Table(
    "extraction_runs",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source", sa.Text),
    sa.Column("started_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("finished_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("status", sa.Text),
    sa.Column("records_extracted", sa.Integer),
    sa.Column("records_inserted", sa.Integer),
    sa.Column("records_rejected", sa.Integer),
    sa.Column("run_id", sa.Text),
    sa.Column("error_message", sa.Text),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("change_type", sa.Text),
    sa.Column("response_status_code", sa.Integer),
    sa.Column("response_etag", sa.Text),
    sa.Column("response_last_modified", sa.Text),
    sa.Column("response_body_sha256", sa.Text),
    sa.Column("response_headers", postgresql.JSONB),
    sa.Column("response_inner_content_sha256", sa.Text),
)

_NHTSA_SOURCE = "nhtsa"

_BASE_URL = "https://static.nhtsa.gov/odi/ffdd/rcl"
_INCREMENTAL_URL = f"{_BASE_URL}/FLAT_RCL_POST_2010.zip"
_HISTORICAL_PRE_2010_URL = f"{_BASE_URL}/FLAT_RCL_PRE_2010.zip"

# Sanity guard for the incremental path. Current corpus is ~240k POST_2010
# records (Finding H); 500k headroom catches a runaway upstream change
# (e.g., NHTSA merging in the PRE_2010 archive) without firing on
# multi-year organic growth. Not applied to the deep-rescan path.
_MAX_INCREMENTAL_RECORDS = 500_000

_EXPECTED_FIELDS = 29

# Lowercase RCL.txt field names in TSV column order. Field 1 (RECORD_ID)
# becomes the schema's `source_recall_id` via validation_alias; the
# remaining 28 names match the bronze column names directly.
_FIELD_NAMES: tuple[str, ...] = (
    "record_id",  # field 1 → source_recall_id (alias)
    "campno",  # field 2
    "maketxt",  # field 3
    "modeltxt",  # field 4
    "yeartxt",  # field 5
    "mfgcampno",  # field 6
    "compname",  # field 7
    "mfgname",  # field 8
    "bgman",  # field 9
    "endman",  # field 10
    "rcltype",  # field 11
    "potaff",  # field 12
    "odate",  # field 13
    "influenced_by",  # field 14
    "mfgtxt",  # field 15
    "rcdate",  # field 16
    "datea",  # field 17
    "rpno",  # field 18
    "fmvss",  # field 19
    "desc_defect",  # field 20
    "conequence_defect",  # field 21
    "corrective_action",  # field 22
    "notes",  # field 23
    "rcl_cmpt_id",  # field 24
    "mfr_comp_name",  # field 25
    "mfr_comp_desc",  # field 26
    "mfr_comp_ptno",  # field 27
    "do_not_drive",  # field 28
    "park_outside",  # field 29
)
assert len(_FIELD_NAMES) == _EXPECTED_FIELDS  # noqa: S101 — module-load invariant

# Marker keys for drift rows passed from extract() → validate_records().
# Drift rows are rows whose field count differs from _EXPECTED_FIELDS;
# they survive extract() but bypass NhtsaRecord instantiation in
# validate_records(), routing directly to the rejected table with
# failure_stage="extract".
_DRIFT_FAILURE_KEY = "_drift_failure_reason"
_DRIFT_RAW_LINE_KEY = "_drift_raw_line"


class NhtsaExtractor(FlatFileExtractor[NhtsaRecord]):
    """Extractor for the NHTSA flat-file recall corpus — incremental path.

    Strategy: full-dump every run from
    ``FLAT_RCL_POST_2010.zip``. Watermark surfaces are disqualified
    per Findings B and C; bronze content-hash dedup (ADR 0007)
    handles idempotency. Inner-content SHA-256 captured to
    ``extraction_runs.response_inner_content_sha256`` per Finding J
    drives the "did the data change?" oracle.

    For historical loads / forced re-ingestion use
    ``NhtsaDeepRescanLoader``, which pulls both PRE_2010 and POST_2010
    archives and never updates the watermark — see its docstring.
    """

    source_name: str = _NHTSA_SOURCE
    file_url: str = _INCREMENTAL_URL
    settings: Settings

    # Development-mode RCDATE filter. When set, rows whose RCDATE is
    # earlier than `since` (or whose RCDATE is empty) are dropped from
    # the bronze write. Intended for free-tier-aware dev workflows on
    # the Neon dev branch — the production historical seed always uses
    # `NhtsaDeepRescanLoader` which has no `since` filter and lands the
    # full corpus. Slight ADR 0027 deviation: bronze is normally raw,
    # but a `since`-filtered run stores only a date-bounded slice. The
    # `change_type` flag on `extraction_runs` records why.
    since: date | None = None

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")
    # Wrapper bytes stashed during extract() for landing in land_raw().
    # Per ADR 0007 the bronze "raw" is what NHTSA served — the wrapper
    # ZIP — not the decompressed TSV. Phase 6 re-ingest decompresses
    # on demand.
    _wrapper_bytes: bytes = PrivateAttr(default=b"")

    def model_post_init(self, __context: Any) -> None:
        self._engine = sa.create_engine(
            self.settings.neon_database_url.get_secret_value(),
            pool_pre_ping=True,
        )
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """Download, decompress, and parse the POST_2010 TSV.

        Returns a list of dicts keyed by RCL.txt field names. Drift
        rows (field count != 29) survive as marker dicts for
        ``validate_records`` to route to quarantine — see
        ``_DRIFT_FAILURE_KEY``. If ``self.since`` is set, rows with
        RCDATE < since (or empty RCDATE) are dropped from the result.
        """
        wrapper_path, response, wrapper_bytes = self._download_to_temp(self.file_url)
        try:
            inner_bytes, _inner_name = self._decompress_zip(wrapper_path, "*.txt")
        finally:
            wrapper_path.unlink(missing_ok=True)

        self._capture_flatfile_response(response, wrapper_bytes, inner_bytes)
        self._wrapper_bytes = wrapper_bytes

        # YYYYMMDD strings sort identically to their parsed-date order,
        # so we can apply the `since` filter via a cheap string compare
        # without parsing every row's RCDATE upfront.
        since_str = self.since.strftime("%Y%m%d") if self.since is not None else None
        # RCDATE is field 16 (1-indexed) → array index 15.
        rcdate_idx = _FIELD_NAMES.index("rcdate")

        records: list[dict[str, Any]] = []
        for row_index, raw_line, fields in self._iter_tab_delimited(inner_bytes):
            if len(fields) != _EXPECTED_FIELDS:
                err = FlatFileFieldCountError(
                    row_index=row_index,
                    expected=_EXPECTED_FIELDS,
                    observed=len(fields),
                )
                records.append(
                    {
                        _DRIFT_FAILURE_KEY: str(err),
                        _DRIFT_RAW_LINE_KEY: raw_line,
                    }
                )
                continue
            if since_str is not None:
                row_rcdate = fields[rcdate_idx]
                if not row_rcdate or row_rcdate < since_str:
                    continue
            records.append(dict(zip(_FIELD_NAMES, fields, strict=True)))

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"NHTSA incremental query returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. Possible "
                "causes: upstream merger of PRE_2010 archive into POST_2010, "
                "or a multi-decade organic-growth event that warrants raising "
                "the ceiling."
            )

        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """Write the wrapper ZIP bytes to R2 (raw per ADR 0007).

        The bronze "raw" is what NHTSA served — the wrapper ZIP — not
        the decompressed TSV. Phase 6 re-ingest decompresses on demand.
        ``raw_records`` is unused (the wrapper bytes are stashed in
        ``_wrapper_bytes`` during ``extract()`` to avoid re-downloading
        on retry of this step).
        """
        path = self._r2_client.land(
            source=_NHTSA_SOURCE,
            content=self._wrapper_bytes,
            suffix="zip",
        )
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[NhtsaRecord], list[QuarantineRecord]]:
        """Instantiate ``NhtsaRecord`` per row; route drift + Pydantic errors to quarantine.

        Three cases per record:
          1. Drift marker (field-count mismatch raised in ``extract``) →
             quarantine with ``failure_stage="extract"``.
          2. Pydantic ``ValidationError`` (parseable row, malformed
             value) → quarantine with ``failure_stage="validate_records"``.
          3. Successful instantiation → valid list.
        """
        valid: list[NhtsaRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            if _DRIFT_FAILURE_KEY in record:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=None,
                        raw_record=record,
                        failure_reason=record[_DRIFT_FAILURE_KEY],
                        failure_stage="extract",
                        raw_landing_path=self._current_landing_path,
                    )
                )
                continue
            try:
                valid.append(NhtsaRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        # The TSV's RECORD_ID lives under the dict key
                        # `record_id` (validation_alias); fall back to None
                        # if a malformed row lacks it.
                        source_recall_id=record.get("record_id") or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[NhtsaRecord]
    ) -> tuple[list[NhtsaRecord], list[QuarantineRecord]]:
        """Apply null-id and date-sanity invariants. No NHTSA-specific invariant."""
        passing: list[NhtsaRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = check_null_source_id(record.source_recall_id) or check_date_sanity(
                record.rcdate, "rcdate"
            )
            if failure:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=record.source_recall_id,
                        raw_record=record.model_dump(mode="json"),
                        failure_reason=failure,
                        failure_stage="invariants",
                        raw_landing_path=self._current_landing_path,
                    )
                )
            else:
                passing.append(record)
        return passing, quarantined

    def load_bronze(
        self,
        records: list[NhtsaRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Write valid records to bronze; route quarantine to rejected table."""
        loader = BronzeLoader(
            bronze_table=_nhtsa_bronze,
            rejected_table=_nhtsa_rejected,
            # source_recall_id holds RECORD_ID (TSV field 1, NHTSA's
            # stable per-row natural key per RCL.txt). No composite
            # identity is needed — RECORD_ID is unique across the corpus.
            identity_fields=("source_recall_id",),
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._touch_freshness(conn)
        return count

    # --- Private helpers ---

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump ``last_successful_extract_at`` so monitoring sees the run as fresh.

        NHTSA has no usable cursor and no usable ETag (Findings B + C);
        the watermark row exists only to track freshness for the
        incremental path. Deep-rescan does not touch this field — the
        incremental extractor owns it exclusively.
        """
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _NHTSA_SOURCE)
            .values(
                last_successful_extract_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )

    def _record_run(
        self,
        run_id: str,
        started_at: datetime,
        status: str,
        result: ExtractionResult | None = None,
        error_message: str | None = None,
        change_type: str = "routine",
    ) -> None:
        """Persist a row to ``extraction_runs`` with all forensic columns.

        Mirrors ``UsdaEstablishmentExtractor._record_run`` plus the new
        ``response_inner_content_sha256`` column from migration 0011.
        Diagnostic logging on failure (``error_type`` + ``message``) so
        constraint violations like a missing FK row in
        ``source_watermarks`` are diagnosable from logs (per
        implementation_plan §449-459).
        """
        row: dict[str, Any] = {
            "source": _NHTSA_SOURCE,
            "started_at": started_at,
            "finished_at": datetime.now(UTC),
            "status": status,
            "run_id": run_id,
            "error_message": error_message,
            "change_type": change_type,
        }
        if result is not None:
            row["records_extracted"] = result.records_fetched
            row["records_inserted"] = result.records_loaded
            row["records_rejected"] = (
                result.records_rejected_validate + result.records_rejected_invariants
            )
            row["raw_landing_path"] = result.raw_landing_path
        if self._captured_response_status_code is not None:
            row["response_status_code"] = self._captured_response_status_code
            row["response_etag"] = self._captured_response_etag
            row["response_last_modified"] = self._captured_response_last_modified
            row["response_body_sha256"] = self._captured_response_body_sha256
            row["response_headers"] = self._captured_response_headers
            row["response_inner_content_sha256"] = self._captured_response_inner_content_sha256
        try:
            with self._engine.begin() as conn:
                conn.execute(_extraction_runs.insert().values(**row))
        except Exception as exc:
            # Run-recording is best-effort; a failure here doesn't lose
            # bronze data. Capture exception type + message so a
            # constraint violation surfaces in logs.
            logger.warning(
                "extraction_run.record_failed",
                run_id=run_id,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )


class NhtsaDeepRescanLoader(NhtsaExtractor):
    """Historical / deep-rescan loader for NHTSA flat-file records.

    Pulls BOTH ``FLAT_RCL_PRE_2010.zip`` AND ``FLAT_RCL_POST_2010.zip``
    on every run (~322k total rows, dating back to 1966-01-19). Two
    behaviors differ from ``NhtsaExtractor``:

    1. **No count guard.** The historical pull is structurally large
       (~322k > ``_MAX_INCREMENTAL_RECORDS``), so the guard would fire
       immediately. Removed.
    2. **Never updates ``source_watermarks``.** The incremental
       extractor owns the watermark exclusively. Deep rescan is purely
       additive to the bronze table — content-hash dedup absorbs
       duplicate rows naturally.

    R2 landing produces three objects per run: each ZIP wrapper plus
    a small JSON manifest pointing at both. The manifest's path is
    used as ``raw_landing_path`` for every bronze row in the run; the
    manifest content lets future re-ingest replay either source ZIP.

    Used by the ``deep-rescan-nhtsa.yml`` GitHub Actions workflow
    (Phase 7 weekly cron) and for one-time historical seeding via
    ``recalls deep-rescan nhtsa --change-type=historical_seed``.
    """

    source_name: str = _NHTSA_SOURCE
    # `file_url` is inherited from NhtsaExtractor; the deep-rescan loader
    # downloads two URLs but the parent's `file_url` field is required by
    # the FlatFileExtractor base class. Left at the default; never used.

    # Bytes for the second wrapper (the parent's _wrapper_bytes holds POST_2010).
    _pre_2010_wrapper_bytes: bytes = PrivateAttr(default=b"")
    # Hashes for both inner files — needed for the manifest landed in land_raw.
    _post_2010_inner_sha256: str = PrivateAttr(default="")
    _pre_2010_inner_sha256: str = PrivateAttr(default="")

    def extract(self) -> list[dict[str, Any]]:
        """Download both archives; return concatenated rows.

        The forensic capture (``_captured_response_*``) is populated
        from POST_2010 — the larger and more representative archive.
        Per-archive inner-content hashes are stashed for the manifest
        landed in ``land_raw``; the canonical
        ``response_inner_content_sha256`` is the POST_2010 inner hash
        (so day-over-day diffs on this column track the rolling-current
        archive, matching the incremental path's semantics).
        """
        import hashlib

        # POST_2010 first — populates _captured_response_* and _wrapper_bytes
        # via the parent's machinery.
        post_path, post_response, post_wrapper = self._download_to_temp(_INCREMENTAL_URL)
        try:
            post_inner, _ = self._decompress_zip(post_path, "*.txt")
        finally:
            post_path.unlink(missing_ok=True)
        self._capture_flatfile_response(post_response, post_wrapper, post_inner)
        self._wrapper_bytes = post_wrapper
        self._post_2010_inner_sha256 = self._captured_response_inner_content_sha256 or ""

        # PRE_2010 — captured into private attrs only; does NOT overwrite
        # the parent's _captured_response_* state.
        pre_path, _pre_response, pre_wrapper = self._download_to_temp(_HISTORICAL_PRE_2010_URL)
        try:
            pre_inner, _ = self._decompress_zip(pre_path, "*.txt")
        finally:
            pre_path.unlink(missing_ok=True)
        self._pre_2010_wrapper_bytes = pre_wrapper
        self._pre_2010_inner_sha256 = hashlib.sha256(pre_inner).hexdigest()

        records: list[dict[str, Any]] = []
        for source_inner in (pre_inner, post_inner):
            for row_index, raw_line, fields in self._iter_tab_delimited(source_inner):
                if len(fields) != _EXPECTED_FIELDS:
                    err = FlatFileFieldCountError(
                        row_index=row_index,
                        expected=_EXPECTED_FIELDS,
                        observed=len(fields),
                    )
                    records.append(
                        {
                            _DRIFT_FAILURE_KEY: str(err),
                            _DRIFT_RAW_LINE_KEY: raw_line,
                        }
                    )
                    continue
                records.append(dict(zip(_FIELD_NAMES, fields, strict=True)))

        # No count guard on the deep-rescan path — historical pull is
        # structurally large.
        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """Land both wrapper ZIPs plus a JSON manifest pointing at them.

        The manifest's R2 path becomes ``raw_landing_path`` for every
        bronze row in this run. Re-ingest reads the manifest to find
        either source ZIP.
        """
        post_path = self._r2_client.land(
            source=_NHTSA_SOURCE,
            content=self._wrapper_bytes,
            suffix="zip",
        )
        pre_path = self._r2_client.land(
            source=_NHTSA_SOURCE,
            content=self._pre_2010_wrapper_bytes,
            suffix="zip",
        )
        manifest = {
            "deep_rescan": True,
            "sources": [
                {
                    "url": _HISTORICAL_PRE_2010_URL,
                    "r2_path": pre_path,
                    "inner_content_sha256": self._pre_2010_inner_sha256,
                    "wrapper_bytes": len(self._pre_2010_wrapper_bytes),
                },
                {
                    "url": _INCREMENTAL_URL,
                    "r2_path": post_path,
                    "inner_content_sha256": self._post_2010_inner_sha256,
                    "wrapper_bytes": len(self._wrapper_bytes),
                },
            ],
        }
        manifest_path = self._r2_client.land(
            source=_NHTSA_SOURCE,
            content=json.dumps(manifest, indent=2).encode("utf-8"),
            suffix="json",
        )
        self._current_landing_path = manifest_path
        return manifest_path

    def load_bronze(
        self,
        records: list[NhtsaRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Write to bronze WITHOUT advancing the watermark.

        The incremental extractor owns ``source_watermarks.last_successful_extract_at``
        exclusively; deep-rescan is purely additive to bronze.
        """
        loader = BronzeLoader(
            bronze_table=_nhtsa_bronze,
            rejected_table=_nhtsa_rejected,
            identity_fields=("source_recall_id",),
        )
        with self._engine.begin() as conn:
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
