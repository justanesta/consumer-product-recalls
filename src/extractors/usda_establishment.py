"""USDA FSIS Establishment Listing extractor (Phase 5b.2).

Targets ``GET /fsis/api/establishments/v/1`` — a flat-array endpoint returning
all 7,945 FSIS-regulated establishments in one response. Per Findings A–G in
``documentation/usda/establishment_api_observations.md``:

- **No pagination; no incremental cursor.** The full dataset returns in one
  flat JSON array; idempotency is handled by the bronze content-hash loader
  (ADR 0007). The ETag / Last-Modified conditional-GET path (below) saves the
  download but doesn't replace the bronze hash dedup as the source of truth
  for "did the data change?".
- **ETag conditional-GET, scaffolded but disabled by default.** Finding A
  revision (2026-05-03) confirmed the API emits ``ETag`` and ``Last-Modified``
  under browser fingerprint, mirroring the recall endpoint. This extractor's
  ``_fetch`` / ``_read_etag_state`` / ``_update_watermark_state`` /
  ``_guard_etag_contradiction`` / ``_touch_freshness`` helpers are 1:1 mirrors
  of the corresponding methods on ``UsdaExtractor`` — bug fixes here likely
  apply there and vice versa. Default ``etag_enabled=False`` until viability
  data accumulated in ``extraction_runs.response_*`` (migration 0010) clears
  the gate at ``scripts/sql/_pipeline/etag_viability.sql``.
- **No incremental vs historical split.** The "incremental vs historical load
  paths" architectural standing requirement (implementation_plan.md Phase 5
  preamble) is moot here — there is no cursor to advance and no count guard
  is meaningful at the incremental level. A single ``_MAX_TOTAL_RECORDS``
  guard catches an upstream shape change.
- **No deep-rescan workflow.** Same reason — every run is functionally a
  deep rescan.
- **Akamai Bot Manager** protects the same hostname as the recall API
  (Finding O on the recall side); browser-like headers are reused via
  ``src/extractors/_fsis_headers.browser_headers``.

The motivating downstream use case is enrichment of USDA recall events:
``stg_usda_fsis_recalls.establishment`` joins to ``establishment_name`` (with
``dbas`` array fallback) on normalized name, attaching ``establishment_id``
(stable FSIS FK), address, geolocation, FIPS, and active-MPI status. That
silver join lands in Phase 5b.2 Step 5.
"""

from __future__ import annotations

import json
from typing import Any

import sqlalchemy as sa
import structlog
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME
from src.bronze.invariants import (
    PER_RECORD_INVARIANTS_BY_SOURCE_NAME,
    run_per_record_invariants,
)
from src.bronze.loader import BronzeLoader
from src.extractors._base import (
    QuarantineRecord,
    TransientExtractionError,
)
from src.extractors._fsis_base import FsisConditionalGetExtractor
from src.schemas.usda_establishment import UsdaFsisEstablishment

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_establishments_bronze = sa.Table(
    "usda_fsis_establishments_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("establishment_name", sa.Text),
    sa.Column("establishment_number", sa.Text),
    sa.Column("address", sa.Text),
    sa.Column("city", sa.Text),
    sa.Column("state", sa.Text),
    sa.Column("zip", sa.Text),
    sa.Column("latest_mpi_active_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("status_regulated_est", sa.Text),
    sa.Column("activities", postgresql.JSONB),
    sa.Column("dbas", postgresql.JSONB),
    sa.Column("phone", sa.Text),
    sa.Column("duns_number", sa.Text),
    sa.Column("county", sa.Text),
    sa.Column("fips_code", sa.Text),
    sa.Column("geolocation", sa.Text),
    sa.Column("grant_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("size", sa.Text),
    sa.Column("district", sa.Text),
    sa.Column("circuit", sa.Text),
)

_establishments_rejected = sa.Table(
    "usda_fsis_establishments_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

# ``_source_watermarks`` (imported above) and ``extraction_runs`` are the shared
# cross-source operational tables — see src/extractors/_tables.py. (last_cursor is
# repurposed for this source's prior Last-Modified header value — Finding D.)

_SOURCE = "usda_establishments"

# Sanity guard: current dataset is 7,945 records (Finding B). 20,000 gives
# ~2.5x headroom against organic growth while still catching a runaway
# upstream shape change (e.g., a sibling endpoint silently merging in).
_MAX_TOTAL_RECORDS = 20_000


class UsdaEstablishmentExtractor(FsisConditionalGetExtractor[UsdaFsisEstablishment]):
    """Full-dump extractor for the FSIS Establishment Listing API.

    ETag conditional-GET pattern is scaffolded as a 1:1 mirror of
    ``UsdaExtractor`` (recall side). When ``etag_enabled=True``, the extractor
    reads ``source_watermarks.last_etag`` / ``last_cursor`` (= prior
    Last-Modified), sends ``If-None-Match`` and ``If-Modified-Since``, and
    short-circuits cleanly on 304 (skipping the ~810 KB download and the
    bronze write). A contradiction guard fails the run if a 304 is paired with
    a ``last-modified`` header that has advanced past the prior recorded
    value. Production default ON since 2026-05-09 after
    ``scripts/sql/_pipeline/etag_viability.sql`` cleared the green-light gate.
    """

    source_name: str = _SOURCE
    # settings, etag_enabled (production default ON since 2026-05-09 per establishment
    # Finding A — etag_viability.sql green-lit it), the engine/R2 + ETag-capture state,
    # and model_post_init are inherited from FsisConditionalGetExtractor.
    # UsdaEstablishments has no deep-rescan loader (every run is a full dump).

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """Single GET to the establishments endpoint.

        Returns [] on a 304 Not Modified (and sets _not_modified so downstream
        lifecycle steps no-op). Raises TransientExtractionError on 5xx /
        network / oversized response. Raises ExtractionError (no retry) on
        the contradiction guard (304 paired with advanced last-modified).
        """
        prior_etag, prior_last_modified = self._read_etag_state()
        records, status_code, etag, last_modified = self._fetch(prior_etag, prior_last_modified)

        if status_code == 304:
            self._not_modified = True
            logger.info(
                "usda_establishments.extract.not_modified",
                etag=prior_etag,
                last_modified_header=last_modified,
            )
            self._guard_etag_contradiction(prior_last_modified, last_modified)
            return []

        if len(records) > _MAX_TOTAL_RECORDS:
            raise TransientExtractionError(
                f"USDA establishments query returned {len(records)} records — "
                f"exceeds guard of {_MAX_TOTAL_RECORDS}. "
                "Possible cause: upstream dataset shape change."
            )

        # Stash captured headers for atomic write in load_bronze().
        self._captured_etag = etag
        self._captured_last_modified = last_modified
        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        if self._not_modified:
            # Nothing to land; skip R2 write. Empty path string is a no-op
            # marker consumed by load_bronze() and by quarantine routing
            # (which has no records to route on a 304 path).
            self._current_landing_path = ""
            return ""
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UsdaFsisEstablishment], list[QuarantineRecord]]:
        valid: list[UsdaFsisEstablishment] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UsdaFsisEstablishment.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("establishment_id") or "<unknown>"),
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UsdaFsisEstablishment]
    ) -> tuple[list[UsdaFsisEstablishment], list[QuarantineRecord]]:
        # Only the null-id check applies. No date_sanity: latest_mpi_active_date
        # is administrative, not a publication timestamp; an FSIS dataset
        # re-baseline could legitimately reset it. No bilingual pairing.
        passing: list[UsdaFsisEstablishment] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = run_per_record_invariants(
                record, PER_RECORD_INVARIANTS_BY_SOURCE_NAME[_SOURCE]
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
        records: list[UsdaFsisEstablishment],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        if self._not_modified:
            # 304 path: no records, no quarantine, but we DO advance
            # last_successful_extract_at so monitoring sees the run as fresh.
            with self._engine.begin() as conn:
                self._touch_freshness(conn)
            return 0

        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_SOURCE],
            bronze_table=_establishments_bronze,
            rejected_table=_establishments_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._update_watermark_state(
                conn,
                etag=self._captured_etag,
                last_modified=self._captured_last_modified,
            )
        return count
