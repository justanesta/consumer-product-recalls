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
    check_usda_bilingual_pairing,
    run_per_record_invariants,
)
from src.bronze.loader import BronzeLoader
from src.extractors._base import (
    QuarantineRecord,
    TransientExtractionError,
)
from src.extractors._fsis_base import FsisConditionalGetExtractor
from src.schemas.usda import UsdaFsisRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_usda_bronze = sa.Table(
    "usda_fsis_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("langcode", sa.Text),
    sa.Column("title", sa.Text),
    sa.Column("recall_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("recall_type", sa.Text),
    sa.Column("recall_classification", sa.Text),
    sa.Column("archive_recall", sa.Boolean),
    sa.Column("has_spanish", sa.Boolean),
    sa.Column("active_notice", sa.Boolean),
    sa.Column("last_modified_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("closed_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("related_to_outbreak", sa.Boolean),
    sa.Column("closed_year", sa.Text),
    sa.Column("year", sa.Text),
    sa.Column("risk_level", sa.Text),
    # Multi-value fields are JSONB as of the 2026-06 USDA API change (migration 0028,
    # Finding S) — FSIS flipped them from comma-joined scalars to JSON arrays.
    sa.Column("recall_reason", postgresql.JSONB),
    sa.Column("processing", postgresql.JSONB),
    sa.Column("states", postgresql.JSONB),
    sa.Column("establishment", postgresql.JSONB),
    sa.Column("labels", postgresql.JSONB),
    sa.Column("qty_recovered", sa.Text),
    sa.Column("summary", sa.Text),
    sa.Column("product_items", postgresql.JSONB),
    sa.Column("distro_list", postgresql.JSONB),
    sa.Column("media_contact", sa.Text),
    sa.Column("company_media_contact", postgresql.JSONB),
    sa.Column("recall_url", sa.Text),
    sa.Column("en_press_release", postgresql.JSONB),
    sa.Column("press_release", postgresql.JSONB),
    # Added by the 2026-06 API change (Finding S); scalar export form of the recall number.
    sa.Column("recall_number_export", sa.Text),
)

_usda_rejected = sa.Table(
    "usda_fsis_recalls_rejected",
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
# cross-source operational tables — see src/extractors/_tables.py.

_USDA_SOURCE = "usda"

# Guard ceiling for the incremental path. Full dataset is ~2,001 records (Finding B);
# 5_000 leaves a wide margin for organic growth while still catching a runaway bug
# (e.g., the API starting to return paginated results we don't handle, or some other
# upstream change ballooning the dataset). Not applied on the deep-rescan path.
_MAX_INCREMENTAL_RECORDS = 5_000

# Akamai Bot Manager workaround lives in src/extractors/_fsis_headers.py
# (shared with the Establishment extractor; both APIs sit on www.fsis.usda.gov).


class UsdaExtractor(FsisConditionalGetExtractor[UsdaFsisRecord]):
    """
    Extractor for USDA FSIS recall records — incremental path.

    Strategy: full-dump every run. Finding D confirmed both
    `field_last_modified_date_value` and `field_last_modified_date` query parameters
    are silently ignored — there is no working server-side filter. The full ~2,001-
    record dataset is returned in one flat JSON array; idempotency is handled by the
    bronze content-hash loader (ADR 0007).

    ETag optimization (Finding A — cache-control: public, max-age=3100): the extractor
    reads `source_watermarks.last_etag`, sends `If-None-Match` on every request when
    populated, and short-circuits cleanly on 304 (skipping the ~12 MB download and
    skipping the bronze write). A contradiction guard fails the run if a 304 is paired
    with a `last-modified` header that has advanced past the prior recorded value.
    Disable by setting `etag_enabled=False` (or by manually nulling
    source_watermarks.last_etag for the usda row).

    For historical loads / forced re-ingestion use `UsdaDeepRescanLoader`, which never
    sends `If-None-Match` and never updates the watermark — see its docstring.
    """

    source_name: str = _USDA_SOURCE
    # settings, etag_enabled (production default ON since 2026-05-09 per Finding P —
    # etag_viability.sql green-lit it), the engine/R2 + ETag-capture state, and
    # model_post_init are inherited from FsisConditionalGetExtractor.
    # UsdaDeepRescanLoader below overrides etag_enabled to False.

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch all USDA FSIS recall records.

        Returns [] on a 304 Not Modified (and sets _not_modified so downstream
        steps no-op). Raises TransientExtractionError on 5xx / network /
        oversized response. Raises ExtractionError (no retry) on the
        contradiction guard (304 paired with advanced last-modified).
        """
        prior_etag, prior_last_modified = self._read_etag_state()
        records, status_code, etag, last_modified = self._fetch(prior_etag, prior_last_modified)

        if status_code == 304:
            self._not_modified = True
            logger.info(
                "usda.extract.not_modified",
                etag=prior_etag,
                last_modified_header=last_modified,
            )
            self._guard_etag_contradiction(prior_last_modified, last_modified)
            return []

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"USDA incremental query returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. "
                "Possible cause: upstream dataset size change or API shape drift."
            )

        # Stash captured headers for atomic write in load_bronze().
        self._captured_etag = etag
        self._captured_last_modified = last_modified
        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        if self._not_modified:
            # Nothing to land; skip R2 write. Empty path string is a no-op marker
            # consumed by load_bronze() and by quarantine routing (which has no
            # records to route on a 304 path).
            self._current_landing_path = ""
            return ""
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_USDA_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UsdaFsisRecord], list[QuarantineRecord]]:
        valid: list[UsdaFsisRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UsdaFsisRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("field_recall_number")) or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UsdaFsisRecord]
    ) -> tuple[list[UsdaFsisRecord], list[QuarantineRecord]]:
        # Run per-record invariants first (null id, date sanity).
        post_basic: list[UsdaFsisRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = run_per_record_invariants(
                record, PER_RECORD_INVARIANTS_BY_SOURCE_NAME[_USDA_SOURCE]
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
                post_basic.append(record)

        # Bilingual pairing invariant: Spanish records without an English sibling are
        # quarantined (ADR 0006). The shared invariant function lives in src/bronze/invariants.py
        # and was scaffolded in Phase 2 for exactly this site.
        passing, bilingual_rejects = check_usda_bilingual_pairing(
            post_basic,
            recall_number_fn=lambda r: r.source_recall_id,
            is_spanish_fn=lambda r: r.langcode == "Spanish",
            raw_landing_path=self._current_landing_path,
        )
        quarantined.extend(bilingual_rejects)
        return passing, quarantined

    def load_bronze(
        self,
        records: list[UsdaFsisRecord],
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
            DEDUP_CONTRACT_BY_SOURCE_NAME[_USDA_SOURCE],
            bronze_table=_usda_bronze,
            rejected_table=_usda_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._update_watermark_state(
                conn,
                etag=self._captured_etag,
                last_modified=self._captured_last_modified,
            )
        return count


class UsdaDeepRescanLoader(UsdaExtractor):
    """
    Historical / deep-rescan loader for USDA FSIS records.

    USDA's API has no working server-side filter (Finding D), so the deep-rescan path
    fetches the same full-dump response shape as the incremental path. The two
    behaviors that differ from `UsdaExtractor`:

    1. **Never sends `If-None-Match`** — even when source_watermarks.last_etag is
       populated. The deep-rescan workflow exists to re-pull the full payload
       unconditionally, so any silent ETag-bug self-corrects within ≤7 days
       (the cron cadence of deep-rescan-usda.yml in Phase 7).
    2. **Never updates source_watermarks** — the incremental extractor owns the
       watermark and ETag exclusively. Deep rescan is purely additive to the bronze
       table.

    Used by the deep-rescan-usda.yml GitHub Actions workflow.
    """

    # Force-disable ETag handling for this subclass regardless of config.
    etag_enabled: bool = False

    def load_bronze(
        self,
        records: list[UsdaFsisRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        # Does NOT touch source_watermarks — the incremental extractor owns it.
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_USDA_SOURCE],
            bronze_table=_usda_bronze,
            rejected_table=_usda_rejected,
        )
        with self._engine.begin() as conn:
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
