"""USDA FSIS Establishment Listing extractor (Phase 5b.2).

Targets ``GET /fsis/api/establishments/v/1`` — a flat-array endpoint returning
all 7,945 FSIS-regulated establishments in one response. Per Findings A–G in
``documentation/usda/establishment_api_observations.md``:

- **No pagination, no ETag, no incremental cursor.** Every run is a full dump.
  Idempotency is handled by the bronze content-hash loader (ADR 0007).
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
from datetime import UTC, datetime
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.invariants import check_null_source_id
from src.bronze.loader import BronzeLoader
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    AuthenticationError,
    ExtractionResult,
    QuarantineRecord,
    RateLimitError,
    RestApiExtractor,
    TransientExtractionError,
)
from src.extractors._fsis_headers import browser_headers
from src.landing.r2 import R2LandingClient
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
)

_SOURCE = "usda_establishments"

# Sanity guard: current dataset is 7,945 records (Finding B). 20,000 gives
# ~2.5x headroom against organic growth while still catching a runaway
# upstream shape change (e.g., a sibling endpoint silently merging in).
_MAX_TOTAL_RECORDS = 20_000


class UsdaEstablishmentExtractor(RestApiExtractor[UsdaFsisEstablishment]):
    """Full-dump extractor for the FSIS Establishment Listing API."""

    source_name: str = _SOURCE
    settings: Settings

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")

    def model_post_init(self, __context: Any) -> None:
        self._engine = sa.create_engine(
            self.settings.neon_database_url.get_secret_value(),
            pool_pre_ping=True,
        )
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """Single GET to the establishments endpoint; full dataset returned in one array."""
        records = self._fetch()
        if len(records) > _MAX_TOTAL_RECORDS:
            raise TransientExtractionError(
                f"USDA establishments query returned {len(records)} records — "
                f"exceeds guard of {_MAX_TOTAL_RECORDS}. "
                "Possible cause: upstream dataset shape change."
            )
        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
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
            failure = check_null_source_id(record.source_recall_id)
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
        loader = BronzeLoader(
            bronze_table=_establishments_bronze,
            rejected_table=_establishments_rejected,
            # establishment_id is the stable FSIS FK (Finding F) and never
            # has bilingual siblings or other composite components.
            identity_fields=("source_recall_id",),
        )
        with self._engine.begin() as conn:
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]

    # --- Private helpers ---

    def _fetch(self) -> list[dict[str, Any]]:
        """Single GET to the establishments endpoint.

        Raises TransientExtractionError on 5xx and network errors.
        Raises RateLimitError on 429.
        Raises AuthenticationError on 401/403 (unexpected — this API has no auth).
        """
        try:
            with httpx.Client(
                timeout=self.timeout_seconds,
                headers=browser_headers(),
            ) as client:
                response = client.get(self.base_url)
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"USDA establishments network error: {exc}") from exc

        if response.status_code == 200:
            data = response.json()
            return data if isinstance(data, list) else []
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            self._capture_error_response(response)
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(
                f"USDA establishments API returned {response.status_code} "
                "(unexpected — no auth required)"
            )
        self._capture_error_response(response)
        raise TransientExtractionError(f"USDA establishments API returned {response.status_code}")

    def _capture_error_response(self, response: httpx.Response) -> None:
        try:
            self._r2_client.land_error_response(
                source=_SOURCE,
                request_method=response.request.method,
                request_url=str(response.request.url),
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                "usda_establishments.error_capture_failed",
                status_code=response.status_code,
            )

    def _record_run(
        self,
        run_id: str,
        started_at: datetime,
        status: str,
        result: ExtractionResult | None = None,
        error_message: str | None = None,
    ) -> None:
        row: dict[str, Any] = {
            "source": _SOURCE,
            "started_at": started_at,
            "finished_at": datetime.now(UTC),
            "status": status,
            "run_id": run_id,
            "error_message": error_message,
        }
        if result is not None:
            row["records_extracted"] = result.records_fetched
            row["records_inserted"] = result.records_loaded
            row["records_rejected"] = (
                result.records_rejected_validate + result.records_rejected_invariants
            )
            row["raw_landing_path"] = result.raw_landing_path
        try:
            with self._engine.begin() as conn:
                conn.execute(_extraction_runs.insert().values(**row))
        except Exception as exc:
            # Run-recording is best-effort: the bronze write already committed,
            # so a failure here doesn't lose data. Include the exception type
            # and message so a constraint violation (e.g., missing FK row in
            # source_watermarks for a new source) is diagnosable from logs
            # rather than requiring code-side instrumentation to reproduce.
            logger.warning(
                "extraction_run.record_failed",
                run_id=run_id,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )
