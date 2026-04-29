from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
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
    AuthenticationError,
    ExtractionResult,
    QuarantineRecord,
    RateLimitError,
    RestApiExtractor,
    TransientExtractionError,
)
from src.landing.r2 import R2LandingClient
from src.schemas.cpsc import CpscRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
# Column set is the minimum required for BronzeLoader queries (source_recall_id,
# content_hash, extraction_timestamp) plus the full schema for inserts.
_metadata = sa.MetaData()

_cpsc_bronze = sa.Table(
    "cpsc_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("recall_id", sa.Integer),
    sa.Column("recall_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("last_publish_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("title", sa.Text),
    sa.Column("description", sa.Text),
    sa.Column("url", sa.Text),
    sa.Column("consumer_contact", sa.Text),
    sa.Column("products", postgresql.JSONB),
    sa.Column("manufacturers", postgresql.JSONB),
    sa.Column("retailers", postgresql.JSONB),
    sa.Column("importers", postgresql.JSONB),
    sa.Column("distributors", postgresql.JSONB),
    sa.Column("manufacturer_countries", postgresql.JSONB),
    sa.Column("product_upcs", postgresql.JSONB),
    sa.Column("hazards", postgresql.JSONB),
    sa.Column("remedies", postgresql.JSONB),
    sa.Column("remedy_options", postgresql.JSONB),
    sa.Column("in_conjunctions", postgresql.JSONB),
    sa.Column("sold_at_label", sa.Text),
    sa.Column("images", postgresql.JSONB),
    sa.Column("injuries", postgresql.JSONB),
)

_cpsc_rejected = sa.Table(
    "cpsc_recalls_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", postgresql.JSONB),
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
)

_CPSC_SOURCE = "cpsc"
_DEFAULT_LOOKBACK_DAYS = 1
# Guard: if an incremental window returns more records than this, the
# LastPublishDateStart parameter likely didn't apply (e.g. watermark returned
# unexpected type or param name drifted). Abort rather than silently load the
# full ~9,700-record dataset. Not applied on deep-rescan / historical-seed paths.
# Threshold is set well below the full CPSC dataset (~9,700 records) but above
# any realistic daily or weekly increment. Wide-window cassette tests return
# ~2,500 records — a value of 5,000 allows those while still catching a
# full-dataset return.
_MAX_INCREMENTAL_RECORDS = 5_000


class CpscExtractor(RestApiExtractor[CpscRecord]):
    """
    Extractor for CPSC Recall Retrieval Web Services.

    Queries the CPSC REST API incrementally using LastPublishDateStart, reads
    the current watermark from source_watermarks, and updates it transactionally
    with the bronze insert (ADR 0020).

    No auth required. No pagination — a single request returns all matching records.
    """

    source_name: str = _CPSC_SOURCE
    settings: Settings

    # Private state: created in model_post_init, shared across lifecycle calls
    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    # Stored by land_raw() so validate_records() and check_invariants() can
    # reference it when building QuarantineRecords.
    _current_landing_path: str = PrivateAttr(default="")

    def model_post_init(self, __context: Any) -> None:
        self._engine = sa.create_engine(
            self.settings.neon_database_url.get_secret_value(),
            pool_pre_ping=True,
        )
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch all recalls published on or after the current watermark date.
        Raises TransientExtractionError on 5xx, RateLimitError on 429,
        AuthenticationError on 401/403.
        """
        with self._engine.connect() as conn:
            start_date = self._get_watermark(conn)

        if not isinstance(start_date, date):
            raise TransientExtractionError(
                f"CPSC watermark returned unexpected type {type(start_date)!r}; "
                "aborting to avoid unfiltered full-database pull"
            )

        url = f"{self.base_url}?format=json&LastPublishDateStart={start_date.isoformat()}"
        records = self._fetch(url)

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"CPSC incremental query returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. "
                "Possible cause: invalid or missing LastPublishDateStart parameter."
            )

        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_CPSC_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[CpscRecord], list[QuarantineRecord]]:
        valid: list[CpscRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(CpscRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("RecallNumber")) or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[CpscRecord]
    ) -> tuple[list[CpscRecord], list[QuarantineRecord]]:
        passing: list[CpscRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = check_null_source_id(record.source_recall_id) or check_date_sanity(
                record.recall_date, "recall_date"
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
        records: list[CpscRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        loader = BronzeLoader(bronze_table=_cpsc_bronze, rejected_table=_cpsc_rejected)
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            if records:
                max_date = max(r.last_publish_date for r in records).date()
                self._update_watermark(conn, max_date)
        return count

    # --- Private helpers ---

    def _fetch(self, url: str) -> list[dict[str, Any]]:
        """Make a single GET request to the CPSC API. Raises on non-200 status."""
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(url)
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"CPSC network error: {exc}") from exc

        if response.status_code == 200:
            data = response.json()
            return data if isinstance(data, list) else []
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            self._capture_error_response(url, response)
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(f"CPSC API returned {response.status_code}")
        self._capture_error_response(url, response)
        raise TransientExtractionError(f"CPSC API returned {response.status_code}")

    def _capture_error_response(self, url: str, response: httpx.Response) -> None:
        """Best-effort: land non-2xx response to R2 for future cassette promotion."""
        try:
            self._r2_client.land_error_response(
                source=_CPSC_SOURCE,
                request_method=response.request.method,
                request_url=url,
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                "cpsc.error_capture_failed",
                status_code=response.status_code,
                url=url,
            )

    def _get_watermark(self, conn: sa.Connection) -> date:
        row = conn.execute(
            sa.select(_source_watermarks.c.last_cursor).where(
                _source_watermarks.c.source == _CPSC_SOURCE
            )
        ).fetchone()
        if row and row[0]:
            return date.fromisoformat(row[0])
        return datetime.now(UTC).date() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)

    def _update_watermark(self, conn: sa.Connection, new_date: date) -> None:
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _CPSC_SOURCE)
            .values(last_cursor=new_date.isoformat(), updated_at=datetime.now(UTC))
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
            "source": _CPSC_SOURCE,
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
        except Exception:
            logger.warning("extraction_run.record_failed", run_id=run_id, status=status)
