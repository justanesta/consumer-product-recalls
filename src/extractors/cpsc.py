from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import sqlalchemy as sa
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.invariants import check_date_sanity, check_null_source_id
from src.bronze.loader import BronzeLoader
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    AuthenticationError,
    QuarantineRecord,
    RateLimitError,
    RestApiExtractor,
    TransientExtractionError,
)
from src.landing.r2 import R2LandingClient
from src.schemas.cpsc import CpscRecord

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

_CPSC_SOURCE = "cpsc"
_DEFAULT_LOOKBACK_DAYS = 1


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

        url = f"{self.base_url}?format=json&LastPublishDateStart={start_date.isoformat()}"
        return self._fetch(url)

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
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(f"CPSC API returned {response.status_code}")
        raise TransientExtractionError(f"CPSC API returned {response.status_code}")

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
