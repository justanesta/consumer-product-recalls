from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import func, select

from src.bronze.hashing import content_hash

if TYPE_CHECKING:
    from typing import Any

    from pydantic import BaseModel
    from sqlalchemy import Connection, Table

    from src.extractors._base import QuarantineRecord

logger = structlog.get_logger()


def filter_new_records(
    hashed: list[tuple[str, str, BaseModel]],
    existing_hashes: dict[str, str],
) -> list[tuple[str, str, BaseModel]]:
    """
    Filter (source_recall_id, content_hash, record) tuples to only those where
    the hash is new or has changed since the last successful extraction.
    Pure function — no DB access, fully unit-testable in isolation.
    """
    return [item for item in hashed if existing_hashes.get(item[0]) != item[1]]


class BronzeLoader:
    """
    Generic bronze table writer.

    Implements:
    - Content-hash conditional insert (ADR 0007): skip rows whose hash matches the
      most recent existing row for that source_recall_id.
    - Quarantine routing (ADR 0013): write failed records to the source _rejected table
      and emit a structured warning log.

    The caller owns the transaction boundary (pass a Connection from engine.begin()).
    This allows concrete extractor load_bronze() implementations to include
    source_watermarks and extraction_runs updates in the same transaction (ADR 0020):

        def load_bronze(self, records, quarantined, raw_landing_path):
            with self._engine.begin() as conn:
                count = self._loader.load(conn, records, quarantined, raw_landing_path)
                conn.execute(update_watermarks_stmt)   # same txn per ADR 0020
            return count

    Convention: every bronze Pydantic schema must declare a source_recall_id field.
    BronzeLoader raises ValueError on the first record that violates this convention.
    """

    def __init__(self, bronze_table: Table, rejected_table: Table) -> None:
        self._bronze = bronze_table
        self._rejected = rejected_table

    def _fetch_existing_hashes(
        self,
        conn: Connection,
        source_recall_ids: list[str],
    ) -> dict[str, str]:
        """
        Return {source_recall_id: content_hash} for the most recent row of each ID.
        Uses a subquery to find the row at max(extraction_timestamp) per ID.
        """
        if not source_recall_ids:
            return {}

        bt = self._bronze
        latest_ts = (
            select(
                bt.c.source_recall_id,
                func.max(bt.c.extraction_timestamp).label("max_ts"),
            )
            .where(bt.c.source_recall_id.in_(source_recall_ids))
            .group_by(bt.c.source_recall_id)
            .subquery()
        )
        stmt = select(bt.c.source_recall_id, bt.c.content_hash).join(
            latest_ts,
            (bt.c.source_recall_id == latest_ts.c.source_recall_id)
            & (bt.c.extraction_timestamp == latest_ts.c.max_ts),
        )
        rows = conn.execute(stmt).fetchall()
        return {row[0]: row[1] for row in rows}

    def load(
        self,
        conn: Connection,
        records: list[BaseModel],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
        extraction_timestamp: datetime | None = None,
    ) -> int:
        """
        Insert new/changed records and write quarantine rows. Returns bronze insert count.

        Args:
            conn: Active SQLAlchemy connection; caller manages transaction.
            records: Validated Pydantic bronze models. Each must have source_recall_id.
            quarantined: Records that failed validate_records() or check_invariants().
            raw_landing_path: R2 key from land_raw(); stored on every inserted row.
            extraction_timestamp: Defaults to now(UTC) if not provided.

        Returns:
            Count of bronze rows actually inserted (hash-identical rows excluded).
        """
        ts = extraction_timestamp or datetime.now(UTC)
        log = logger.bind(bronze_table=self._bronze.name)

        if not records and not quarantined:
            return 0

        # --- Compute hashes ---
        hashed: list[tuple[str, str, BaseModel]] = []
        for record in records:
            row_data = record.model_dump(mode="json")
            rid = row_data.get("source_recall_id")
            if not rid:
                raise ValueError(
                    f"{type(record).__name__} has no source_recall_id field. "
                    "All bronze schemas must declare source_recall_id."
                )
            hashed.append((str(rid), content_hash(row_data), record))

        # --- Fetch latest existing hashes for this batch ---
        ids = [item[0] for item in hashed]
        existing = self._fetch_existing_hashes(conn, ids)

        # --- Skip rows whose hash hasn't changed ---
        to_insert = filter_new_records(hashed, existing)
        log.debug(
            "bronze_loader.dedup",
            total=len(hashed),
            to_insert=len(to_insert),
            skipped=len(hashed) - len(to_insert),
        )

        # --- Batch insert new / changed records ---
        if to_insert:
            insert_rows: list[dict[str, Any]] = []
            for _rid, h, record in to_insert:
                row_data = record.model_dump(mode="json")
                row_data["content_hash"] = h
                row_data["extraction_timestamp"] = ts
                row_data["raw_landing_path"] = raw_landing_path
                insert_rows.append(row_data)
            conn.execute(self._bronze.insert(), insert_rows)

        # --- Quarantine routing (T1) ---
        if quarantined:
            rejected_rows: list[dict[str, Any]] = [
                {
                    "source_recall_id": q.source_recall_id,
                    "raw_record": q.raw_record,
                    "failure_reason": q.failure_reason,
                    "failure_stage": q.failure_stage,
                    "rejected_at": ts,
                    "raw_landing_path": q.raw_landing_path,
                }
                for q in quarantined
            ]
            conn.execute(self._rejected.insert(), rejected_rows)
            log.warning(
                "bronze_loader.quarantine",
                count=len(quarantined),
                rejected_table=self._rejected.name,
            )

        log.info("bronze_loader.load.completed", inserted=len(to_insert))
        return len(to_insert)
