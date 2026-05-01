from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import and_, func, select, tuple_

from src.bronze.hashing import content_hash

if TYPE_CHECKING:
    from typing import Any

    from pydantic import BaseModel
    from sqlalchemy import Connection, Table

    from src.extractors._base import QuarantineRecord

logger = structlog.get_logger()


def filter_new_records(
    hashed: list[tuple[tuple[str, ...], str, BaseModel]],
    existing_hashes: dict[tuple[str, ...], str],
) -> list[tuple[tuple[str, ...], str, BaseModel]]:
    """
    Filter (identity_key, content_hash, record) tuples to only those where
    the hash is new or has changed since the last successful extraction.

    `identity_key` is a tuple of values matching the loader's `identity_fields`
    — `("source_recall_id",)` for sources where source_recall_id is naturally
    unique (CPSC, FDA), or a composite like `("source_recall_id", "langcode")`
    for sources whose natural identity spans multiple columns (USDA bilingual
    pairs share field_recall_number across English and Spanish siblings; see
    documentation/usda/recall_api_observations.md Finding F).

    Pure function — no DB access, fully unit-testable in isolation.
    """
    return [item for item in hashed if existing_hashes.get(item[0]) != item[1]]


class BronzeLoader:
    """
    Generic bronze table writer.

    Implements:
    - Content-hash conditional insert (ADR 0007): skip rows whose hash matches the
      most recent existing row for that identity tuple.
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

    `identity_fields` controls how rows are deduplicated. The default
    `("source_recall_id",)` covers single-natural-key sources (CPSC, FDA). Sources
    whose natural identity spans multiple columns — USDA's bilingual pairs share
    `source_recall_id` across English and Spanish siblings (Finding F) — pass a
    composite like `("source_recall_id", "langcode")`. Each identity column must
    exist on `bronze_table` and must be present in every record's `model_dump()`.
    """

    def __init__(
        self,
        bronze_table: Table,
        rejected_table: Table,
        hash_exclude_fields: frozenset[str] = frozenset(),
        identity_fields: tuple[str, ...] = ("source_recall_id",),
    ) -> None:
        if not identity_fields:
            raise ValueError("identity_fields must contain at least one column name")
        self._bronze = bronze_table
        self._rejected = rejected_table
        self._hash_exclude_fields = hash_exclude_fields
        self._identity_fields = identity_fields

    def _identity_columns(self) -> list[Any]:
        """Return SQLAlchemy column objects for each identity field on the bronze table."""
        return [getattr(self._bronze.c, f) for f in self._identity_fields]

    def _fetch_existing_hashes(
        self,
        conn: Connection,
        identity_keys: list[tuple[str, ...]],
    ) -> dict[tuple[str, ...], str]:
        """
        Return {identity_tuple: content_hash} for the most recent row of each
        composite identity key. Uses a subquery to find the row at
        max(extraction_timestamp) per identity grouping.
        """
        if not identity_keys:
            return {}

        bt = self._bronze
        identity_cols = self._identity_columns()
        identity_tuple = tuple_(*identity_cols)

        # Subquery: latest extraction_timestamp per identity grouping. The
        # composite IN reduces the scan to only the recall_ids in this batch.
        latest_ts = (
            select(
                *identity_cols,
                func.max(bt.c.extraction_timestamp).label("max_ts"),
            )
            .where(identity_tuple.in_(identity_keys))
            .group_by(*identity_cols)
            .subquery()
        )

        # Outer query: join on identity columns + max timestamp to recover the
        # content_hash from that exact (identity, timestamp) row.
        join_conditions = [
            getattr(bt.c, f) == getattr(latest_ts.c, f) for f in self._identity_fields
        ]
        join_conditions.append(bt.c.extraction_timestamp == latest_ts.c.max_ts)
        stmt = select(*identity_cols, bt.c.content_hash).join(latest_ts, and_(*join_conditions))

        rows = conn.execute(stmt).fetchall()
        n = len(self._identity_fields)
        # Each row: (identity_col_1, identity_col_2, ..., content_hash).
        # If multiple bronze rows share the same identity tuple at the same
        # max_ts, the dict comprehension will collapse them to one — but
        # constructing identity correctly upstream means this never happens.
        return {tuple(row[:n]): row[n] for row in rows}

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
            records: Validated Pydantic bronze models. Each must populate every
                `identity_fields` column in its `model_dump()` output.
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

        # --- Compute identity tuples + hashes ---
        hashed: list[tuple[tuple[str, ...], str, BaseModel]] = []
        for record in records:
            row_data = record.model_dump(mode="json")
            identity_values: list[str] = []
            for field_name in self._identity_fields:
                value = row_data.get(field_name)
                if value is None or value == "":
                    raise ValueError(
                        f"{type(record).__name__} has no '{field_name}' field "
                        f"(or value is empty). All bronze schemas must declare every "
                        f"identity field configured on the loader: {self._identity_fields}."
                    )
                identity_values.append(str(value))
            identity_key = tuple(identity_values)

            # hash_exclude_fields strips query artifacts (e.g. FDA's RID position counter)
            # from the hash input without removing them from the DB row — row_data is
            # written to the DB unchanged; only the hash computation sees the filtered dict.
            hash_input = (
                {k: v for k, v in row_data.items() if k not in self._hash_exclude_fields}
                if self._hash_exclude_fields
                else row_data
            )
            hashed.append((identity_key, content_hash(hash_input), record))

        # --- Fetch latest existing hashes for this batch ---
        existing = self._fetch_existing_hashes(conn, [item[0] for item in hashed])

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
            for _identity, h, record in to_insert:
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
