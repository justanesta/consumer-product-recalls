from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import sqlalchemy as sa
import structlog
from sqlalchemy import and_, func, select, tuple_

from src.bronze.hashing import content_hash

if TYPE_CHECKING:
    from typing import Any

    from pydantic import BaseModel
    from sqlalchemy import Connection, Table

    from src.bronze.dedup_contracts import DedupContract
    from src.extractors._base import QuarantineRecord

logger = structlog.get_logger()

# Postgres' Bind message uses an int16 parameter count — practical ceiling
# is 65,535 parameters per query. The composite ``tuple_(*cols).in_(...)``
# clause in ``_fetch_existing_hashes`` contributes
# ``len(identity_fields) × len(identity_keys)`` parameters; for NHTSA's
# 11-tuple that's ~723k at the ~65k-row corpus where the size-related
# ``OperationalError`` was first observed after the type-binding fix (the
# corpus is now ~321k → ~3.5M unchunked).
# Chunking the lookup at ~5k keys per query stays under the protocol
# ceiling and reduces planner memory pressure on small-compute Neon
# branches. Results merge losslessly because dedup is per-row, not
# cross-row — each identity tuple appears in exactly one chunk's result.
_PG_PARAM_SAFETY_LIMIT = 60_000


class WithinBatchIdentityCollisionError(ValueError):
    """Raised when within-batch dedup encounters the same identity tuple
    paired with two different content hashes.

    Used by sources that opt into ``within_batch_dedup=True`` (NHTSA per
    ADR 0030). The expected dedup case is ``(identity, hash)`` byte-duplicate
    rows produced by the source — same identity, same hash, collapse to one.
    Same identity with *different* hashes indicates either a source-format
    change (a previously-constant field now varies within a duplicate set)
    or an extractor bug; either way it requires investigation rather than
    silent loss of one of the variants.
    """


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
        within_batch_dedup: bool = False,
        allow_null_identity: bool = False,
    ) -> None:
        if not identity_fields:
            raise ValueError("identity_fields must contain at least one column name")
        self._bronze = bronze_table
        self._rejected = rejected_table
        self._hash_exclude_fields = hash_exclude_fields
        self._identity_fields = identity_fields
        self._within_batch_dedup = within_batch_dedup
        self._allow_null_identity = allow_null_identity

    @classmethod
    def from_contract(
        cls,
        contract: DedupContract,
        *,
        bronze_table: Table,
        rejected_table: Table,
        within_batch_dedup: bool | None = None,
        allow_null_identity: bool | None = None,
    ) -> BronzeLoader:
        """Build a loader from a source's :class:`DedupContract` (the dedup-oracle SSOT).

        The oracle (``identity_fields`` + ``hash_exclude_fields``) comes straight from the
        contract — that is what must be identical across a source's incremental,
        deep-rescan, and recovery paths. The operational flags default to the contract's
        incremental values; pass an explicit ``within_batch_dedup`` / ``allow_null_identity``
        to override per mode (e.g. FDA's deep-rescan passes ``within_batch_dedup=True``).
        ``bronze_table`` / ``rejected_table`` stay call-site args — they are per-source
        table objects, not part of the semantic oracle.
        """
        return cls(
            bronze_table=bronze_table,
            rejected_table=rejected_table,
            hash_exclude_fields=contract.hash_exclude_fields,
            identity_fields=contract.identity_fields,
            within_batch_dedup=(
                contract.default_within_batch_dedup
                if within_batch_dedup is None
                else within_batch_dedup
            ),
            allow_null_identity=(
                contract.default_allow_null_identity
                if allow_null_identity is None
                else allow_null_identity
            ),
        )

    def _identity_columns(self) -> list[Any]:
        """Return SQLAlchemy column objects for each identity field on the bronze table."""
        return [getattr(self._bronze.c, f) for f in self._identity_fields]

    def _identity_text_expr(self, col: Any) -> Any:
        """Return a text-canonical SQL expression for one identity column.

        Used by ``_fetch_existing_hashes`` to make the IN comparison and the
        result columns uniformly text-vs-text, regardless of the source
        column's SQL type. The motivation is the NHTSA case (per ADR 0030):
        the 11-tuple identity includes ``bgman`` / ``endman`` ``TIMESTAMPTZ``
        columns, and the loader's identity-value normalization produces ``""``
        for missing values when ``allow_null_identity=True``. SQLAlchemy
        types each IN-clause bind parameter from the corresponding column,
        so an empty string would be bound as TIMESTAMPTZ — Postgres rejects
        that with ``DataError: invalid input syntax for type timestamp with
        time zone: ""``. Casting both sides to text dodges the typed-bind
        problem; aligning the format aligns populated values too.

        For ``DateTime``-family columns: ``to_char`` with an ISO-8601-with-Z
        format matching Pydantic's ``model_dump(mode="json")`` serialization
        of UTC datetimes (e.g. ``"2024-03-06T00:00:00Z"``). ``AT TIME ZONE
        'UTC'`` forces UTC interpretation regardless of session timezone.
        For all other column types: ``cast → text`` with NULL coalesced to
        empty string. Integer / numeric columns work too: ``str(int_value)``
        on the Python side equals ``cast(int_col, TEXT)`` on the Postgres
        side.

        Caveat: the format does not include microseconds. NHTSA's date
        fields always have second-or-coarser precision, so this is fine.
        Future sources with microsecond-precision datetimes in identity
        would need to extend the format string to ``...HH24:MI:SS.US"Z"``.
        """
        if isinstance(col.type, sa.DateTime):
            return func.coalesce(
                func.to_char(
                    col.op("AT TIME ZONE")(sa.literal("UTC")),
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"',
                ),
                "",
            )
        return func.coalesce(sa.cast(col, sa.Text), "")

    def _dedup_within_batch(
        self,
        hashed: list[tuple[tuple[str, ...], str, BaseModel]],
    ) -> list[tuple[tuple[str, ...], str, BaseModel]]:
        """Collapse ``(identity, content_hash)`` duplicates within a single batch.

        Same ``(identity, hash)`` pair → keep first occurrence and discard the
        rest. Records sharing both identity and hash are byte-equivalent (post
        ``hash_exclude_fields`` filtering), so the choice of which to keep is
        arbitrary. This is the case ADR 0030 documents for NHTSA's TSV-shipped
        byte-duplicate rows (~0.7% of the corpus per Finding L).

        Same identity with *different* hashes → raise
        ``WithinBatchIdentityCollisionError``. The expected NHTSA dedup case
        leaves all colliding rows byte-identical post-RECORD_ID-exclusion;
        a hash mismatch means the source changed shape (a field that used to
        be constant within a duplicate set now varies) or the extractor has a
        bug. Surface loudly rather than silently picking one variant.

        Pure function on the input list — no DB access. Caller controls
        whether this runs via ``within_batch_dedup`` flag at construction.
        """
        seen: dict[tuple[str, ...], str] = {}
        deduped: list[tuple[tuple[str, ...], str, BaseModel]] = []
        for item in hashed:
            identity, hash_value, _record = item
            existing_hash = seen.get(identity)
            if existing_hash is None:
                seen[identity] = hash_value
                deduped.append(item)
            elif existing_hash == hash_value:
                # Genuine byte-duplicate (per ADR 0030 / Finding L) — collapse.
                continue
            else:
                raise WithinBatchIdentityCollisionError(
                    f"Within-batch identity collision with different content hashes: "
                    f"identity={identity!r}, hashes={existing_hash!r} vs {hash_value!r}. "
                    f"Indicates either a source-format change (a previously-constant "
                    f"field now varies within a duplicate set) or an extractor bug."
                )
        return deduped

    def _fetch_existing_hashes(
        self,
        conn: Connection,
        identity_keys: list[tuple[str, ...]],
    ) -> dict[tuple[str, ...], str]:
        """
        Return {identity_tuple: content_hash} for the most recent row of each
        composite identity key. Uses a subquery to find the row at
        max(extraction_timestamp) per identity grouping.

        Identity columns are text-canonicalized via ``_identity_text_expr``
        on both sides of the IN comparison so the bind parameters are
        uniformly TEXT-typed, dodging the ``TIMESTAMPTZ`` empty-string
        binding error (see helper docstring). The result tuples are also
        text-canonical, matching the format the caller's ``identity_keys``
        use, so dict-key equality works in ``filter_new_records``.

        Routes by batch size against ``_PG_PARAM_SAFETY_LIMIT``:

        - **Batches that fit one IN-query** (``len <= chunk_size`` — e.g. CPSC's
          ~10-row daily delta) run as a single ``_fetch_existing_hashes_chunk``:
          one seq-scan, no temp-table overhead.
        - **Batches that would otherwise fan out into many chunked IN-queries**
          (NHTSA's ~241k full dump → ~45 chunks, each a full bronze seq-scan
          recomputing the text-canonical identity per row) take one set-based
          ``_fetch_existing_hashes_staged`` join instead: O(corpus) one pass vs
          O(corpus × chunks). The result dict is identical either way — the
          staged path is a pure ``IN → JOIN`` restructure of the chunk query.
        """
        if not identity_keys:
            return {}

        n_cols = len(self._identity_fields)
        chunk_size = max(1, _PG_PARAM_SAFETY_LIMIT // n_cols)

        if len(identity_keys) <= chunk_size:
            return self._fetch_existing_hashes_chunk(conn, identity_keys)
        return self._fetch_existing_hashes_staged(conn, identity_keys)

    def _fetch_existing_hashes_chunk(
        self,
        conn: Connection,
        identity_keys: list[tuple[str, ...]],
    ) -> dict[tuple[str, ...], str]:
        """One-shot existing-hash lookup for a chunk of identity_keys.

        Same query shape as the historical ``_fetch_existing_hashes`` body:
        text-canonical composite IN match against bronze, GROUP BY identity
        for the latest extraction_timestamp, JOIN back to bronze on the
        text-canonical identity + max_ts to recover content_hash. The
        wrapper above splits large batches; this method assumes ``chunk``
        is already small enough to fit under ``_PG_PARAM_SAFETY_LIMIT``.
        """
        bt = self._bronze
        identity_text_exprs = [self._identity_text_expr(col) for col in self._identity_columns()]

        # Subquery: latest extraction_timestamp per text-canonical identity
        # grouping. The composite IN reduces the scan to only the identity
        # tuples in this batch. Both LHS (text-canonicalized bronze cols)
        # and RHS (caller's identity_keys, already strings) are TEXT — empty
        # strings bind cleanly regardless of the underlying column type.
        latest_ts = (
            select(
                *[
                    expr.label(f)
                    for expr, f in zip(identity_text_exprs, self._identity_fields, strict=True)
                ],
                func.max(bt.c.extraction_timestamp).label("max_ts"),
            )
            .where(tuple_(*identity_text_exprs).in_(identity_keys))
            .group_by(*identity_text_exprs)
            .subquery()
        )

        # Outer query: join the subquery's text-canonical identity output
        # back to bronze, also via text-canonical expressions, so the join
        # keys are type-aligned. ``extraction_timestamp`` joins natively.
        # Build a fresh expression list — SQLAlchemy expressions aren't
        # safe to share across distinct query positions.
        outer_text_exprs = [self._identity_text_expr(col) for col in self._identity_columns()]
        join_conditions = [
            outer_text_exprs[i] == getattr(latest_ts.c, f)
            for i, f in enumerate(self._identity_fields)
        ]
        join_conditions.append(bt.c.extraction_timestamp == latest_ts.c.max_ts)
        stmt = select(
            *[getattr(latest_ts.c, f) for f in self._identity_fields],
            bt.c.content_hash,
        ).join(latest_ts, and_(*join_conditions))

        rows = conn.execute(stmt).fetchall()
        n = len(self._identity_fields)
        # Each row: (text_canonical_identity_1, ..., text_canonical_identity_N,
        # content_hash). The text-canonical tuple matches the caller's
        # identity_keys format, so the returned dict's keys can be looked
        # up directly via ``existing.get(item[0])`` in ``filter_new_records``.
        return {tuple(row[:n]): row[n] for row in rows}

    def _fetch_existing_hashes_staged(
        self,
        conn: Connection,
        identity_keys: list[tuple[str, ...]],
    ) -> dict[tuple[str, ...], str]:
        """Set-based existing-hash lookup via a session TEMP table.

        One bronze pass instead of the ~45-59 chunked IN seq-scans the chunk
        loop did for a full-corpus batch (NHTSA daily/deep-rescan). Returns the
        identical ``{identity_tuple: latest_content_hash}`` dict: this is a pure
        ``IN → JOIN`` restructure of ``_fetch_existing_hashes_chunk`` — same
        ``_identity_text_expr`` text-canonical comparison, same two-stage
        latest-per-identity recovery — so the dedup semantics ``filter_new_records``
        sees are unchanged (correct by construction, not just by test).

        Mechanics: bulk-load the (already text-canonical) ``identity_keys`` into a
        TEMP table as chunked multi-row INSERTs (no ``psycopg2`` COPY dependency),
        ``ANALYZE`` it so the planner hash-joins (a zero-stats temp table risks a
        nested loop → per-row bronze seq-scans), then JOIN bronze to it on
        ``_identity_text_expr`` in place of the chunked ``tuple_(...).in_(...)``.
        The TEMP table lives in ``pg_temp`` — it needs only the database
        ``TEMPORARY`` privilege (held by ``recalls_app`` via the PUBLIC default),
        not ``CREATE`` on ``public`` — and ``ON COMMIT DROP`` reaps it when the
        caller's transaction commits (no leak, nothing survives a rollback).
        """
        bt = self._bronze
        n = len(self._identity_fields)
        chunk_size = max(1, _PG_PARAM_SAFETY_LIMIT // n)
        staging_name = f"_dedup_ids_{uuid4().hex}"
        staging_cols = [f"c{i}" for i in range(n)]

        # 1. Session TEMP table of N text identity columns; self-reaping at commit.
        col_ddl = ", ".join(f"{c} text" for c in staging_cols)
        conn.exec_driver_sql(f"CREATE TEMP TABLE {staging_name} ({col_ddl}) ON COMMIT DROP")
        staging = sa.table(staging_name, *[sa.column(c, sa.Text) for c in staging_cols])

        # 2. Load the text identity tuples as chunked multi-row INSERTs — each a
        #    single VALUES statement under the bind-param ceiling.
        for start in range(0, len(identity_keys), chunk_size):
            batch = identity_keys[start : start + chunk_size]
            conn.execute(
                staging.insert().values(
                    [dict(zip(staging_cols, key, strict=True)) for key in batch]
                )
            )

        # 3. Stats so the planner hash-joins instead of nested-looping per bronze row.
        conn.exec_driver_sql(f"ANALYZE {staging_name}")

        # 4. Latest extraction_timestamp per text-canonical identity, restricted to
        #    the staged identities by JOIN (the chunk path's IN clause, set-based).
        sub_text_exprs = [self._identity_text_expr(col) for col in self._identity_columns()]
        sub_join = and_(*[sub_text_exprs[i] == staging.c[staging_cols[i]] for i in range(n)])
        latest_ts = (
            select(
                *[
                    expr.label(f)
                    for expr, f in zip(sub_text_exprs, self._identity_fields, strict=True)
                ],
                func.max(bt.c.extraction_timestamp).label("max_ts"),
            )
            .select_from(bt.join(staging, sub_join))
            .group_by(*sub_text_exprs)
            .subquery()
        )

        # 5. Join back to bronze for content_hash at that max_ts — identical to the
        #    chunk path's outer query.
        outer_text_exprs = [self._identity_text_expr(col) for col in self._identity_columns()]
        join_conditions = [
            outer_text_exprs[i] == getattr(latest_ts.c, f)
            for i, f in enumerate(self._identity_fields)
        ]
        join_conditions.append(bt.c.extraction_timestamp == latest_ts.c.max_ts)
        stmt = select(
            *[getattr(latest_ts.c, f) for f in self._identity_fields],
            bt.c.content_hash,
        ).join(latest_ts, and_(*join_conditions))

        rows = conn.execute(stmt).fetchall()
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
                    if not self._allow_null_identity:
                        raise ValueError(
                            f"{type(record).__name__} has no '{field_name}' field "
                            f"(or value is empty). All bronze schemas must declare every "
                            f"identity field configured on the loader: {self._identity_fields}."
                        )
                    # NHTSA per ADR 0030: nullable identity components
                    # (bgman, endman, mfr_comp_*) are legitimate empty
                    # values, not bugs. Normalize None and "" to the
                    # same sentinel so they map to the same identity
                    # bucket — within_batch_dedup can then collapse
                    # byte-identical rows whose identity tuples include
                    # empty values.
                    identity_values.append("")
                else:
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

        # --- Within-batch dedup (opt-in per source; NHTSA per ADR 0030) ---
        if self._within_batch_dedup:
            pre_dedup_count = len(hashed)
            hashed = self._dedup_within_batch(hashed)
            if len(hashed) != pre_dedup_count:
                log.debug(
                    "bronze_loader.within_batch_dedup",
                    pre_dedup=pre_dedup_count,
                    post_dedup=len(hashed),
                    collapsed=pre_dedup_count - len(hashed),
                )

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
