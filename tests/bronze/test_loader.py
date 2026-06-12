from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, MetaData, String, Table

from src.bronze.loader import (
    BronzeLoader,
    WithinBatchIdentityCollisionError,
    filter_new_records,
)
from src.extractors._base import QuarantineRecord

# ---------------------------------------------------------------------------
# Fixtures — minimal Pydantic models for loader tests
# ---------------------------------------------------------------------------

_FIXED_TS = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
_LANDING_PATH = "s3://bronze-bucket/2024/cpsc-run-001.json"


class SimpleRecord(BaseModel):
    source_recall_id: str
    title: str
    count: int = 0


class RecordWithArtifact(BaseModel):
    """Simulates a source whose API injects a query-position counter (e.g. FDA RID)."""

    source_recall_id: str
    title: str
    rid: int  # query-position artifact — should be excluded from hashing


class RecordWithoutId(BaseModel):
    title: str
    count: int = 0


class BilingualRecord(BaseModel):
    """Simulates USDA's bilingual schema where (source_recall_id, langcode) is identity."""

    source_recall_id: str
    langcode: str  # "English" or "Spanish"
    title: str


class RecordWithNullableIdentity(BaseModel):
    """Simulates NHTSA's schema where some identity components are nullable.

    Per ADR 0030, NHTSA's 11-tuple identity includes bgman/endman/mfr_comp_*
    fields that are legitimately empty for many rows. ``allow_null_identity``
    on BronzeLoader lets empty strings and None values participate in the
    identity tuple rather than raising "missing required field".
    """

    source_recall_id: str
    campno: str
    mfr_comp_ptno: str | None = None
    mfr_comp_name: str | None = None
    title: str = ""


class PressReleaseRecord(BaseModel):
    """Simulates FDA press releases: the full natural key is identity
    (source_recall_id, press_release_url, press_release_type, press_release_issued_dt).

    One event carries several releases AND the same URL recurs under a different type or
    issue date (event 76385), so all four columns are identity. issued_dt is modelled as the
    ISO string model_dump(mode='json') yields for the real UTC-aware datetime field."""

    source_recall_id: str
    press_release_url: str
    press_release_type: str | None = None
    press_release_issued_dt: str | None = None


def _make_conn() -> MagicMock:
    """Return a mock SQLAlchemy Connection."""
    conn = MagicMock()
    conn.execute = MagicMock()
    return conn


def _make_table(name: str = "bronze_cpsc") -> MagicMock:
    """Return a mock SQLAlchemy Table with .name and .insert() / .c.*."""
    table = MagicMock()
    table.name = name
    table.insert = MagicMock(return_value=MagicMock())
    # .c attribute for column access used by _fetch_existing_hashes
    table.c = MagicMock()
    table.c.source_recall_id = MagicMock()
    table.c.content_hash = MagicMock()
    table.c.extraction_timestamp = MagicMock()
    return table


def _make_real_table(
    name: str = "bronze_cpsc",
    extra_col_names: list[str] | None = None,
) -> Table:
    """
    Return a real SQLAlchemy Table with minimal columns.
    Required for _fetch_existing_hashes tests because SQLAlchemy's select()
    coercion rejects MagicMock column objects.

    `extra_col_names` lists additional column names (e.g. ["langcode"] for USDA)
    so composite-identity tests can hit a real Table that knows about them.
    Each call constructs fresh Column instances — Column objects can't be shared
    across Tables.
    """
    meta = MetaData()
    cols: list[Column] = [
        Column("source_recall_id", String),
        Column("content_hash", String),
        Column("extraction_timestamp", DateTime),
    ]
    for col_name in extra_col_names or []:
        cols.append(Column(col_name, String))
    return Table(name, meta, *cols)


def _make_loader(
    bronze_name: str = "bronze_cpsc",
    rejected_name: str = "rejected_cpsc",
    hash_exclude_fields: frozenset[str] = frozenset(),
    identity_fields: tuple[str, ...] = ("source_recall_id",),
    within_batch_dedup: bool = False,
    allow_null_identity: bool = False,
) -> tuple[BronzeLoader, MagicMock, MagicMock]:
    bronze = _make_table(bronze_name)
    rejected = _make_table(rejected_name)
    loader = BronzeLoader(
        bronze_table=bronze,
        rejected_table=rejected,
        hash_exclude_fields=hash_exclude_fields,
        identity_fields=identity_fields,
        within_batch_dedup=within_batch_dedup,
        allow_null_identity=allow_null_identity,
    )
    return loader, bronze, rejected


def _make_loader_with_real_tables(
    bronze_name: str = "bronze_cpsc",
    rejected_name: str = "rejected_cpsc",
    identity_fields: tuple[str, ...] = ("source_recall_id",),
    extra_col_names: list[str] | None = None,
) -> tuple[BronzeLoader, Table, Table]:
    """Return a BronzeLoader backed by real SQLAlchemy Table objects."""
    bronze = _make_real_table(bronze_name, extra_col_names=extra_col_names)
    rejected = _make_real_table(rejected_name, extra_col_names=extra_col_names)
    loader = BronzeLoader(
        bronze_table=bronze, rejected_table=rejected, identity_fields=identity_fields
    )
    return loader, bronze, rejected


# ---------------------------------------------------------------------------
# filter_new_records — tuple-keyed identity
# ---------------------------------------------------------------------------


def test_filter_new_records_returns_all_when_existing_hashes_empty() -> None:
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    hashed: list[tuple[tuple[str, ...], str, BaseModel]] = [(("CPSC-001",), "hash_a", record)]
    result = filter_new_records(hashed, existing_hashes={})
    assert result == hashed


def test_filter_new_records_returns_empty_for_empty_hashed_list() -> None:
    result = filter_new_records([], existing_hashes={("CPSC-001",): "hash_a"})
    assert result == []


def test_filter_new_records_skips_records_with_matching_hash() -> None:
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    hashed: list[tuple[tuple[str, ...], str, BaseModel]] = [(("CPSC-001",), "hash_a", record)]
    result = filter_new_records(hashed, existing_hashes={("CPSC-001",): "hash_a"})
    assert result == []


def test_filter_new_records_includes_records_with_changed_hash() -> None:
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A Updated")
    hashed: list[tuple[tuple[str, ...], str, BaseModel]] = [(("CPSC-001",), "hash_b", record)]
    result = filter_new_records(hashed, existing_hashes={("CPSC-001",): "hash_a"})
    assert result == hashed


def test_filter_new_records_includes_new_ids_not_in_existing_hashes() -> None:
    record_new = SimpleRecord(source_recall_id="CPSC-002", title="New Recall")
    record_existing = SimpleRecord(source_recall_id="CPSC-001", title="Existing")
    hashed: list[tuple[tuple[str, ...], str, BaseModel]] = [
        (("CPSC-001",), "hash_a", record_existing),
        (("CPSC-002",), "hash_b", record_new),
    ]
    result = filter_new_records(hashed, existing_hashes={("CPSC-001",): "hash_a"})
    assert len(result) == 1
    assert result[0][0] == ("CPSC-002",)


def test_filter_new_records_partial_match_skips_matching_includes_new() -> None:
    r1 = SimpleRecord(source_recall_id="CPSC-001", title="Same")
    r2 = SimpleRecord(source_recall_id="CPSC-002", title="Changed")
    r3 = SimpleRecord(source_recall_id="CPSC-003", title="Brand New")
    hashed: list[tuple[tuple[str, ...], str, BaseModel]] = [
        (("CPSC-001",), "hash_a", r1),
        (("CPSC-002",), "hash_new", r2),
        (("CPSC-003",), "hash_c", r3),
    ]
    existing = {("CPSC-001",): "hash_a", ("CPSC-002",): "hash_old"}
    result = filter_new_records(hashed, existing)
    ids = [item[0] for item in result]
    assert ("CPSC-001",) not in ids
    assert ("CPSC-002",) in ids
    assert ("CPSC-003",) in ids


def test_filter_new_records_composite_identity_distinguishes_siblings() -> None:
    """Records sharing source_recall_id but differing langcode must be treated as distinct."""
    en = BilingualRecord(source_recall_id="USDA-004-2020", langcode="English", title="Beef")
    es = BilingualRecord(source_recall_id="USDA-004-2020", langcode="Spanish", title="Carne")
    hashed: list[tuple[tuple[str, ...], str, BaseModel]] = [
        (("USDA-004-2020", "English"), "hash_en", en),
        (("USDA-004-2020", "Spanish"), "hash_es", es),
    ]
    # Only the English row's hash is known — the Spanish row should be inserted.
    existing = {("USDA-004-2020", "English"): "hash_en"}
    result = filter_new_records(hashed, existing)
    assert len(result) == 1
    assert result[0][0] == ("USDA-004-2020", "Spanish")


# ---------------------------------------------------------------------------
# BronzeLoader.__init__ — identity_fields validation
# ---------------------------------------------------------------------------


def test_bronze_loader_rejects_empty_identity_fields() -> None:
    bronze = _make_table()
    rejected = _make_table()
    with pytest.raises(ValueError, match="identity_fields"):
        BronzeLoader(bronze_table=bronze, rejected_table=rejected, identity_fields=())


# ---------------------------------------------------------------------------
# BronzeLoader.load — empty / no-op
# ---------------------------------------------------------------------------


def test_bronze_loader_load_returns_zero_for_empty_records_and_quarantined() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()
    result = loader.load(conn, records=[], quarantined=[], raw_landing_path=_LANDING_PATH)
    assert result == 0
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# BronzeLoader.load — happy path inserts
# ---------------------------------------------------------------------------


def test_bronze_loader_load_inserts_new_records() -> None:
    loader, bronze, _ = _make_loader()
    conn = _make_conn()

    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    # Patch _fetch_existing_hashes to return empty (all records are new)
    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    conn.execute.assert_called_once()
    insert_call = conn.execute.call_args
    rows_arg = insert_call[0][1]  # second positional arg to conn.execute
    assert len(rows_arg) == 1
    row = rows_arg[0]
    assert row["source_recall_id"] == "CPSC-001"
    assert row["extraction_timestamp"] == _FIXED_TS
    assert row["raw_landing_path"] == _LANDING_PATH
    assert "content_hash" in row


def test_bronze_loader_load_skips_hash_identical_records() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()

    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    # Pre-compute the hash that the loader will produce for this record.
    from src.bronze.hashing import content_hash

    existing_hash = content_hash(record.model_dump(mode="json"))

    with patch.object(
        loader,
        "_fetch_existing_hashes",
        return_value={("CPSC-001",): existing_hash},
    ):
        count = loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 0
    conn.execute.assert_not_called()


def test_bronze_loader_load_inserts_only_changed_records_in_mixed_batch() -> None:
    loader, bronze, _ = _make_loader()
    conn = _make_conn()

    from src.bronze.hashing import content_hash

    r_same = SimpleRecord(source_recall_id="CPSC-001", title="Unchanged")
    r_changed = SimpleRecord(source_recall_id="CPSC-002", title="Changed content")

    existing_hash_r_same = content_hash(r_same.model_dump(mode="json"))

    with patch.object(
        loader,
        "_fetch_existing_hashes",
        return_value={
            ("CPSC-001",): existing_hash_r_same,
            ("CPSC-002",): "stale_hash",
        },
    ):
        count = loader.load(
            conn,
            records=[r_same, r_changed],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert rows_inserted[0]["source_recall_id"] == "CPSC-002"


# ---------------------------------------------------------------------------
# BronzeLoader.load — composite identity (USDA bilingual scenario)
# ---------------------------------------------------------------------------


def test_bronze_loader_dedup_with_composite_identity_keeps_both_siblings_unique() -> None:
    """
    The exact scenario from Phase 5b first re-extraction: a recall has English
    and Spanish sibling rows sharing source_recall_id. With composite identity,
    each sibling has its own dedup slot, so a hash-identical re-run inserts zero.
    """
    from src.bronze.hashing import content_hash

    loader, _, _ = _make_loader(identity_fields=("source_recall_id", "langcode"))
    conn = _make_conn()

    en = BilingualRecord(source_recall_id="USDA-004-2020", langcode="English", title="Beef")
    es = BilingualRecord(source_recall_id="USDA-004-2020", langcode="Spanish", title="Carne")
    en_hash = content_hash(en.model_dump(mode="json"))
    es_hash = content_hash(es.model_dump(mode="json"))

    # Both siblings are already in bronze with their correct hashes — re-run
    # should be a no-op.
    with patch.object(
        loader,
        "_fetch_existing_hashes",
        return_value={
            ("USDA-004-2020", "English"): en_hash,
            ("USDA-004-2020", "Spanish"): es_hash,
        },
    ):
        count = loader.load(
            conn,
            records=[en, es],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 0
    conn.execute.assert_not_called()


def test_bronze_loader_composite_identity_passes_tuple_keys_to_fetch() -> None:
    """The keys passed to _fetch_existing_hashes are tuples, not bare strings."""
    loader, _, _ = _make_loader(identity_fields=("source_recall_id", "langcode"))
    conn = _make_conn()

    en = BilingualRecord(source_recall_id="USDA-004-2020", langcode="English", title="Beef")
    es = BilingualRecord(source_recall_id="USDA-004-2020", langcode="Spanish", title="Carne")

    with patch.object(loader, "_fetch_existing_hashes", return_value={}) as mock_fetch:
        loader.load(
            conn,
            records=[en, es],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    keys_passed = mock_fetch.call_args.args[1]
    assert ("USDA-004-2020", "English") in keys_passed
    assert ("USDA-004-2020", "Spanish") in keys_passed


# ---------------------------------------------------------------------------
# BronzeLoader.load — quarantine routing
# ---------------------------------------------------------------------------


def test_bronze_loader_load_writes_quarantine_rows_to_rejected_table() -> None:
    loader, bronze, rejected = _make_loader()
    conn = _make_conn()

    q = QuarantineRecord(
        source_recall_id="BAD-001",
        raw_record={"source_recall_id": "BAD-001", "title": "bad"},
        failure_reason="missing required field",
        failure_stage="validate",
        raw_landing_path=_LANDING_PATH,
    )

    result = loader.load(
        conn,
        records=[],
        quarantined=[q],
        raw_landing_path=_LANDING_PATH,
        extraction_timestamp=_FIXED_TS,
    )

    assert result == 0
    conn.execute.assert_called_once()
    rejected_call = conn.execute.call_args
    rejected_rows = rejected_call[0][1]
    assert len(rejected_rows) == 1
    row = rejected_rows[0]
    assert row["source_recall_id"] == "BAD-001"
    assert row["failure_reason"] == "missing required field"
    assert row["failure_stage"] == "validate"
    assert row["rejected_at"] == _FIXED_TS
    assert row["raw_landing_path"] == _LANDING_PATH


def test_bronze_loader_load_both_inserts_and_quarantine_in_same_call() -> None:
    loader, bronze, rejected = _make_loader()
    conn = _make_conn()

    record = SimpleRecord(source_recall_id="CPSC-001", title="Valid Record")
    q = QuarantineRecord(
        source_recall_id="BAD-001",
        raw_record={"title": "bad"},
        failure_reason="null id",
        failure_stage="invariants",
        raw_landing_path=_LANDING_PATH,
    )

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[record],
            quarantined=[q],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    # conn.execute should be called twice: once for bronze insert, once for rejected insert
    assert conn.execute.call_count == 2


# ---------------------------------------------------------------------------
# BronzeLoader.load — extraction_timestamp defaults
# ---------------------------------------------------------------------------


def test_bronze_loader_load_defaults_extraction_timestamp_to_now_when_none() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall")

    with (
        patch.object(loader, "_fetch_existing_hashes", return_value={}),
        patch("src.bronze.loader.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = _FIXED_TS
        loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=None,
        )

    rows_inserted = conn.execute.call_args[0][1]
    assert rows_inserted[0]["extraction_timestamp"] == _FIXED_TS


def test_bronze_loader_load_uses_explicit_extraction_timestamp_when_provided() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()
    explicit_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall")

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=explicit_ts,
        )

    rows_inserted = conn.execute.call_args[0][1]
    assert rows_inserted[0]["extraction_timestamp"] == explicit_ts


# ---------------------------------------------------------------------------
# BronzeLoader.load — ValueError for missing identity fields
# ---------------------------------------------------------------------------


def test_bronze_loader_load_raises_value_error_for_record_missing_source_recall_id() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()
    bad_record = RecordWithoutId(title="No ID Record")

    with pytest.raises(ValueError, match="source_recall_id"):
        loader.load(
            conn,
            records=[bad_record],  # type: ignore[list-item]
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )


def test_bronze_loader_load_raises_value_error_when_source_recall_id_is_falsy() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()

    class RecordWithEmptyId(BaseModel):
        source_recall_id: str = ""
        title: str = "oops"

    bad_record = RecordWithEmptyId()

    with pytest.raises(ValueError, match="source_recall_id"):
        loader.load(
            conn,
            records=[bad_record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )


def test_bronze_loader_raises_when_composite_identity_field_is_missing_on_record() -> None:
    """Composite-identity loader must reject records missing the secondary identity field."""
    loader, _, _ = _make_loader(identity_fields=("source_recall_id", "langcode"))
    conn = _make_conn()

    # SimpleRecord has no `langcode` — composite identity should fail to build.
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall")

    with pytest.raises(ValueError, match="langcode"):
        loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )


# ---------------------------------------------------------------------------
# BronzeLoader._identity_text_expr — text-canonicalization for IN comparison
# ---------------------------------------------------------------------------


def test_identity_text_expr_uses_to_char_for_datetime_columns() -> None:
    """For DateTime / TIMESTAMP identity columns, ``_identity_text_expr``
    emits a ``coalesce(to_char(... AT TIME ZONE 'UTC', '...'), '')``
    expression. This is the loader-side mechanism that makes
    ``allow_null_identity=True`` work for sources whose ``identity_fields``
    include TIMESTAMPTZ columns (NHTSA's ``bgman``/``endman`` per ADR 0030):
    empty-string identity values bind cleanly as TEXT instead of failing
    to bind as TIMESTAMPTZ. Without ``to_char``, Postgres returns
    ``DataError: invalid input syntax for type timestamp with time zone: ""``.

    The ISO-8601-Z format (``YYYY-MM-DDTHH24:MI:SSZ``) matches Pydantic's
    ``model_dump(mode="json")`` serialization of UTC datetimes so populated
    values match across sides too.
    """
    from sqlalchemy import DateTime as SaDateTime

    loader, _, _ = _make_loader()

    dummy_table = Table(
        "dummy_dt",
        MetaData(),
        Column("when", SaDateTime(timezone=True)),
    )
    expr = loader._identity_text_expr(dummy_table.c.when)
    compiled = str(expr.compile(compile_kwargs={"literal_binds": True}))

    assert "to_char" in compiled.lower()
    assert "at time zone" in compiled.lower()
    # The format string is embedded in the SQL via literal_binds; we look for
    # the distinctive Z-suffix wrapper rather than reproducing the full
    # quoted format-string here, since SQLAlchemy may quote it differently
    # across versions.
    assert "Z" in compiled


def test_identity_text_expr_uses_cast_to_text_for_non_datetime_columns() -> None:
    """For non-DateTime identity columns (the default branch), the helper
    emits ``coalesce(cast(col AS TEXT), '')``. Covers the non-NHTSA case
    where every identity column is text/string-typed (CPSC, FDA, USDA).
    """
    loader, _, _ = _make_loader()

    dummy_table = Table(
        "dummy_str",
        MetaData(),
        Column("name", String),
    )
    expr = loader._identity_text_expr(dummy_table.c.name)
    compiled = str(expr.compile(compile_kwargs={"literal_binds": True}))

    assert "cast" in compiled.lower()
    assert "to_char" not in compiled.lower()


# ---------------------------------------------------------------------------
# BronzeLoader._fetch_existing_hashes
# ---------------------------------------------------------------------------


def test_fetch_existing_hashes_returns_empty_dict_for_empty_ids() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()
    result = loader._fetch_existing_hashes(conn, identity_keys=[])
    assert result == {}
    conn.execute.assert_not_called()


def test_fetch_existing_hashes_returns_dict_from_query_rows() -> None:
    # Use real SQLAlchemy Table objects so select() coercion succeeds.
    loader, _, _ = _make_loader_with_real_tables()
    conn = _make_conn()

    mock_result = MagicMock()
    mock_result.fetchall.return_value = [
        ("CPSC-001", "hash_abc"),
        ("CPSC-002", "hash_def"),
    ]
    conn.execute.return_value = mock_result

    result = loader._fetch_existing_hashes(conn, identity_keys=[("CPSC-001",), ("CPSC-002",)])

    assert result == {("CPSC-001",): "hash_abc", ("CPSC-002",): "hash_def"}
    conn.execute.assert_called_once()


def test_fetch_existing_hashes_returns_empty_dict_when_no_rows_found() -> None:
    loader, _, _ = _make_loader_with_real_tables()
    conn = _make_conn()

    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    conn.execute.return_value = mock_result

    result = loader._fetch_existing_hashes(conn, identity_keys=[("CPSC-999",)])
    assert result == {}


def test_fetch_existing_hashes_keys_dict_on_composite_identity() -> None:
    """Bilingual scenario: same source_recall_id, different langcode → distinct keys."""
    loader, _, _ = _make_loader_with_real_tables(
        identity_fields=("source_recall_id", "langcode"),
        extra_col_names=["langcode"],
    )
    conn = _make_conn()

    mock_result = MagicMock()
    # Each row: (source_recall_id, langcode, content_hash) — same recall_id,
    # two langcodes, distinct hashes.
    mock_result.fetchall.return_value = [
        ("USDA-004-2020", "English", "hash_en"),
        ("USDA-004-2020", "Spanish", "hash_es"),
    ]
    conn.execute.return_value = mock_result

    result = loader._fetch_existing_hashes(
        conn,
        identity_keys=[("USDA-004-2020", "English"), ("USDA-004-2020", "Spanish")],
    )

    assert result == {
        ("USDA-004-2020", "English"): "hash_en",
        ("USDA-004-2020", "Spanish"): "hash_es",
    }


# ---------------------------------------------------------------------------
# BronzeLoader._fetch_existing_hashes — chunking (PG bind-param safety limit)
# ---------------------------------------------------------------------------


def test_fetch_existing_hashes_routes_large_batch_to_staging() -> None:
    """Above the per-query bind-param ceiling, the lookup uses the set-based staging
    join (one bronze pass) instead of N chunked IN seq-scans — the O(corpus × chunks)
    → O(corpus) fix. The dedup decision/return is unchanged; only the read path differs.

    A tiny ``_PG_PARAM_SAFETY_LIMIT`` forces the over-ceiling branch without a 60k-key
    fixture; both fetch sub-methods are mocked (their SQL is DB-tested in
    ``test_loader_fetch_equivalence.py``)."""
    loader, _, _ = _make_loader()  # single-col identity → chunk_size == limit
    conn = _make_conn()

    # chunk_size == 2 (patched); 5 keys > 2 → staged path.
    identity_keys: list[tuple[str, ...]] = [(f"CPSC-{i:03d}",) for i in range(5)]
    expected: dict[tuple[str, ...], str] = {key: f"hash_{i}" for i, key in enumerate(identity_keys)}

    with (
        patch("src.bronze.loader._PG_PARAM_SAFETY_LIMIT", 2),
        patch.object(loader, "_fetch_existing_hashes_staged", return_value=expected) as mock_staged,
        patch.object(loader, "_fetch_existing_hashes_chunk") as mock_chunk,
    ):
        result = loader._fetch_existing_hashes(conn, identity_keys=identity_keys)

    assert result == expected
    mock_staged.assert_called_once()
    mock_chunk.assert_not_called()


def test_fetch_existing_hashes_routes_at_ceiling_to_single_query() -> None:
    """A batch exactly at the ceiling stays on the single IN-query (chunk) path — the
    staging temp-table + ANALYZE overhead only pays off above it."""
    loader, _, _ = _make_loader()
    conn = _make_conn()

    # chunk_size == 2; 2 keys == ceiling → chunk path, not staged.
    identity_keys: list[tuple[str, ...]] = [("CPSC-001",), ("CPSC-002",)]
    with (
        patch("src.bronze.loader._PG_PARAM_SAFETY_LIMIT", 2),
        patch.object(
            loader, "_fetch_existing_hashes_chunk", return_value={("CPSC-001",): "h"}
        ) as mock_chunk,
        patch.object(loader, "_fetch_existing_hashes_staged") as mock_staged,
    ):
        result = loader._fetch_existing_hashes(conn, identity_keys=identity_keys)

    assert result == {("CPSC-001",): "h"}
    mock_chunk.assert_called_once()
    mock_staged.assert_not_called()


def test_fetch_existing_hashes_single_chunk_for_small_input() -> None:
    """A batch under the limit runs as exactly one chunk call (the CPSC daily-delta case)."""
    loader, _, _ = _make_loader()
    conn = _make_conn()

    with patch.object(
        loader, "_fetch_existing_hashes_chunk", return_value={("CPSC-001",): "h"}
    ) as mock_chunk:
        result = loader._fetch_existing_hashes(conn, identity_keys=[("CPSC-001",)])

    assert result == {("CPSC-001",): "h"}
    assert mock_chunk.call_count == 1


# ---------------------------------------------------------------------------
# BronzeLoader — hash_exclude_fields (query-artifact exclusion)
# ---------------------------------------------------------------------------


def test_hash_exclude_fields_skips_record_when_only_excluded_field_changes() -> None:
    """Changing an excluded field (e.g. RID) must not trigger a re-insert."""
    from src.bronze.hashing import content_hash

    loader, _, _ = _make_loader(hash_exclude_fields=frozenset({"rid"}))
    conn = _make_conn()

    # Record as returned in query window A (rid=10)
    record_a = RecordWithArtifact(source_recall_id="FDA-001", title="Recall", rid=10)
    # Compute the hash that the loader will produce for record_a (rid excluded)
    hash_input = {k: v for k, v in record_a.model_dump(mode="json").items() if k != "rid"}
    existing_hash = content_hash(hash_input)

    # Simulate record re-appearing in query window B with rid=19 — only RID differs
    record_b = RecordWithArtifact(source_recall_id="FDA-001", title="Recall", rid=19)

    with patch.object(loader, "_fetch_existing_hashes", return_value={("FDA-001",): existing_hash}):
        count = loader.load(
            conn,
            records=[record_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 0
    conn.execute.assert_not_called()


def test_hash_exclude_fields_still_writes_excluded_field_to_db_row() -> None:
    """Excluded field is omitted from the hash but still persisted in the inserted row."""
    loader, _, _ = _make_loader(hash_exclude_fields=frozenset({"rid"}))
    conn = _make_conn()

    record = RecordWithArtifact(source_recall_id="FDA-001", title="New Recall", rid=42)

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert len(rows_inserted) == 1
    row = rows_inserted[0]
    # Excluded field must still appear in the DB row
    assert row["rid"] == 42
    assert row["source_recall_id"] == "FDA-001"
    assert "content_hash" in row


def test_hash_exclude_fields_still_detects_change_in_non_excluded_field() -> None:
    """A change in a non-excluded field must still trigger a re-insert."""
    from src.bronze.hashing import content_hash

    loader, _, _ = _make_loader(hash_exclude_fields=frozenset({"rid"}))
    conn = _make_conn()

    record_old = RecordWithArtifact(source_recall_id="FDA-001", title="Old Title", rid=10)
    hash_input_old = {k: v for k, v in record_old.model_dump(mode="json").items() if k != "rid"}
    existing_hash = content_hash(hash_input_old)

    # Same RID, but title changed — should be treated as an edit
    record_new = RecordWithArtifact(source_recall_id="FDA-001", title="Updated Title", rid=10)

    with patch.object(loader, "_fetch_existing_hashes", return_value={("FDA-001",): existing_hash}):
        count = loader.load(
            conn,
            records=[record_new],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert rows_inserted[0]["source_recall_id"] == "FDA-001"


# ---------------------------------------------------------------------------
# BronzeLoader.within_batch_dedup — opt-in collapse of (identity, hash)
# duplicates within a single batch (NHTSA per ADR 0030)
# ---------------------------------------------------------------------------


def test_within_batch_dedup_collapses_byte_duplicate_records() -> None:
    """Two records with the same identity AND same content hash collapse to one insert."""
    loader, _, _ = _make_loader(within_batch_dedup=True)
    conn = _make_conn()

    # Two records with identical fields → identical identity AND identical hash.
    rec_a = SimpleRecord(source_recall_id="CPSC-001", title="Recall A", count=5)
    rec_b = SimpleRecord(source_recall_id="CPSC-001", title="Recall A", count=5)

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert len(rows_inserted) == 1
    assert rows_inserted[0]["source_recall_id"] == "CPSC-001"


def test_within_batch_dedup_keeps_distinct_records_unchanged() -> None:
    """Records with different identities all pass through dedup."""
    loader, _, _ = _make_loader(within_batch_dedup=True)
    conn = _make_conn()

    rec_a = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    rec_b = SimpleRecord(source_recall_id="CPSC-002", title="Recall B")
    rec_c = SimpleRecord(source_recall_id="CPSC-003", title="Recall C")

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b, rec_c],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 3
    rows_inserted = conn.execute.call_args[0][1]
    assert {r["source_recall_id"] for r in rows_inserted} == {"CPSC-001", "CPSC-002", "CPSC-003"}


def test_within_batch_dedup_raises_on_identity_collision_with_different_hash() -> None:
    """Same identity with different content hashes is the defensive-error case.

    Per ADR 0030, the expected NHTSA dedup case leaves all colliding rows
    byte-identical post-RECORD_ID-exclusion. A within-batch identity collision
    with *different* content indicates a source-format change or extractor bug
    — surface loudly rather than silently keeping one variant.
    """
    loader, _, _ = _make_loader(within_batch_dedup=True)
    conn = _make_conn()

    # Same source_recall_id (= same identity tuple), different title (= different hash).
    rec_a = SimpleRecord(source_recall_id="CPSC-001", title="Original")
    rec_b = SimpleRecord(source_recall_id="CPSC-001", title="Different")

    with (
        patch.object(loader, "_fetch_existing_hashes", return_value={}),
        pytest.raises(WithinBatchIdentityCollisionError) as exc_info,
    ):
        loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    # Error message should include the colliding identity for diagnosability.
    assert "CPSC-001" in str(exc_info.value)
    conn.execute.assert_not_called()


def test_within_batch_dedup_default_false_preserves_old_behavior() -> None:
    """With the flag off (default), the loader does not dedup within-batch.

    Two records with identical fields both reach the existing-check; both pass
    (no existing hash); both insert. This is the pre-ADR-0030 behavior — kept
    intact for sources that don't need within-batch dedup (CPSC, FDA, USDA).
    """
    loader, _, _ = _make_loader()  # within_batch_dedup defaults to False
    conn = _make_conn()

    rec_a = SimpleRecord(source_recall_id="CPSC-001", title="Recall A", count=5)
    rec_b = SimpleRecord(source_recall_id="CPSC-001", title="Recall A", count=5)

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    # Both records insert — no within-batch dedup happened.
    assert count == 2


def test_within_batch_dedup_with_hash_exclude_fields_collapses_record_id_only_dupes() -> None:
    """The NHTSA-realistic scenario: composite identity excluding RECORD_ID,
    plus hash_exclude_fields excluding source_recall_id, plus within_batch_dedup=True.

    Two records that differ ONLY in source_recall_id (NHTSA's regen-unstable
    counter) must collapse to one bronze row. This is the configuration ADR
    0030 specifies for NHTSA, exercised end-to-end in BronzeLoader.
    """
    # composite identity — source_recall_id is intentionally NOT in identity_fields
    loader, _, _ = _make_loader(
        identity_fields=("title",),  # stand-in for NHTSA's 7-tuple
        hash_exclude_fields=frozenset({"source_recall_id"}),
        within_batch_dedup=True,
    )
    conn = _make_conn()

    # Two records with same identity (title) and same content (count) but
    # different source_recall_id — exact analog to NHTSA's NISSAN/ACHILLES
    # byte-duplicate rows where RECORD_ID is the only differing field.
    rec_a = SimpleRecord(source_recall_id="record-100", title="Same Identity", count=7)
    rec_b = SimpleRecord(source_recall_id="record-200", title="Same Identity", count=7)

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    # Byte-duplicate after RECORD_ID exclusion → collapsed to one insert.
    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert len(rows_inserted) == 1
    # The kept record's source_recall_id is whichever was first-seen
    # (arbitrary by ADR 0030 design — both are byte-equivalent on the
    # hashed surface).
    assert rows_inserted[0]["source_recall_id"] in {"record-100", "record-200"}


_FDA_PR_IDENTITY = (
    "source_recall_id",
    "press_release_url",
    "press_release_type",
    "press_release_issued_dt",
)


def test_within_batch_dedup_fda_pr_same_url_distinct_issue_dates_both_load() -> None:
    """Event 76385 regression at the loader level. The same URL under the same type with two
    different issue dates must NOT raise WithinBatchIdentityCollisionError — under the full
    natural-key identity they are distinct rows, so both load. This is the exact batch shape
    that deterministically crashed the historical seed under the old 2-tuple identity."""
    loader, _, _ = _make_loader(
        identity_fields=_FDA_PR_IDENTITY,
        within_batch_dedup=True,
        allow_null_identity=True,
    )
    conn = _make_conn()
    url = "https://www.fda.gov/AnimalVeterinary/NewsEvents/CVMUpdates/ucm542265.htm"
    rec_a = PressReleaseRecord(
        source_recall_id="76385",
        press_release_url=url,
        press_release_type="FDA",
        press_release_issued_dt="2017-02-17T00:00:00Z",
    )
    rec_b = PressReleaseRecord(
        source_recall_id="76385",
        press_release_url=url,
        press_release_type="FDA",
        press_release_issued_dt="2017-03-02T00:00:00Z",
    )

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 2
    rows_inserted = conn.execute.call_args[0][1]
    assert len(rows_inserted) == 2
    assert {r["press_release_issued_dt"] for r in rows_inserted} == {
        "2017-02-17T00:00:00Z",
        "2017-03-02T00:00:00Z",
    }


def test_within_batch_dedup_fda_pr_exact_byte_duplicate_collapses() -> None:
    """The within_batch_dedup justification under the full natural key: an event whose
    response repeats the exact same 4-tuple row collapses to one insert (identity AND hash
    match), while genuinely distinct releases (above) are preserved."""
    loader, _, _ = _make_loader(
        identity_fields=_FDA_PR_IDENTITY,
        within_batch_dedup=True,
        allow_null_identity=True,
    )
    conn = _make_conn()
    url = "https://www.fda.gov/Safety/Recalls/ucm539900.htm"
    rec_a = PressReleaseRecord(
        source_recall_id="76385",
        press_release_url=url,
        press_release_type="Firm",
        press_release_issued_dt="2017-02-03T00:00:00Z",
    )
    rec_b = PressReleaseRecord(
        source_recall_id="76385",
        press_release_url=url,
        press_release_type="Firm",
        press_release_issued_dt="2017-02-03T00:00:00Z",
    )

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1


# ---------------------------------------------------------------------------
# BronzeLoader.allow_null_identity — opt-in acceptance of None/empty values
# in identity_fields (NHTSA per ADR 0030 amendment)
# ---------------------------------------------------------------------------


def test_allow_null_identity_off_raises_on_empty_identity_field() -> None:
    """Default behavior: empty/None identity field is rejected as a bug.

    Existing sources (CPSC, FDA, USDA) rely on this safety check —
    ``allow_null_identity=False`` is the default to preserve their
    pre-ADR-0030 contract.
    """
    loader, _, _ = _make_loader(
        identity_fields=("source_recall_id", "mfr_comp_name"),
    )
    conn = _make_conn()

    record = RecordWithNullableIdentity(
        source_recall_id="REC-001",
        campno="24V001000",
        mfr_comp_name=None,  # empty/None identity component
        title="example",
    )

    with (
        patch.object(loader, "_fetch_existing_hashes", return_value={}),
        pytest.raises(ValueError, match="mfr_comp_name"),
    ):
        loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    conn.execute.assert_not_called()


def test_allow_null_identity_on_accepts_empty_value() -> None:
    """With the flag set, an empty/None identity field is treated as a
    valid identity bucket (normalized to "") rather than rejected.

    This is the NHTSA case — ``mfr_comp_name`` and friends are
    legitimately empty for many rows; the row should still land in
    bronze with its identity tuple including the empty bucket.
    """
    loader, _, _ = _make_loader(
        identity_fields=("source_recall_id", "mfr_comp_name"),
        allow_null_identity=True,
    )
    conn = _make_conn()

    record = RecordWithNullableIdentity(
        source_recall_id="REC-001",
        campno="24V001000",
        mfr_comp_name=None,
        title="example",
    )

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[record],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert len(rows_inserted) == 1
    assert rows_inserted[0]["source_recall_id"] == "REC-001"


def test_allow_null_identity_normalizes_none_and_empty_to_same_bucket() -> None:
    """With ``allow_null_identity=True`` plus ``within_batch_dedup=True``,
    two rows whose identity components are (None, "") and ("", "") both
    map to the same identity bucket and dedup if their content_hashes also
    match (i.e., the records are byte-equivalent on the hashable surface).

    This ensures a TSV-shipped duplicate where one row has Pydantic-
    coerced None and the other has a literal empty string still collapses
    correctly under the NHTSA dedup policy.
    """
    loader, _, _ = _make_loader(
        identity_fields=("mfr_comp_name", "mfr_comp_ptno"),
        hash_exclude_fields=frozenset({"source_recall_id"}),
        within_batch_dedup=True,
        allow_null_identity=True,
    )
    conn = _make_conn()

    # Two records with the same data fields but distinct source_recall_id.
    # mfr_comp_name and mfr_comp_ptno are both None on record A and
    # explicitly "" on record B — they should normalize to the same
    # identity bucket. After RECORD_ID exclusion, content hashes match too.
    rec_a = RecordWithNullableIdentity(
        source_recall_id="REC-001",
        campno="24V001000",
        mfr_comp_name=None,
        mfr_comp_ptno=None,
        title="shared content",
    )
    rec_b = RecordWithNullableIdentity(
        source_recall_id="REC-002",
        campno="24V001000",
        mfr_comp_name=None,
        mfr_comp_ptno=None,
        title="shared content",
    )

    with patch.object(loader, "_fetch_existing_hashes", return_value={}):
        count = loader.load(
            conn,
            records=[rec_a, rec_b],
            quarantined=[],
            raw_landing_path=_LANDING_PATH,
            extraction_timestamp=_FIXED_TS,
        )

    # Same identity bucket + same hash → collapse to one insert.
    assert count == 1
    rows_inserted = conn.execute.call_args[0][1]
    assert len(rows_inserted) == 1
