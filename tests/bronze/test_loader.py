from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, MetaData, String, Table

from src.bronze.loader import BronzeLoader, filter_new_records
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


class RecordWithoutId(BaseModel):
    title: str
    count: int = 0


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


def _make_real_table(name: str = "bronze_cpsc") -> Table:
    """
    Return a real SQLAlchemy Table with minimal columns.
    Required for _fetch_existing_hashes tests because SQLAlchemy's select()
    coercion rejects MagicMock column objects.
    """
    meta = MetaData()
    return Table(
        name,
        meta,
        Column("source_recall_id", String),
        Column("content_hash", String),
        Column("extraction_timestamp", DateTime),
    )


def _make_loader(
    bronze_name: str = "bronze_cpsc",
    rejected_name: str = "rejected_cpsc",
) -> tuple[BronzeLoader, MagicMock, MagicMock]:
    bronze = _make_table(bronze_name)
    rejected = _make_table(rejected_name)
    loader = BronzeLoader(bronze_table=bronze, rejected_table=rejected)
    return loader, bronze, rejected


def _make_loader_with_real_tables(
    bronze_name: str = "bronze_cpsc",
    rejected_name: str = "rejected_cpsc",
) -> tuple[BronzeLoader, Table, Table]:
    """Return a BronzeLoader backed by real SQLAlchemy Table objects."""
    bronze = _make_real_table(bronze_name)
    rejected = _make_real_table(rejected_name)
    loader = BronzeLoader(bronze_table=bronze, rejected_table=rejected)
    return loader, bronze, rejected


# ---------------------------------------------------------------------------
# filter_new_records
# ---------------------------------------------------------------------------


def test_filter_new_records_returns_all_when_existing_hashes_empty() -> None:
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    hashed: list[tuple[str, str, BaseModel]] = [("CPSC-001", "hash_a", record)]
    result = filter_new_records(hashed, existing_hashes={})
    assert result == hashed


def test_filter_new_records_returns_empty_for_empty_hashed_list() -> None:
    result = filter_new_records([], existing_hashes={"CPSC-001": "hash_a"})
    assert result == []


def test_filter_new_records_skips_records_with_matching_hash() -> None:
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A")
    hashed: list[tuple[str, str, BaseModel]] = [("CPSC-001", "hash_a", record)]
    result = filter_new_records(hashed, existing_hashes={"CPSC-001": "hash_a"})
    assert result == []


def test_filter_new_records_includes_records_with_changed_hash() -> None:
    record = SimpleRecord(source_recall_id="CPSC-001", title="Recall A Updated")
    hashed: list[tuple[str, str, BaseModel]] = [("CPSC-001", "hash_b", record)]
    # existing hash is "hash_a" — content has changed
    result = filter_new_records(hashed, existing_hashes={"CPSC-001": "hash_a"})
    assert result == hashed


def test_filter_new_records_includes_new_ids_not_in_existing_hashes() -> None:
    record_new = SimpleRecord(source_recall_id="CPSC-002", title="New Recall")
    record_existing = SimpleRecord(source_recall_id="CPSC-001", title="Existing")
    hashed: list[tuple[str, str, BaseModel]] = [
        ("CPSC-001", "hash_a", record_existing),
        ("CPSC-002", "hash_b", record_new),
    ]
    result = filter_new_records(hashed, existing_hashes={"CPSC-001": "hash_a"})
    assert len(result) == 1
    assert result[0][0] == "CPSC-002"


def test_filter_new_records_partial_match_skips_matching_includes_new() -> None:
    r1 = SimpleRecord(source_recall_id="CPSC-001", title="Same")
    r2 = SimpleRecord(source_recall_id="CPSC-002", title="Changed")
    r3 = SimpleRecord(source_recall_id="CPSC-003", title="Brand New")
    hashed: list[tuple[str, str, BaseModel]] = [
        ("CPSC-001", "hash_a", r1),
        ("CPSC-002", "hash_new", r2),
        ("CPSC-003", "hash_c", r3),
    ]
    existing = {"CPSC-001": "hash_a", "CPSC-002": "hash_old"}
    result = filter_new_records(hashed, existing)
    ids = [item[0] for item in result]
    assert "CPSC-001" not in ids
    assert "CPSC-002" in ids
    assert "CPSC-003" in ids


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

    with patch.object(loader, "_fetch_existing_hashes", return_value={"CPSC-001": existing_hash}):
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
        return_value={"CPSC-001": existing_hash_r_same, "CPSC-002": "stale_hash"},
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
# BronzeLoader.load — ValueError for missing source_recall_id
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


# ---------------------------------------------------------------------------
# BronzeLoader._fetch_existing_hashes
# ---------------------------------------------------------------------------


def test_fetch_existing_hashes_returns_empty_dict_for_empty_ids() -> None:
    loader, _, _ = _make_loader()
    conn = _make_conn()
    result = loader._fetch_existing_hashes(conn, source_recall_ids=[])
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

    result = loader._fetch_existing_hashes(conn, source_recall_ids=["CPSC-001", "CPSC-002"])

    assert result == {"CPSC-001": "hash_abc", "CPSC-002": "hash_def"}
    conn.execute.assert_called_once()


def test_fetch_existing_hashes_returns_empty_dict_when_no_rows_found() -> None:
    loader, _, _ = _make_loader_with_real_tables()
    conn = _make_conn()

    mock_result = MagicMock()
    mock_result.fetchall.return_value = []
    conn.execute.return_value = mock_result

    result = loader._fetch_existing_hashes(conn, source_recall_ids=["CPSC-999"])
    assert result == {}
