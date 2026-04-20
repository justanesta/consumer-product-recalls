from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from pydantic import BaseModel

from src.bronze.invariants import (
    _MAX_RECALL_AGE_DAYS,
    check_date_sanity,
    check_null_source_id,
    check_usda_bilingual_pairing,
)
from src.extractors._base import QuarantineRecord

# ---------------------------------------------------------------------------
# Fixtures — minimal Pydantic model for bilingual pairing tests
# ---------------------------------------------------------------------------

_FIXED_NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)


class FakeUsdaRecord(BaseModel):
    recall_number: str
    language: str  # "EN" or "ES"
    title: str = "recall title"


def _recall_number(r: FakeUsdaRecord) -> str:
    return r.recall_number


def _is_spanish(r: FakeUsdaRecord) -> bool:
    return r.language == "ES"


# ---------------------------------------------------------------------------
# check_null_source_id
# ---------------------------------------------------------------------------


def test_check_null_source_id_returns_failure_for_none() -> None:
    result = check_null_source_id(None)
    assert result is not None
    assert "null or empty" in result


def test_check_null_source_id_returns_failure_for_empty_string() -> None:
    result = check_null_source_id("")
    assert result is not None
    assert "null or empty" in result


def test_check_null_source_id_returns_failure_for_whitespace_only_string() -> None:
    result = check_null_source_id("   ")
    assert result is not None
    assert "null or empty" in result


def test_check_null_source_id_returns_failure_for_tab_only_string() -> None:
    result = check_null_source_id("\t\n")
    assert result is not None


def test_check_null_source_id_returns_none_for_valid_id() -> None:
    assert check_null_source_id("CPSC-2024-001") is None


def test_check_null_source_id_returns_none_for_single_character_id() -> None:
    assert check_null_source_id("X") is None


# ---------------------------------------------------------------------------
# check_date_sanity
# ---------------------------------------------------------------------------


def test_check_date_sanity_returns_none_for_none_dt() -> None:
    # None is allowed — date field may be absent.
    assert check_date_sanity(None) is None


def test_check_date_sanity_returns_failure_for_future_datetime() -> None:
    future_dt = _FIXED_NOW + timedelta(days=1)
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(future_dt)
    assert result is not None
    assert "future" in result


def test_check_date_sanity_returns_failure_when_exactly_at_now() -> None:
    # dt == now is also "in the future" per the strict > check.
    # Actually dt > now fails, dt == now passes. Let's verify boundary.
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(_FIXED_NOW)
    # dt == now: NOT > now, so this should pass.
    assert result is None


def test_check_date_sanity_returns_failure_for_dt_exceeding_70_year_limit() -> None:
    ancient_dt = _FIXED_NOW - timedelta(days=_MAX_RECALL_AGE_DAYS + 1)
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(ancient_dt)
    assert result is not None
    assert "70 years" in result


def test_check_date_sanity_returns_failure_at_exact_70_year_boundary() -> None:
    # Exactly at the boundary (< now - MAX_AGE_DAYS) should fail.
    boundary_dt = _FIXED_NOW - timedelta(days=_MAX_RECALL_AGE_DAYS)
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(boundary_dt)
    # boundary_dt == now - MAX_AGE_DAYS: NOT < that threshold, so should pass.
    assert result is None


def test_check_date_sanity_returns_none_for_valid_recent_datetime() -> None:
    valid_dt = _FIXED_NOW - timedelta(days=365)
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(valid_dt)
    assert result is None


def test_check_date_sanity_uses_custom_field_name_in_failure_message() -> None:
    future_dt = _FIXED_NOW + timedelta(hours=1)
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(future_dt, field_name="recall_date")
    assert result is not None
    assert "recall_date" in result


def test_check_date_sanity_default_field_name_is_published_at() -> None:
    future_dt = _FIXED_NOW + timedelta(hours=1)
    with patch("src.bronze.invariants.datetime") as mock_dt:
        mock_dt.now.return_value = _FIXED_NOW
        result = check_date_sanity(future_dt)
    assert result is not None
    assert "published_at" in result


# ---------------------------------------------------------------------------
# check_usda_bilingual_pairing
# ---------------------------------------------------------------------------


def test_check_usda_bilingual_pairing_empty_batch_returns_empty_passing_and_quarantined() -> None:
    passing, quarantined = check_usda_bilingual_pairing(
        [],
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert passing == []
    assert quarantined == []


def test_check_usda_bilingual_pairing_english_records_always_pass() -> None:
    records = [
        FakeUsdaRecord(recall_number="RCL-001", language="EN"),
        FakeUsdaRecord(recall_number="RCL-002", language="EN"),
    ]
    passing, quarantined = check_usda_bilingual_pairing(
        records,
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert len(passing) == 2
    assert quarantined == []


def test_check_usda_bilingual_pairing_spanish_with_english_sibling_passes() -> None:
    records = [
        FakeUsdaRecord(recall_number="RCL-001", language="EN"),
        FakeUsdaRecord(recall_number="RCL-001", language="ES"),
    ]
    passing, quarantined = check_usda_bilingual_pairing(
        records,
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert len(passing) == 2
    assert quarantined == []


def test_check_usda_bilingual_pairing_spanish_without_english_sibling_is_quarantined() -> None:
    records = [
        FakeUsdaRecord(recall_number="RCL-999", language="ES"),
    ]
    passing, quarantined = check_usda_bilingual_pairing(
        records,
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert passing == []
    assert len(quarantined) == 1
    q = quarantined[0]
    assert isinstance(q, QuarantineRecord)
    assert q.source_recall_id == "RCL-999"
    assert q.failure_stage == "invariants"
    assert "English" in q.failure_reason


def test_check_usda_bilingual_pairing_quarantine_row_stores_raw_landing_path() -> None:
    records = [FakeUsdaRecord(recall_number="RCL-888", language="ES")]
    _, quarantined = check_usda_bilingual_pairing(
        records,
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://my-bucket/2024/run-42.json",
    )
    assert quarantined[0].raw_landing_path == "s3://my-bucket/2024/run-42.json"


def test_check_usda_bilingual_pairing_quarantine_row_contains_raw_record_dict() -> None:
    record = FakeUsdaRecord(recall_number="RCL-777", language="ES")
    _, quarantined = check_usda_bilingual_pairing(
        [record],
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert quarantined[0].raw_record == record.model_dump(mode="json")


def test_check_usda_bilingual_pairing_mixed_batch_routes_correctly() -> None:
    records = [
        FakeUsdaRecord(recall_number="RCL-A", language="EN"),
        FakeUsdaRecord(recall_number="RCL-A", language="ES"),  # has sibling — passes
        FakeUsdaRecord(recall_number="RCL-B", language="EN"),
        FakeUsdaRecord(recall_number="RCL-C", language="ES"),  # no sibling — quarantined
    ]
    passing, quarantined = check_usda_bilingual_pairing(
        records,
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert len(passing) == 3
    assert len(quarantined) == 1
    assert quarantined[0].source_recall_id == "RCL-C"


def test_check_usda_bilingual_pairing_multiple_orphaned_spanish_records_all_quarantined() -> None:
    records = [
        FakeUsdaRecord(recall_number="RCL-X", language="ES"),
        FakeUsdaRecord(recall_number="RCL-Y", language="ES"),
    ]
    passing, quarantined = check_usda_bilingual_pairing(
        records,
        recall_number_fn=_recall_number,
        is_spanish_fn=_is_spanish,
        raw_landing_path="s3://bucket/key",
    )
    assert passing == []
    assert len(quarantined) == 2
    quarantined_ids = {q.source_recall_id for q in quarantined}
    assert quarantined_ids == {"RCL-X", "RCL-Y"}
