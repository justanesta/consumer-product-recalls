from __future__ import annotations

from datetime import UTC, datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from src.bronze.hashing import _json_default, _strip_none, content_hash, normalize_float

# ---------------------------------------------------------------------------
# _strip_none
# ---------------------------------------------------------------------------


def test_strip_none_removes_none_values_from_flat_dict() -> None:
    result = _strip_none({"a": 1, "b": None, "c": "keep"})
    assert result == {"a": 1, "c": "keep"}


def test_strip_none_removes_none_values_recursively_from_nested_dict() -> None:
    result = _strip_none({"outer": {"inner_keep": 1, "inner_none": None}, "top_none": None})
    assert result == {"outer": {"inner_keep": 1}}


def test_strip_none_preserves_none_inside_list_elements() -> None:
    # Arrays are semantically ordered — None values inside lists must not be altered.
    result = _strip_none({"items": [1, None, 3]})
    assert result == {"items": [1, None, 3]}


def test_strip_none_recurses_into_dicts_nested_inside_lists() -> None:
    result = _strip_none({"items": [{"keep": 1, "drop": None}]})
    assert result == {"items": [{"keep": 1}]}


def test_strip_none_handles_empty_dict() -> None:
    assert _strip_none({}) == {}


def test_strip_none_handles_empty_list() -> None:
    assert _strip_none([]) == []


def test_strip_none_passes_through_non_dict_non_list_scalar() -> None:
    assert _strip_none(42) == 42
    assert _strip_none("hello") == "hello"
    assert _strip_none(None) is None


def test_strip_none_dict_with_all_none_values_returns_empty_dict() -> None:
    assert _strip_none({"a": None, "b": None}) == {}


# ---------------------------------------------------------------------------
# _json_default
# ---------------------------------------------------------------------------


def test_json_default_converts_utc_datetime_to_iso8601_microseconds() -> None:
    dt = datetime(2024, 3, 1, 12, 0, 0, 123456, tzinfo=UTC)
    result = _json_default(dt)
    assert result == "2024-03-01T12:00:00.123456+00:00"


def test_json_default_converts_non_utc_datetime_to_utc_iso8601() -> None:
    # A timezone-aware datetime in a non-UTC zone must be converted to UTC first.
    eastern = timezone(offset=__import__("datetime").timedelta(hours=-5))
    dt = datetime(2024, 6, 15, 8, 0, 0, 0, tzinfo=eastern)
    result = _json_default(dt)
    assert result == "2024-06-15T13:00:00.000000+00:00"


def test_json_default_includes_microsecond_precision_when_zero() -> None:
    dt = datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=UTC)
    result = _json_default(dt)
    assert result == "2024-01-01T00:00:00.000000+00:00"


def test_json_default_converts_decimal_to_str() -> None:
    result = _json_default(Decimal("3.14"))
    assert result == "3.14"


def test_json_default_converts_uuid_to_str() -> None:
    uid = UUID("12345678-1234-5678-1234-567812345678")
    result = _json_default(uid)
    assert result == "12345678-1234-5678-1234-567812345678"


def test_json_default_converts_arbitrary_object_to_str() -> None:
    class Custom:
        def __str__(self) -> str:
            return "custom_value"

    result = _json_default(Custom())
    assert result == "custom_value"


# ---------------------------------------------------------------------------
# content_hash — determinism and correctness
# ---------------------------------------------------------------------------


def test_content_hash_is_deterministic_for_same_input() -> None:
    record = {"source_recall_id": "CPSC-001", "title": "Test Recall", "count": 42}
    assert content_hash(record) == content_hash(record)


def test_content_hash_produces_different_hash_for_different_input() -> None:
    r1 = {"source_recall_id": "CPSC-001", "title": "Recall A"}
    r2 = {"source_recall_id": "CPSC-001", "title": "Recall B"}
    assert content_hash(r1) != content_hash(r2)


def test_content_hash_excludes_none_fields() -> None:
    # A record with None values should hash the same as without those keys.
    r_with_none = {"source_recall_id": "CPSC-001", "title": "Recall", "optional": None}
    r_without = {"source_recall_id": "CPSC-001", "title": "Recall"}
    assert content_hash(r_with_none) == content_hash(r_without)


def test_content_hash_is_key_order_independent() -> None:
    r1 = {"source_recall_id": "CPSC-001", "title": "Recall", "count": 42}
    r2 = {"count": 42, "title": "Recall", "source_recall_id": "CPSC-001"}
    assert content_hash(r1) == content_hash(r2)


def test_content_hash_preserves_none_in_list_positions() -> None:
    # None inside a list is NOT stripped — list order and content are semantically ordered.
    r_with_none_in_list = {"id": "X-1", "items": [1, None, 3]}
    r_without_none = {"id": "X-1", "items": [1, 3]}
    assert content_hash(r_with_none_in_list) != content_hash(r_without_none)


def test_content_hash_distinguishes_different_float_precision() -> None:
    # content_hash does NOT round — callers must pre-round. Different precision => different hash.
    r1 = {"source_recall_id": "NHTSA-001", "lat": 1.1234567}
    r2 = {"source_recall_id": "NHTSA-001", "lat": 1.1234568}
    assert content_hash(r1) != content_hash(r2)


def test_content_hash_preserves_utf8_characters() -> None:
    r1 = {"source_recall_id": "FDA-001", "title": "café"}
    r2 = {"source_recall_id": "FDA-001", "title": "cafe"}
    assert content_hash(r1) != content_hash(r2)


def test_content_hash_pinned_known_value_simple_record() -> None:
    # PINNED per ADR 0007 — changing this function invalidates all existing bronze hashes.
    # The expected value was computed by running the implementation and recording the output.
    record = {"source_recall_id": "CPSC-001", "title": "Test Recall", "count": 42}
    expected = "ddcf31d5864e6a9cb2ee497885fe59513a86ee77a8c0fe6f8a815888106e2407"
    assert content_hash(record) == expected


def test_content_hash_pinned_known_value_with_none_stripped() -> None:
    # None field is stripped before hashing — must match hash of record without that field.
    record = {"source_recall_id": "CPSC-001", "title": "Test Recall", "count": 42, "optional": None}
    expected = "ddcf31d5864e6a9cb2ee497885fe59513a86ee77a8c0fe6f8a815888106e2407"
    assert content_hash(record) == expected


def test_content_hash_pinned_known_value_nested_with_none_and_list() -> None:
    # Nested dict None stripped; None inside list preserved.
    record = {"id": "X-1", "nested": {"a": 1, "b": None}, "items": [1, None, 3]}
    expected = "26471774a7cb070756862c9294e7527ae8be3a5fc14b26796f0f4909bb8f9f48"
    assert content_hash(record) == expected


def test_content_hash_pinned_known_value_with_datetime() -> None:
    # Datetime is serialised via _json_default to UTC ISO-8601 microseconds.
    dt = datetime(2024, 3, 1, 12, 0, 0, 0, tzinfo=UTC)
    record = {"source_recall_id": "CPSC-002", "published_at": dt}
    expected = "6ce873304d42e2b67b0c2f3a9543d9548f2480bfee5507e84f06610b3d24f5f2"
    assert content_hash(record) == expected


def test_content_hash_returns_64_char_hex_string() -> None:
    # SHA-256 produces 32 bytes = 64 hex characters.
    h = content_hash({"source_recall_id": "X"})
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# normalize_float
# ---------------------------------------------------------------------------


def test_normalize_float_rounds_to_six_places_by_default() -> None:
    assert normalize_float(1.1234567) == pytest.approx(1.123457)


def test_normalize_float_respects_custom_places_parameter() -> None:
    assert normalize_float(3.141592653589793, places=2) == pytest.approx(3.14)


def test_normalize_float_no_op_when_value_already_at_precision() -> None:
    assert normalize_float(1.5, places=6) == pytest.approx(1.5)


def test_normalize_float_handles_zero() -> None:
    assert normalize_float(0.0) == 0.0


def test_normalize_float_handles_negative_value() -> None:
    assert normalize_float(-1.9999999, places=6) == pytest.approx(-2.0)
