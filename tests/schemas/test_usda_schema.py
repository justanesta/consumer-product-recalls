from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.usda import (
    UsdaFsisRecord,
    _normalize_str,
    _parse_nullable_usda_date,
    _parse_usda_date,
    _to_bool,
    _to_nullable_bool,
)

# ---------------------------------------------------------------------------
# Minimal valid row matching the live FSIS recall API shape
# ---------------------------------------------------------------------------

_REQUIRED: dict = {
    "field_recall_number": "004-2020",
    "langcode": "English",
    "field_title": "Sample recall title",
    "field_recall_date": "2020-05-15",
    "field_recall_type": "Active Recall",
    "field_recall_classification": "Class I",
    "field_archive_recall": "True",
    "field_has_spanish": "True",
    "field_active_notice": "False",
}

_FULL_ROW: dict = {
    **_REQUIRED,
    "field_last_modified_date": "2020-05-20",
    "field_closed_date": "",
    "field_related_to_outbreak": "False",
    "field_closed_year": "",
    "field_year": "2020",
    "field_risk_level": "High - Class I",
    "field_recall_reason": "Product Contamination",
    "field_processing": "Raw-Intact",
    "field_states": "California, Nevada",
    "field_establishment": "Acme Meats, LLC",
    "field_labels": "",
    "field_qty_recovered": "1,000 lbs",
    "field_summary": "<p>Summary HTML...</p>",
    "field_product_items": "Ground beef 1lb tubes",
    "field_distro_list": "",
    "field_media_contact": "",
    "field_company_media_contact": "press@acme.example.com",
    "field_recall_url": "http://www.fsis.usda.gov/recalls-alerts/sample-slug",
    "field_en_press_release": "",
    "field_press_release": "",
}


# ---------------------------------------------------------------------------
# Validator unit tests
# ---------------------------------------------------------------------------


class TestToBool:
    def test_true_string(self) -> None:
        assert _to_bool("True") is True

    def test_false_string(self) -> None:
        assert _to_bool("False") is False

    def test_native_bool(self) -> None:
        assert _to_bool(True) is True
        assert _to_bool(False) is False

    def test_lowercase_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_bool("true")

    def test_int_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_bool(1)

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_bool("")


class TestToNullableBool:
    def test_none_returns_none(self) -> None:
        assert _to_nullable_bool(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _to_nullable_bool("") is None

    def test_true_string(self) -> None:
        assert _to_nullable_bool("True") is True

    def test_false_string(self) -> None:
        assert _to_nullable_bool("False") is False


class TestNormalizeStr:
    def test_empty_string_to_none(self) -> None:
        assert _normalize_str("") is None

    def test_none_to_none(self) -> None:
        assert _normalize_str(None) is None

    def test_pass_through(self) -> None:
        assert _normalize_str("hello") == "hello"


class TestParseUsdaDate:
    def test_yyyy_mm_dd(self) -> None:
        assert _parse_usda_date("2020-05-15") == datetime(2020, 5, 15, tzinfo=UTC)

    def test_already_datetime_tz_aware(self) -> None:
        dt = datetime(2020, 5, 15, tzinfo=UTC)
        assert _parse_usda_date(dt) is dt

    def test_already_datetime_naive_gets_utc(self) -> None:
        dt = datetime(2020, 5, 15)
        result = _parse_usda_date(dt)
        assert result.tzinfo == UTC

    def test_mm_dd_yyyy_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_usda_date("05/15/2020")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_usda_date("")


class TestParseNullableUsdaDate:
    def test_empty_string_to_none(self) -> None:
        assert _parse_nullable_usda_date("") is None

    def test_none_to_none(self) -> None:
        assert _parse_nullable_usda_date(None) is None

    def test_valid_date(self) -> None:
        assert _parse_nullable_usda_date("2020-05-15") == datetime(2020, 5, 15, tzinfo=UTC)


# ---------------------------------------------------------------------------
# UsdaFsisRecord validation
# ---------------------------------------------------------------------------


class TestUsdaFsisRecord:
    def test_minimal_required_fields(self) -> None:
        record = UsdaFsisRecord.model_validate(_REQUIRED)
        assert record.source_recall_id == "004-2020"
        assert record.langcode == "English"
        assert record.recall_date == datetime(2020, 5, 15, tzinfo=UTC)
        assert record.archive_recall is True
        assert record.has_spanish is True
        assert record.active_notice is False

    def test_full_row(self) -> None:
        record = UsdaFsisRecord.model_validate(_FULL_ROW)
        assert record.last_modified_date == datetime(2020, 5, 20, tzinfo=UTC)
        assert record.closed_date is None
        assert record.related_to_outbreak is False
        assert record.recall_url == "http://www.fsis.usda.gov/recalls-alerts/sample-slug"

    def test_spanish_record(self) -> None:
        row = {**_REQUIRED, "langcode": "Spanish"}
        record = UsdaFsisRecord.model_validate(row)
        assert record.langcode == "Spanish"

    def test_invalid_langcode_raises(self) -> None:
        row = {**_REQUIRED, "langcode": "french"}
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_lowercase_langcode_raises(self) -> None:
        # Literal is case-sensitive — confirms strict-mode posture.
        row = {**_REQUIRED, "langcode": "english"}
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_empty_string_optional_str_becomes_none(self) -> None:
        row = {**_REQUIRED, "field_summary": "", "field_states": ""}
        record = UsdaFsisRecord.model_validate(row)
        assert record.summary is None
        assert record.states is None

    def test_empty_string_optional_date_becomes_none(self) -> None:
        row = {**_REQUIRED, "field_last_modified_date": "", "field_closed_date": ""}
        record = UsdaFsisRecord.model_validate(row)
        assert record.last_modified_date is None
        assert record.closed_date is None

    def test_empty_string_optional_bool_becomes_none(self) -> None:
        row = {**_REQUIRED, "field_related_to_outbreak": ""}
        record = UsdaFsisRecord.model_validate(row)
        assert record.related_to_outbreak is None

    def test_required_bool_with_empty_string_raises(self) -> None:
        row = {**_REQUIRED, "field_archive_recall": ""}
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_extra_field_raises(self) -> None:
        row = {**_REQUIRED, "field_unknown": "value"}
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_missing_required_field_raises(self) -> None:
        row = {k: v for k, v in _REQUIRED.items() if k != "field_recall_date"}
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_invalid_date_format_raises(self) -> None:
        row = {**_REQUIRED, "field_recall_date": "05/15/2020"}  # wrong format
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_lowercase_bool_raises(self) -> None:
        row = {**_REQUIRED, "field_archive_recall": "true"}
        with pytest.raises(ValidationError):
            UsdaFsisRecord.model_validate(row)

    def test_model_dump_uses_snake_case_keys(self) -> None:
        record = UsdaFsisRecord.model_validate(_FULL_ROW)
        dumped = record.model_dump(mode="json")
        assert dumped["source_recall_id"] == "004-2020"
        assert "recall_date" in dumped
        assert "last_modified_date" in dumped
        assert "field_recall_number" not in dumped
        assert "field_recall_date" not in dumped

    def test_dead_fields_kept_for_shape(self) -> None:
        # en_press_release / press_release are 100% / 99.9% empty per Finding C
        # — schema accepts them and normalizes empty string to None.
        record = UsdaFsisRecord.model_validate(_FULL_ROW)
        assert record.en_press_release is None
        assert record.press_release is None
