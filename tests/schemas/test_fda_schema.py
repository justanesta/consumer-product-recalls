from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.fda import FdaRecord, _parse_fda_date, _to_int, _to_nullable_int, _to_str

# ---------------------------------------------------------------------------
# Minimal valid row matching the bulk POST displaycolumns + RID
# ---------------------------------------------------------------------------

_REQUIRED: dict = {
    "PRODUCTID": "219875",
    "RECALLEVENTID": "98815",
    "RID": 1,
    "CENTERCD": "CFSAN",
    "PRODUCTTYPESHORT": "Food",
    "EVENTLMD": "04/24/2026",
    "FIRMLEGALNAM": "Acme Foods LLC",
}

_FULL_ROW: dict = {
    **_REQUIRED,
    "FIRMFEINUM": "1610287",
    "RECALLNUM": "F-0123-2026",
    "PHASETXT": "Ongoing",
    "CENTERCLASSIFICATIONTYPETXT": "1",
    "RECALLINITIATIONDT": "04/01/2026",
    "CENTERCLASSIFICATIONDT": "04/10/2026",
    "TERMINATIONDT": None,
    "ENFORCEMENTREPORTDT": None,
    "DETERMINATIONDT": None,
    "INITIALFIRMNOTIFICATIONTXT": "Letter",
    "DISTRIBUTIONAREASUMMARYTXT": "Nationwide",
    "VOLUNTARYTYPETXT": "Voluntary: Firm Initiated",
    "PRODUCTDESCRIPTIONTXT": "Contaminated crackers",
    "PRODUCTSHORTREASONTXT": "Salmonella contamination",
    "PRODUCTDISTRIBUTEDQUANTITY": "50,000 cases",
}


# ---------------------------------------------------------------------------
# Validator unit tests
# ---------------------------------------------------------------------------


class TestToInt:
    def test_string_int(self) -> None:
        assert _to_int("12345") == 12345

    def test_native_int(self) -> None:
        assert _to_int(98815) == 98815

    def test_float_whole(self) -> None:
        assert _to_int(1.0) == 1

    def test_bool_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_int(True)

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_int("")

    def test_none_raises(self) -> None:
        with pytest.raises((ValueError, TypeError)):
            _to_int(None)  # type: ignore[arg-type]


class TestToNullableInt:
    def test_none_returns_none(self) -> None:
        assert _to_nullable_int(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _to_nullable_int("") is None

    def test_string_int(self) -> None:
        assert _to_nullable_int("1610287") == 1610287

    def test_native_int(self) -> None:
        assert _to_nullable_int(42) == 42


class TestToStr:
    def test_string_passthrough(self) -> None:
        assert _to_str("219875") == "219875"

    def test_int_to_str(self) -> None:
        assert _to_str(219875) == "219875"

    def test_float_to_str(self) -> None:
        assert _to_str(219875.0) == "219875"

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_str(None)  # type: ignore[arg-type]


class TestParseFdaDate:
    def test_mm_dd_yyyy(self) -> None:
        result = _parse_fda_date("04/24/2026")
        assert result == datetime(2026, 4, 24, tzinfo=UTC)

    def test_already_datetime_tz_aware(self) -> None:
        dt = datetime(2026, 4, 24, tzinfo=UTC)
        assert _parse_fda_date(dt) is dt

    def test_already_datetime_naive_gets_utc(self) -> None:
        dt = datetime(2026, 4, 24)
        result = _parse_fda_date(dt)
        assert result.tzinfo == UTC

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_fda_date("2026-04-24")

    def test_none_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_fda_date(None)  # type: ignore[arg-type]

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_fda_date("")


# ---------------------------------------------------------------------------
# FdaRecord validation
# ---------------------------------------------------------------------------


class TestFdaRecord:
    def test_minimal_required_fields(self) -> None:
        record = FdaRecord.model_validate(_REQUIRED)
        assert record.source_recall_id == "219875"
        assert record.recall_event_id == 98815
        assert record.rid == 1
        assert record.event_lmd == datetime(2026, 4, 24, tzinfo=UTC)

    def test_full_row(self) -> None:
        record = FdaRecord.model_validate(_FULL_ROW)
        assert record.firm_fei_num == 1610287
        assert record.recall_num == "F-0123-2026"
        assert record.recall_initiation_dt == datetime(2026, 4, 1, tzinfo=UTC)

    def test_productid_as_int_coerced_to_str(self) -> None:
        row = {**_REQUIRED, "PRODUCTID": 219875}
        record = FdaRecord.model_validate(row)
        assert record.source_recall_id == "219875"

    def test_recalleventid_as_string_coerced_to_int(self) -> None:
        record = FdaRecord.model_validate(_REQUIRED)
        assert isinstance(record.recall_event_id, int)

    def test_empty_string_nullable_becomes_none(self) -> None:
        row = {**_REQUIRED, "RECALLNUM": "", "PHASETXT": ""}
        record = FdaRecord.model_validate(row)
        assert record.recall_num is None
        assert record.phase_txt is None

    def test_null_nullable_date_stays_none(self) -> None:
        row = {**_REQUIRED, "TERMINATIONDT": None}
        record = FdaRecord.model_validate(row)
        assert record.termination_dt is None

    def test_empty_string_date_becomes_none(self) -> None:
        row = {**_REQUIRED, "CENTERCLASSIFICATIONDT": ""}
        record = FdaRecord.model_validate(row)
        assert record.center_classification_dt is None

    def test_null_firmfeinum_stays_none(self) -> None:
        row = {**_REQUIRED, "FIRMFEINUM": None}
        record = FdaRecord.model_validate(row)
        assert record.firm_fei_num is None

    def test_empty_string_firmfeinum_becomes_none(self) -> None:
        row = {**_REQUIRED, "FIRMFEINUM": ""}
        record = FdaRecord.model_validate(row)
        assert record.firm_fei_num is None

    def test_extra_field_raises(self) -> None:
        row = {**_REQUIRED, "UNKNOWN_FIELD": "value"}
        with pytest.raises(ValidationError):
            FdaRecord.model_validate(row)

    def test_missing_required_field_raises(self) -> None:
        row = {k: v for k, v in _REQUIRED.items() if k != "EVENTLMD"}
        with pytest.raises(ValidationError):
            FdaRecord.model_validate(row)

    def test_invalid_date_format_raises(self) -> None:
        row = {**_REQUIRED, "EVENTLMD": "2026-04-24"}  # wrong format
        with pytest.raises(ValidationError):
            FdaRecord.model_validate(row)

    def test_model_dump_contains_source_recall_id_and_snake_case_keys(self) -> None:
        record = FdaRecord.model_validate(_FULL_ROW)
        dumped = record.model_dump(mode="json")
        assert dumped["source_recall_id"] == "219875"
        assert "recall_event_id" in dumped
        assert "event_lmd" in dumped
        assert "PRODUCTID" not in dumped
        assert "RECALLEVENTID" not in dumped
