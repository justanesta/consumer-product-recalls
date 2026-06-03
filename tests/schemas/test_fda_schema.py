from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.fda import (
    FdaPressReleaseRecord,
    FdaRecord,
    _parse_fda_date,
    _to_int,
    _to_nullable_int,
    _to_str,
)

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
    # Phase 6a.5 capture-expansion fields (audit §7a SHIP)
    "CODEINFORMATION": "Lot 12345; Best by 2027-01",
    "FIRMCITYNAM": "Lake Havasu",
    "FIRMCOUNTRYNAM": "United States",
    "FIRMLINE1ADR": "1891 Industrial Blvd",
    "FIRMLINE2ADR": None,
    "FIRMPOSTALCD": "86403",
    "FIRMSTATECD": "AZ",
    "FIRMSTATEPRVNCNAM": "Arizona",
    "FIRMSURVIVINGNAM": "Acme Holdings Corp",
    "FIRMSURVIVINGFEI": "9876543",
    "POSTEDINTERNETDT": "05/07/2025",
}


# ---------------------------------------------------------------------------
# Tier-3 press-release rows (capture-expansion (b) PR)
# ---------------------------------------------------------------------------

_PR_ROW: dict = {
    "RECALLEVENTID": "98815",
    "PRESSRELEASEURL": "https://www.fda.gov/safety/recalls/example",
    "PRESSRELEASETYPE": "Firm",
    "PRESSRELEASEISSUEDT": "05/07/2025",
}


class TestFdaPressReleaseRecord:
    def test_happy_path(self) -> None:
        rec = FdaPressReleaseRecord.model_validate(_PR_ROW)
        # source_recall_id is the EVENT id (RECALLEVENTID), stored as str.
        assert rec.source_recall_id == "98815"
        assert rec.press_release_url == "https://www.fda.gov/safety/recalls/example"
        assert rec.press_release_type == "Firm"
        assert rec.press_release_issued_dt == datetime(2025, 5, 7, tzinfo=UTC)

    def test_recalleventid_int_coerced_to_str(self) -> None:
        rec = FdaPressReleaseRecord.model_validate({**_PR_ROW, "RECALLEVENTID": 98815})
        assert rec.source_recall_id == "98815"

    def test_null_issued_date(self) -> None:
        rec = FdaPressReleaseRecord.model_validate({**_PR_ROW, "PRESSRELEASEISSUEDT": None})
        assert rec.press_release_issued_dt is None

    def test_empty_issued_date_is_none(self) -> None:
        # FDA's dual null sentinel (finding J): '' → None, storage-forced (ADR 0027).
        rec = FdaPressReleaseRecord.model_validate({**_PR_ROW, "PRESSRELEASEISSUEDT": ""})
        assert rec.press_release_issued_dt is None

    def test_null_type_preserved(self) -> None:
        rec = FdaPressReleaseRecord.model_validate({**_PR_ROW, "PRESSRELEASETYPE": None})
        assert rec.press_release_type is None

    def test_extra_field_forbidden(self) -> None:
        # The JSON drift fence: a 5th column FDA adds quarantines the row (ADR 0014).
        with pytest.raises(ValidationError):
            FdaPressReleaseRecord.model_validate({**_PR_ROW, "UNEXPECTED": "x"})

    def test_missing_url_quarantines(self) -> None:
        row = {k: v for k, v in _PR_ROW.items() if k != "PRESSRELEASEURL"}
        with pytest.raises(ValidationError):
            FdaPressReleaseRecord.model_validate(row)

    def test_missing_event_id_quarantines(self) -> None:
        row = {k: v for k, v in _PR_ROW.items() if k != "RECALLEVENTID"}
        with pytest.raises(ValidationError):
            FdaPressReleaseRecord.model_validate(row)


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

    def test_capture_expansion_fields(self) -> None:
        """Phase 6a.5 §7a SHIP fields map via aliases; FEI coerces to int,
        postedinternetdt to datetime, and the sparse firm_line2_adr stays None."""
        record = FdaRecord.model_validate(_FULL_ROW)
        assert record.code_information == "Lot 12345; Best by 2027-01"
        assert record.firm_city_nam == "Lake Havasu"
        assert record.firm_country_nam == "United States"
        assert record.firm_line1_adr == "1891 Industrial Blvd"
        assert record.firm_line2_adr is None
        assert record.firm_postal_cd == "86403"
        assert record.firm_state_cd == "AZ"
        assert record.firm_state_prvnc_nam == "Arizona"
        assert record.firm_surviving_nam == "Acme Holdings Corp"
        assert record.firm_surviving_fei == 9876543
        assert record.posted_internet_dt == datetime(2025, 5, 7, tzinfo=UTC)

    def test_capture_expansion_fields_default_none_when_absent(self) -> None:
        """All §7a fields are optional — a row without them validates (the
        cassettes record 21-field responses, which must still replay cleanly)."""
        record = FdaRecord.model_validate(_REQUIRED)
        assert record.code_information is None
        assert record.firm_city_nam is None
        assert record.firm_surviving_fei is None
        assert record.posted_internet_dt is None

    def test_productid_as_int_coerced_to_str(self) -> None:
        row = {**_REQUIRED, "PRODUCTID": 219875}
        record = FdaRecord.model_validate(row)
        assert record.source_recall_id == "219875"

    def test_recalleventid_as_string_coerced_to_int(self) -> None:
        record = FdaRecord.model_validate(_REQUIRED)
        assert isinstance(record.recall_event_id, int)

    def test_empty_string_nullable_preserved(self) -> None:
        # Per ADR 0027 (bronze keeps storage-forced transforms only): nullable
        # text fields preserve the source's '' representation verbatim.
        # Silver staging normalizes via nullif(col, '') in stg_fda_recalls.sql.
        row = {**_REQUIRED, "RECALLNUM": "", "PHASETXT": ""}
        record = FdaRecord.model_validate(row)
        assert record.recall_num == ""
        assert record.phase_txt == ""

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

    def test_missing_event_lmd_defaults_to_none(self) -> None:
        # As of migration 0020 EVENTLMD is nullable (Finding H: un-edited records
        # have null EVENTLMD; the full-corpus seed surfaces them). Was previously
        # a required-field ValidationError.
        row = {k: v for k, v in _REQUIRED.items() if k != "EVENTLMD"}
        record = FdaRecord.model_validate(row)
        assert record.event_lmd is None

    def test_null_event_lmd_stays_none(self) -> None:
        row = {**_REQUIRED, "EVENTLMD": None}
        record = FdaRecord.model_validate(row)
        assert record.event_lmd is None

    def test_empty_string_event_lmd_becomes_none(self) -> None:
        # event_lmd is a date (TIMESTAMPTZ can't hold ''); storage-forced '' → None.
        row = {**_REQUIRED, "EVENTLMD": ""}
        record = FdaRecord.model_validate(row)
        assert record.event_lmd is None

    def test_invalid_date_format_raises(self) -> None:
        # A non-empty, wrongly-formatted date still raises (only None/'' map to None).
        row = {**_REQUIRED, "EVENTLMD": "2026-04-24"}  # wrong format
        with pytest.raises(ValidationError):
            FdaRecord.model_validate(row)

    def test_newly_nullable_core_str_fields_default_none_when_absent(self) -> None:
        # center_cd / product_type_short / firm_legal_nam became nullable (0020).
        row = {
            k: v
            for k, v in _REQUIRED.items()
            if k not in ("CENTERCD", "PRODUCTTYPESHORT", "FIRMLEGALNAM")
        }
        record = FdaRecord.model_validate(row)
        assert record.center_cd is None
        assert record.product_type_short is None
        assert record.firm_legal_nam is None

    def test_newly_nullable_core_str_fields_preserve_empty_string(self) -> None:
        # Per ADR 0027 the str fields preserve '' VERBATIM (NOT '' → None); silver
        # normalizes via nullif. Only the date field maps '' → None.
        row = {**_REQUIRED, "CENTERCD": "", "PRODUCTTYPESHORT": "", "FIRMLEGALNAM": ""}
        record = FdaRecord.model_validate(row)
        assert record.center_cd == ""
        assert record.product_type_short == ""
        assert record.firm_legal_nam == ""

    def test_model_dump_contains_source_recall_id_and_snake_case_keys(self) -> None:
        record = FdaRecord.model_validate(_FULL_ROW)
        dumped = record.model_dump(mode="json")
        assert dumped["source_recall_id"] == "219875"
        assert "recall_event_id" in dumped
        assert "event_lmd" in dumped
        assert "PRODUCTID" not in dumped
        assert "RECALLEVENTID" not in dumped
