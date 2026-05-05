from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.nhtsa import (
    NhtsaRecord,
    _parse_nhtsa_date,
    _parse_nullable_nhtsa_date,
    _to_bool,
    _to_nullable_bool,
)

# ---------------------------------------------------------------------------
# Minimal valid row matching the 29-field TSV shape per RCL.txt + Finding E.
# Keys are lowercase RCL.txt names; the schema's source_recall_id field
# absorbs `record_id` via validation_alias (mirrors USDA's
# field_recall_number → source_recall_id pattern).
# ---------------------------------------------------------------------------

_REQUIRED: dict = {
    "record_id": "200001",
    "campno": "23V123000",
    "maketxt": "DAMON",
    "modeltxt": "INTRUDER",
    "yeartxt": "2024",
    "compname": "EQUIPMENT:RV:LPG SYSTEM",
    "mfgname": "THOR MOTOR COACH",
    "rcltype": "V",
    "potaff": "1500",
    "mfgtxt": "THOR MOTOR COACH",
    "rcdate": "20240120",
    "desc_defect": "Sample defect description",
    "conequence_defect": "Sample consequence",
    "corrective_action": "Sample corrective action",
}

_FULL_ROW: dict = {
    **_REQUIRED,
    "mfgcampno": "RC000018",
    "bgman": "20230101",
    "endman": "20231231",
    "odate": "20240115",
    "influenced_by": "MFR",
    "datea": "20240122",
    "rpno": "23V-001",
    "fmvss": "208",
    "notes": "Owner outreach 2024-02-01",
    "rcl_cmpt_id": "000037237000216701000000332",
    "mfr_comp_name": "Acme Tank Co",
    "mfr_comp_desc": "LPG storage tank",
    "mfr_comp_ptno": "TANK-2024-A",
    "do_not_drive": "No",
    "park_outside": "No",
}


# ---------------------------------------------------------------------------
# Validator unit tests
# ---------------------------------------------------------------------------


class TestToBool:
    def test_yes_string(self) -> None:
        assert _to_bool("Yes") is True

    def test_no_string(self) -> None:
        assert _to_bool("No") is False

    def test_native_bool(self) -> None:
        assert _to_bool(True) is True
        assert _to_bool(False) is False

    def test_lowercase_raises(self) -> None:
        # NHTSA's casing is canonical "Yes" / "No" per Finding E; lowercase
        # would indicate upstream shape drift.
        with pytest.raises(ValueError):
            _to_bool("yes")

    def test_true_string_raises(self) -> None:
        # USDA uses "True" / "False" (different source); coercing those for
        # NHTSA would silently absorb a source mix-up.
        with pytest.raises(ValueError):
            _to_bool("True")

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

    def test_yes_string(self) -> None:
        assert _to_nullable_bool("Yes") is True

    def test_no_string(self) -> None:
        assert _to_nullable_bool("No") is False


class TestParseNhtsaDate:
    def test_yyyymmdd(self) -> None:
        assert _parse_nhtsa_date("20240120") == datetime(2024, 1, 20, tzinfo=UTC)

    def test_oldest_observed_recall_date(self) -> None:
        # RCDATE lower bound from Finding H Q2.
        assert _parse_nhtsa_date("19660119") == datetime(1966, 1, 19, tzinfo=UTC)

    def test_odate_sentinel_preserved(self) -> None:
        # ODATE 19010101 is an unknown-date sentinel per Finding H — bronze
        # preserves the literal datetime; silver staging maps to NULL.
        assert _parse_nhtsa_date("19010101") == datetime(1901, 1, 1, tzinfo=UTC)

    def test_already_datetime_tz_aware(self) -> None:
        dt = datetime(2024, 1, 20, tzinfo=UTC)
        assert _parse_nhtsa_date(dt) is dt

    def test_already_datetime_naive_gets_utc(self) -> None:
        dt = datetime(2024, 1, 20)
        result = _parse_nhtsa_date(dt)
        assert result.tzinfo == UTC

    def test_dashed_format_raises(self) -> None:
        # USDA's YYYY-MM-DD format would indicate upstream shape drift; raise.
        with pytest.raises(ValueError):
            _parse_nhtsa_date("2024-01-20")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_nhtsa_date("")


class TestParseNullableNhtsaDate:
    def test_none_returns_none(self) -> None:
        assert _parse_nullable_nhtsa_date(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_nullable_nhtsa_date("") is None

    def test_valid_passthrough(self) -> None:
        assert _parse_nullable_nhtsa_date("20240120") == datetime(2024, 1, 20, tzinfo=UTC)


# ---------------------------------------------------------------------------
# NhtsaRecord — full-shape validation
# ---------------------------------------------------------------------------


class TestNhtsaRecord:
    def test_required_only(self) -> None:
        """A row with just the required fields parses; nullable fields default to None."""
        record = NhtsaRecord.model_validate(_REQUIRED)
        assert record.source_recall_id == "200001"
        assert record.campno == "23V123000"
        assert record.rcdate == datetime(2024, 1, 20, tzinfo=UTC)
        assert record.bgman is None
        assert record.do_not_drive is None
        assert record.park_outside is None
        assert record.notes is None

    def test_full_row(self) -> None:
        record = NhtsaRecord.model_validate(_FULL_ROW)
        assert record.do_not_drive is False
        assert record.park_outside is False
        assert record.fmvss == "208"
        assert record.rcl_cmpt_id == "000037237000216701000000332"

    def test_source_recall_id_aliased_from_record_id(self) -> None:
        """Pydantic's validation_alias maps the dict key `record_id` →
        the schema attribute `source_recall_id`. The bronze loader's
        identity_fields=("source_recall_id",) then dedups on this value.
        """
        record = NhtsaRecord.model_validate(_REQUIRED)
        assert record.source_recall_id == _REQUIRED["record_id"]

    def test_extra_field_forbidden(self) -> None:
        """A 30th field (Finding F drift event) lands in quarantine via
        ValidationError — extra='forbid' per ADR 0014.
        """
        bad = {**_REQUIRED, "extra_new_column": "WHAT"}
        with pytest.raises(ValidationError):
            NhtsaRecord.model_validate(bad)

    def test_missing_required_field_raises(self) -> None:
        bad = {k: v for k, v in _REQUIRED.items() if k != "campno"}
        with pytest.raises(ValidationError):
            NhtsaRecord.model_validate(bad)

    def test_fmvss_max_length_enforced(self) -> None:
        # Per Finding F (May 2025 width reduction), FMVSS narrowed to CHAR(3).
        # A 4-char value would indicate upstream regression; reject.
        bad = {**_REQUIRED, "fmvss": "1234"}
        with pytest.raises(ValidationError):
            NhtsaRecord.model_validate(bad)

    def test_fmvss_three_chars_passes(self) -> None:
        ok = {**_REQUIRED, "fmvss": "208"}
        record = NhtsaRecord.model_validate(ok)
        assert record.fmvss == "208"

    def test_fmvss_empty_treated_as_none(self) -> None:
        # Empty string for an Optional[str]-shaped FMVSS — preserved as
        # empty string at bronze per ADR 0027 (silver does nullif).
        ok = {**_REQUIRED, "fmvss": ""}
        record = NhtsaRecord.model_validate(ok)
        # Length constraint is satisfied (0 <= 3); empty stays as-is.
        assert record.fmvss == ""

    def test_null_datea_accepted(self) -> None:
        # Finding H Q2: 5/81,714 PRE_2010 records have null DATEA.
        ok = {**_REQUIRED, "datea": ""}
        record = NhtsaRecord.model_validate(ok)
        assert record.datea is None

    def test_null_rcdate_accepted(self) -> None:
        # 2026-05-05 sentinel-date probe (Finding H follow-up): 5/81,714
        # PRE_2010 records have empty RCDATE — same cohort as the empty
        # DATEA records. Schema accepts null to avoid quarantining real
        # recall records.
        partial = {k: v for k, v in _REQUIRED.items() if k != "rcdate"}
        partial["rcdate"] = ""
        record = NhtsaRecord.model_validate(partial)
        assert record.rcdate is None

    def test_odate_sentinel_preserved_at_bronze(self) -> None:
        # ODATE 19010101 is preserved as 1901-01-01 at bronze; silver maps to NULL.
        ok = {**_REQUIRED, "odate": "19010101"}
        record = NhtsaRecord.model_validate(ok)
        assert record.odate == datetime(1901, 1, 1, tzinfo=UTC)

    def test_html_in_narrative_preserved(self) -> None:
        # Finding E: embedded HTML anchors in narrative fields preserved
        # verbatim per ADR 0027; silver staging strips/decodes.
        html = "GO TO <A HREF=HTTP://WWW.SAFERCAR.GOV>HTTP://WWW.SAFERCAR.GOV</A> ."
        ok = {**_REQUIRED, "desc_defect": html}
        record = NhtsaRecord.model_validate(ok)
        assert record.desc_defect == html
        assert "<A HREF=" in record.desc_defect

    def test_yes_no_booleans(self) -> None:
        ok = {**_REQUIRED, "do_not_drive": "Yes", "park_outside": "Yes"}
        record = NhtsaRecord.model_validate(ok)
        assert record.do_not_drive is True
        assert record.park_outside is True

    def test_pre_2007_record_passes(self) -> None:
        """Pre-2007 records lack NOTES/RCL_CMPT_ID/MFR_*/DO_NOT_DRIVE/PARK_OUTSIDE.
        The schema must absorb them without quarantine (Finding F nullability).
        """
        record = NhtsaRecord.model_validate(_REQUIRED)
        assert record.notes is None
        assert record.rcl_cmpt_id is None
        assert record.mfr_comp_name is None
        assert record.do_not_drive is None
        assert record.park_outside is None
