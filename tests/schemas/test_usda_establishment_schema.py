from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.usda_establishment import (
    UsdaFsisEstablishment,
    _coerce_false_to_text,
)

# ---------------------------------------------------------------------------
# Minimal valid row matching the live FSIS Establishment Listing shape
# ---------------------------------------------------------------------------

_REQUIRED: dict = {
    "establishment_id": "6163082",
    "establishment_name": "CS Beef Packers, LLC",
    "establishment_number": "M630",
    "address": "123 Main St",
    "city": "Kuna",
    "state": "ID",
    "zip": "83634",
    "LatestMPIActiveDate": "2026-04-27",
    "status_regulated_est": "",
    "activities": ["Meat Processing"],
    "dbas": [],
}


# ---------------------------------------------------------------------------
# Validator unit tests
# ---------------------------------------------------------------------------


class TestCoerceFalseToText:
    """The boolean ``False`` is coerced to the string ``"false"`` (ADR 0027 option 3)."""

    def test_literal_false_becomes_string_false(self) -> None:
        # Per ADR 0027 option 3 (decided 2026-05-01): JSON boolean ``false`` →
        # the literal string ``"false"`` (storage-forced because the bronze
        # column is TEXT and JSON booleans can't land there). Silver does
        # nullif(col, 'false') to surface as null.
        assert _coerce_false_to_text(False) == "false"

    def test_real_string_passes_through(self) -> None:
        assert _coerce_false_to_text("29.83, -95.47") == "29.83, -95.47"

    def test_empty_string_passes_through(self) -> None:
        # '' is NOT the missing sentinel here — only the literal boolean False is.
        # Keeping '' as-is preserves the distinction.
        assert _coerce_false_to_text("") == ""

    def test_zero_passes_through(self) -> None:
        # 0 is falsy but not the False singleton — must not be mistaken for missing.
        # (Hypothetical; real API doesn't return 0 here, but the coercer's
        # safety property is that only `is False` matches.)
        assert _coerce_false_to_text(0) == 0

    def test_none_passes_through(self) -> None:
        # If the API ever returns null directly, the coercer passes it through
        # and downstream strict-mode validation will reject it (intended quarantine).
        assert _coerce_false_to_text(None) is None


# ---------------------------------------------------------------------------
# Model integration tests
# ---------------------------------------------------------------------------


class TestUsdaFsisEstablishment:
    def test_required_only_validates(self) -> None:
        m = UsdaFsisEstablishment.model_validate(_REQUIRED)
        assert m.source_recall_id == "6163082"
        assert m.establishment_name == "CS Beef Packers, LLC"
        assert m.activities == ["Meat Processing"]
        assert m.dbas == []
        # Per ADR 0027: county/geolocation default to "" when the API omits them
        # (stayed as TEXT NOT NULL columns; default kicks in for absent keys).
        assert m.county == ""
        assert m.geolocation == ""
        assert m.latest_mpi_active_date == datetime(2026, 4, 27, tzinfo=UTC)

    def test_false_sentinel_coerced_for_geolocation(self) -> None:
        # Per ADR 0027 option 3: JSON `false` → string "false". Silver does
        # nullif(geolocation, 'false') to surface as null.
        m = UsdaFsisEstablishment.model_validate({**_REQUIRED, "geolocation": False})
        assert m.geolocation == "false"

    def test_false_sentinel_coerced_for_county(self) -> None:
        m = UsdaFsisEstablishment.model_validate({**_REQUIRED, "county": False})
        assert m.county == "false"

    def test_real_geolocation_string_preserved(self) -> None:
        m = UsdaFsisEstablishment.model_validate(
            {**_REQUIRED, "geolocation": "29.83860699, -95.47217297"}
        )
        assert m.geolocation == "29.83860699, -95.47217297"

    def test_activities_whitespace_preserved(self) -> None:
        # Per ADR 0027: ragged whitespace is preserved verbatim in bronze;
        # silver staging trims via jsonb_array_elements_text → trim → jsonb_agg.
        m = UsdaFsisEstablishment.model_validate(
            {**_REQUIRED, "activities": ["Meat Processing", " Poultry Processing"]}
        )
        assert m.activities == ["Meat Processing", " Poultry Processing"]

    def test_extra_field_rejected(self) -> None:
        # ADR 0014 strict + extra='forbid' contract.
        with pytest.raises(ValidationError):
            UsdaFsisEstablishment.model_validate({**_REQUIRED, "unexpected_field": "x"})

    def test_missing_required_field_rejected(self) -> None:
        bad = {k: v for k, v in _REQUIRED.items() if k != "establishment_name"}
        with pytest.raises(ValidationError):
            UsdaFsisEstablishment.model_validate(bad)

    def test_null_activities_quarantines(self) -> None:
        # null from the API should fail validation (strict-quarantine posture)
        # rather than coerce to an empty list.
        with pytest.raises(ValidationError):
            UsdaFsisEstablishment.model_validate({**_REQUIRED, "activities": None})

    def test_inactive_status_value(self) -> None:
        m = UsdaFsisEstablishment.model_validate({**_REQUIRED, "status_regulated_est": "Inactive"})
        assert m.status_regulated_est == "Inactive"

    def test_empty_string_preserved_on_optional_strs(self) -> None:
        # Per ADR 0027 (bronze keeps storage-forced transforms only): nullable
        # text fields preserve '' verbatim. Silver staging will normalize via
        # nullif(col, '') in stg_usda_fsis_establishments.sql (Phase 5b.2 Step 5).
        m = UsdaFsisEstablishment.model_validate(
            {
                **_REQUIRED,
                "phone": "",
                "duns_number": "",
                "fips_code": "",
                "size": "",
                "district": "",
                "circuit": "",
            }
        )
        assert m.phone == ""
        assert m.duns_number == ""
        assert m.fips_code == ""
        assert m.size == ""
        assert m.district == ""
        assert m.circuit == ""
