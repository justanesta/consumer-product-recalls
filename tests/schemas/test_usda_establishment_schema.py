from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.usda_establishment import (
    UsdaFsisEstablishment,
    _normalize_false_sentinel,
    _strip_list_elements,
)

# ---------------------------------------------------------------------------
# Minimal valid row matching the live FSIS Establishment Listing shape
# ---------------------------------------------------------------------------

_REQUIRED: dict = {
    "establishment_id": "6163082",
    "establishment_name": "CS Beef Packers, LLC",
    "establishment_number": "M630",
    "address": "123 Main St",
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


class TestNormalizeFalseSentinel:
    """The boolean ``False`` is the missing-value sentinel for geolocation/county."""

    def test_literal_false_becomes_none(self) -> None:
        assert _normalize_false_sentinel(False) is None

    def test_real_string_passes_through(self) -> None:
        assert _normalize_false_sentinel("29.83, -95.47") == "29.83, -95.47"

    def test_empty_string_passes_through(self) -> None:
        # '' is NOT the missing sentinel here — only the literal boolean False is.
        # Keeping '' as-is preserves the distinction so silver can decide what to do.
        assert _normalize_false_sentinel("") == ""

    def test_zero_passes_through(self) -> None:
        # 0 is falsy but not the False singleton — must not be mistaken for missing.
        # (Hypothetical; real API doesn't return 0 here, but the validator's
        # safety property is that only `is False` matches.)
        assert _normalize_false_sentinel(0) == 0

    def test_none_passes_through(self) -> None:
        # If the API ever returns null directly, normalization happens upstream
        # via the field's `Optional` type, not this validator.
        assert _normalize_false_sentinel(None) is None  # passes through; field type allows it


class TestStripListElements:
    """activities/dbas may have leading whitespace on non-first elements."""

    def test_leading_space_stripped(self) -> None:
        assert _strip_list_elements(["Meat Processing", " Poultry Processing"]) == [
            "Meat Processing",
            "Poultry Processing",
        ]

    def test_empty_list_preserved(self) -> None:
        assert _strip_list_elements([]) == []

    def test_single_element_unchanged(self) -> None:
        assert _strip_list_elements(["Meat Processing"]) == ["Meat Processing"]

    def test_trailing_whitespace_also_stripped(self) -> None:
        assert _strip_list_elements(["Foo ", " Bar "]) == ["Foo", "Bar"]

    def test_none_raises_valueerror(self) -> None:
        # Per ADR 0013 strict-quarantine posture: null from the API should land in
        # the rejected table, not become an empty list silently. The explicit
        # ValueError (rather than a raw TypeError from iteration) is what
        # Pydantic wraps into ValidationError at the model boundary.
        with pytest.raises(ValueError, match="expected a list"):
            _strip_list_elements(None)


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
        assert m.county is None
        assert m.geolocation is None
        assert m.latest_mpi_active_date == datetime(2026, 4, 27, tzinfo=UTC)

    def test_false_sentinel_normalized_for_geolocation(self) -> None:
        m = UsdaFsisEstablishment.model_validate({**_REQUIRED, "geolocation": False})
        assert m.geolocation is None

    def test_false_sentinel_normalized_for_county(self) -> None:
        m = UsdaFsisEstablishment.model_validate({**_REQUIRED, "county": False})
        assert m.county is None

    def test_real_geolocation_string_preserved(self) -> None:
        m = UsdaFsisEstablishment.model_validate(
            {**_REQUIRED, "geolocation": "29.83860699, -95.47217297"}
        )
        assert m.geolocation == "29.83860699, -95.47217297"

    def test_activities_whitespace_stripped(self) -> None:
        m = UsdaFsisEstablishment.model_validate(
            {**_REQUIRED, "activities": ["Meat Processing", " Poultry Processing"]}
        )
        assert m.activities == ["Meat Processing", "Poultry Processing"]

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
