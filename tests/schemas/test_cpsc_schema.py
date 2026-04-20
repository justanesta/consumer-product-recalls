from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.schemas.cpsc import (
    CpscHazard,
    CpscImage,
    CpscInConjunction,
    CpscInjury,
    CpscManufacturer,
    CpscProduct,
    CpscRecord,
    CpscRemedy,
    CpscRemedyOption,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_REQUIRED_FIELDS: dict = {
    "RecallID": 24001,
    "RecallNumber": "24-001",
    "RecallDate": "2024-01-15",
    "LastPublishDate": "2024-01-15",
}

_FULL_RECORD: dict = {
    **_REQUIRED_FIELDS,
    "Title": "Widget Recall",
    "Description": "Widgets can pinch fingers.",
    "URL": "https://www.cpsc.gov/Recalls/2024/24-001",
    "ConsumerContact": "1-800-555-1234",
    "Products": [
        {
            "Name": "Widget Pro",
            "Description": "Blue widget",
            "Model": "WP-100",
            "Type": "Toy",
            "CategoryID": "1234",
            "NumberOfUnits": "50000",
        }
    ],
    "Manufacturers": [{"Name": "Widget Corp", "CompanyID": "W001"}],
    "Retailers": [{"Name": "Big Store", "CompanyID": "R001"}],
    "Importers": [{"Name": "Import Co", "CompanyID": "I001"}],
    "Distributors": [{"Name": "Dist Inc", "CompanyID": "D001"}],
    "ManufacturerCountries": [{"Country": "China"}],
    "ProductUPCs": [{"UPC": "012345678901"}],
    "Hazards": [{"Name": "Pinch hazard", "HazardType": "Physical", "HazardTypeID": "42"}],
    "Remedies": [{"Name": "Refund"}],
    "RemedyOptions": [{"Option": "Refund"}],
    "InConjunctions": [{"URL": "https://other.gov"}],
    "Images": [{"URL": "https://cdn.cpsc.gov/img.jpg"}],
    "Injuries": [{"Name": "Finger laceration"}],
}


# ---------------------------------------------------------------------------
# CpscRecord — happy path
# ---------------------------------------------------------------------------


def test_valid_full_record_parses() -> None:
    record = CpscRecord.model_validate(_FULL_RECORD)
    assert record.source_recall_id == "24-001"
    assert record.recall_id == 24001
    assert len(record.products) == 1
    assert record.products[0].name == "Widget Pro"
    assert len(record.hazards) == 1
    assert record.hazards[0].name == "Pinch hazard"


def test_valid_minimal_record_parses() -> None:
    record = CpscRecord.model_validate(_REQUIRED_FIELDS)
    assert record.source_recall_id == "24-001"
    assert record.title is None
    assert record.products == []
    assert record.manufacturers == []


# ---------------------------------------------------------------------------
# strict=True — type coercion is rejected
# ---------------------------------------------------------------------------


def test_recall_id_rejects_string() -> None:
    with pytest.raises(ValidationError, match="RecallID"):
        CpscRecord.model_validate({**_REQUIRED_FIELDS, "RecallID": "not-an-int"})


def test_recall_id_rejects_float() -> None:
    with pytest.raises(ValidationError, match="RecallID"):
        CpscRecord.model_validate({**_REQUIRED_FIELDS, "RecallID": 24001.0})


# ---------------------------------------------------------------------------
# extra='forbid' — unknown fields surface loudly (ADR 0014)
# ---------------------------------------------------------------------------


def test_unknown_top_level_field_raises() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CpscRecord.model_validate({**_REQUIRED_FIELDS, "NewUnknownField": "surprise"})


def test_unknown_product_field_raises() -> None:
    with pytest.raises(ValidationError):
        CpscRecord.model_validate(
            {
                **_REQUIRED_FIELDS,
                "Products": [{"Name": "Widget", "UnknownProductField": "surprise"}],
            }
        )


# ---------------------------------------------------------------------------
# Required-by-default fields — catches renames (ADR 0014)
# ---------------------------------------------------------------------------


def test_missing_recall_number_raises() -> None:
    data = {k: v for k, v in _REQUIRED_FIELDS.items() if k != "RecallNumber"}
    with pytest.raises(ValidationError, match="RecallNumber"):
        CpscRecord.model_validate(data)


def test_missing_recall_id_raises() -> None:
    data = {k: v for k, v in _REQUIRED_FIELDS.items() if k != "RecallID"}
    with pytest.raises(ValidationError, match="RecallID"):
        CpscRecord.model_validate(data)


def test_missing_recall_date_raises() -> None:
    data = {k: v for k, v in _REQUIRED_FIELDS.items() if k != "RecallDate"}
    with pytest.raises(ValidationError, match="RecallDate"):
        CpscRecord.model_validate(data)


def test_missing_last_publish_date_raises() -> None:
    data = {k: v for k, v in _REQUIRED_FIELDS.items() if k != "LastPublishDate"}
    with pytest.raises(ValidationError, match="LastPublishDate"):
        CpscRecord.model_validate(data)


# ---------------------------------------------------------------------------
# Date string → UTC datetime coercion
# ---------------------------------------------------------------------------


def test_recall_date_string_coerced_to_utc_datetime() -> None:
    record = CpscRecord.model_validate(_REQUIRED_FIELDS)
    assert isinstance(record.recall_date, datetime)
    assert record.recall_date.tzinfo == UTC
    assert record.recall_date.year == 2024
    assert record.recall_date.month == 1
    assert record.recall_date.day == 15


def test_last_publish_date_string_coerced_to_utc_datetime() -> None:
    record = CpscRecord.model_validate(_REQUIRED_FIELDS)
    assert isinstance(record.last_publish_date, datetime)
    assert record.last_publish_date.tzinfo == UTC


def test_datetime_object_passes_through() -> None:
    dt = datetime(2024, 1, 15, tzinfo=UTC)
    record = CpscRecord.model_validate({**_REQUIRED_FIELDS, "RecallDate": dt})
    assert record.recall_date == dt


# ---------------------------------------------------------------------------
# Optional / nullable scalars
# ---------------------------------------------------------------------------


def test_optional_scalars_default_to_none() -> None:
    record = CpscRecord.model_validate(_REQUIRED_FIELDS)
    assert record.title is None
    assert record.description is None
    assert record.url is None
    assert record.consumer_contact is None


def test_optional_collections_default_to_empty_list() -> None:
    record = CpscRecord.model_validate(_REQUIRED_FIELDS)
    assert record.products == []
    assert record.manufacturers == []
    assert record.hazards == []
    assert record.injuries == []


# ---------------------------------------------------------------------------
# model_dump — BronzeLoader expects source_recall_id in output (ADR 0007)
# ---------------------------------------------------------------------------


def test_model_dump_contains_source_recall_id() -> None:
    record = CpscRecord.model_validate(_REQUIRED_FIELDS)
    dumped = record.model_dump(mode="json")
    assert dumped["source_recall_id"] == "24-001"


def test_model_dump_uses_snake_case_keys() -> None:
    record = CpscRecord.model_validate(_FULL_RECORD)
    dumped = record.model_dump(mode="json")
    assert "recall_id" in dumped
    assert "last_publish_date" in dumped
    assert "manufacturer_countries" in dumped
    # API's PascalCase keys should NOT appear in the dump
    assert "RecallID" not in dumped
    assert "LastPublishDate" not in dumped


# ---------------------------------------------------------------------------
# Sub-model extra='forbid'
# ---------------------------------------------------------------------------


def test_cpsc_product_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        CpscProduct.model_validate({"Name": "Widget", "ExtraField": "oops"})


def test_cpsc_manufacturer_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        CpscManufacturer.model_validate({"Name": "Corp", "ExtraField": "oops"})


def test_cpsc_hazard_parses_optional_fields() -> None:
    hazard = CpscHazard.model_validate({"Name": "Fire"})
    assert hazard.name == "Fire"
    assert hazard.hazard_type is None
    assert hazard.hazard_type_id is None


def test_cpsc_remedy_option_parses() -> None:
    opt = CpscRemedyOption.model_validate({"Option": "Refund"})
    assert opt.option == "Refund"


def test_cpsc_in_conjunction_parses() -> None:
    conj = CpscInConjunction.model_validate({"URL": "https://other.gov"})
    assert conj.url == "https://other.gov"


def test_cpsc_image_parses() -> None:
    img = CpscImage.model_validate({"URL": "https://cdn.cpsc.gov/img.jpg"})
    assert img.url == "https://cdn.cpsc.gov/img.jpg"


def test_cpsc_injury_parses() -> None:
    injury = CpscInjury.model_validate({"Name": "Laceration"})
    assert injury.name == "Laceration"


def test_cpsc_remedy_parses() -> None:
    remedy = CpscRemedy.model_validate({"Name": "Refund"})
    assert remedy.name == "Refund"
