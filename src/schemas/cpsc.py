from __future__ import annotations

from datetime import UTC, date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Shared strict config for all sub-models
_SUB = ConfigDict(extra="forbid", strict=True, populate_by_name=True)


class CpscProduct(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")
    description: str | None = Field(None, validation_alias="Description")
    model: str | None = Field(None, validation_alias="Model")
    type: str | None = Field(None, validation_alias="Type")
    category_id: str | None = Field(None, validation_alias="CategoryID")
    number_of_units: str | None = Field(None, validation_alias="NumberOfUnits")


class CpscManufacturer(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")
    company_id: str | None = Field(None, validation_alias="CompanyID")


class CpscRetailer(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")
    company_id: str | None = Field(None, validation_alias="CompanyID")


class CpscImporter(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")
    company_id: str | None = Field(None, validation_alias="CompanyID")


class CpscDistributor(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")
    company_id: str | None = Field(None, validation_alias="CompanyID")


class CpscManufacturerCountry(BaseModel):
    model_config = _SUB
    country: str = Field(validation_alias="Country")


class CpscProductUPC(BaseModel):
    model_config = _SUB
    upc: str = Field(validation_alias="UPC")


class CpscHazard(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")
    hazard_type: str | None = Field(None, validation_alias="HazardType")
    hazard_type_id: str | None = Field(None, validation_alias="HazardTypeID")


class CpscRemedy(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")


class CpscRemedyOption(BaseModel):
    model_config = _SUB
    option: str = Field(validation_alias="Option")


class CpscInConjunction(BaseModel):
    model_config = _SUB
    url: str = Field(validation_alias="URL")


class CpscImage(BaseModel):
    model_config = _SUB
    url: str = Field(validation_alias="URL")
    caption: str | None = Field(None, validation_alias="Caption")


class CpscInjury(BaseModel):
    model_config = _SUB
    name: str = Field(validation_alias="Name")


def _parse_cpsc_date(v: object) -> datetime:
    """
    CPSC returns dates in two formats depending on the endpoint/vintage:
      - "YYYY-MM-DD" (older records)
      - "YYYY-MM-DDTHH:MM:SS" (newer records, no timezone suffix)
    datetime.fromisoformat() handles both. We always attach UTC so downstream
    code gets a consistent timezone-aware value.
    """
    if isinstance(v, str):
        dt = datetime.fromisoformat(v)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
    if isinstance(v, date) and not isinstance(v, datetime):
        return datetime(v.year, v.month, v.day, tzinfo=UTC)
    return v  # type: ignore[return-value]


class CpscRecord(BaseModel):
    """
    Bronze-layer schema for CPSC recall records (ADR 0014).

    Field names are snake_case Python names; validation_alias maps from the API's
    PascalCase keys. extra='forbid' + strict=True catch schema drift loudly at
    ingest time. Required-by-default fields (no Optional, no default) catch the
    'old-name missing' side of field renames.
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # Required scalars — any rename surfaces as a validation error
    source_recall_id: str = Field(validation_alias="RecallNumber")
    recall_id: int = Field(validation_alias="RecallID")
    recall_date: datetime = Field(validation_alias="RecallDate")
    last_publish_date: datetime = Field(validation_alias="LastPublishDate")

    # Optional scalars — source documents these as potentially absent
    title: str | None = Field(None, validation_alias="Title")
    description: str | None = Field(None, validation_alias="Description")
    url: str | None = Field(None, validation_alias="URL")
    consumer_contact: str | None = Field(None, validation_alias="ConsumerContact")

    # Collections — default to empty list when source omits the key
    products: list[CpscProduct] = Field(default_factory=list, validation_alias="Products")
    manufacturers: list[CpscManufacturer] = Field(
        default_factory=list, validation_alias="Manufacturers"
    )
    retailers: list[CpscRetailer] = Field(default_factory=list, validation_alias="Retailers")
    importers: list[CpscImporter] = Field(default_factory=list, validation_alias="Importers")
    distributors: list[CpscDistributor] = Field(
        default_factory=list, validation_alias="Distributors"
    )
    manufacturer_countries: list[CpscManufacturerCountry] = Field(
        default_factory=list, validation_alias="ManufacturerCountries"
    )
    product_upcs: list[CpscProductUPC] = Field(default_factory=list, validation_alias="ProductUPCs")
    hazards: list[CpscHazard] = Field(default_factory=list, validation_alias="Hazards")
    remedies: list[CpscRemedy] = Field(default_factory=list, validation_alias="Remedies")
    remedy_options: list[CpscRemedyOption] = Field(
        default_factory=list, validation_alias="RemedyOptions"
    )
    in_conjunctions: list[CpscInConjunction] = Field(
        default_factory=list, validation_alias="Inconjunctions"
    )
    sold_at_label: str | None = Field(None, validation_alias="SoldAtLabel")
    images: list[CpscImage] = Field(default_factory=list, validation_alias="Images")
    injuries: list[CpscInjury] = Field(default_factory=list, validation_alias="Injuries")

    @field_validator("recall_date", "last_publish_date", mode="before")
    @classmethod
    def _coerce_date_string_to_utc_datetime(cls, v: object) -> object:
        return _parse_cpsc_date(v)
