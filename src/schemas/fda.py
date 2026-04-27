from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field


def _to_int(v: Any) -> int:
    if isinstance(v, bool):
        raise ValueError(f"Cannot coerce bool {v!r} to int")
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v == int(v):
        return int(v)
    if isinstance(v, str) and v:
        return int(v)
    raise ValueError(f"Cannot coerce {v!r} to int")


def _to_nullable_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    return _to_int(v)


def _to_str(v: Any) -> str:
    """Accept string or numeric IDs from the API (finding J: all values may come as strings)."""
    if isinstance(v, str):
        return v
    if isinstance(v, int | float):
        return str(int(v))
    raise ValueError(f"Cannot coerce {v!r} to str")


def _parse_fda_date(v: Any) -> datetime:
    """Parse MM/DD/YYYY → UTC midnight datetime (finding H in api_observations.md)."""
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str) and v:
        return datetime.strptime(v, "%m/%d/%Y").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as FDA date — expected MM/DD/YYYY")


def _parse_nullable_fda_date(v: Any) -> datetime | None:
    """Normalize FDA's dual null sentinels (null and '') before date parsing (finding J)."""
    if v is None or v == "":
        return None
    return _parse_fda_date(v)


def _normalize_str(v: Any) -> str | None:
    """Normalize FDA's dual null sentinels: null → None, '' → None (finding J)."""
    if v is None or v == "":
        return None
    return v


# Annotated types used by FdaRecord fields — BeforeValidator runs before strict mode
# so string-to-int and string-to-datetime coercions happen before Pydantic type-checks.
_FdaInt = Annotated[int, BeforeValidator(_to_int)]
_FdaNullableInt = Annotated[int | None, BeforeValidator(_to_nullable_int)]
_FdaStrId = Annotated[str, BeforeValidator(_to_str)]
_FdaDate = Annotated[datetime, BeforeValidator(_parse_fda_date)]
_FdaNullableDate = Annotated[datetime | None, BeforeValidator(_parse_nullable_fda_date)]
_FdaNullableStr = Annotated[str | None, BeforeValidator(_normalize_str)]


class FdaRecord(BaseModel):
    """
    Bronze-layer schema for FDA iRES enforcement recall records (ADR 0014).

    Targets the bulk POST /recalls/ object-array response shape — RESULT is a list of
    dicts with UPPERCASE column-name keys (finding D in api_observations.md). The schema
    covers exactly the displaycolumns requested by FdaExtractor plus RID (auto-injected).

    Key validation behaviors:
    - RECALLEVENTID / RID / FIRMFEINUM come as strings; BeforeValidator coerces to int.
    - PRODUCTID may come as string or number; BeforeValidator normalizes to str.
    - Date fields use MM/DD/YYYY format (finding H); coerced to UTC midnight datetime.
    - Empty '' normalized to None on all Optional[str] fields — FDA uses both null and ''
      as null sentinels for the same fields across records (finding J).
    - strict=True + extra='forbid' catches schema drift at ingest (ADR 0014).
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # Core identifiers — non-nullable; validation failures quarantine the row
    source_recall_id: _FdaStrId = Field(validation_alias="PRODUCTID")
    recall_event_id: _FdaInt = Field(validation_alias="RECALLEVENTID")
    rid: _FdaInt = Field(validation_alias="RID")
    center_cd: str = Field(validation_alias="CENTERCD")
    product_type_short: str = Field(validation_alias="PRODUCTTYPESHORT")
    event_lmd: _FdaDate = Field(validation_alias="EVENTLMD")
    firm_legal_nam: str = Field(validation_alias="FIRMLEGALNAM")

    # Nullable scalars — null and '' both map to None (finding J / finding M-extension)
    firm_fei_num: _FdaNullableInt = Field(default=None, validation_alias="FIRMFEINUM")
    recall_num: _FdaNullableStr = Field(default=None, validation_alias="RECALLNUM")
    phase_txt: _FdaNullableStr = Field(default=None, validation_alias="PHASETXT")
    center_classification_type_txt: _FdaNullableStr = Field(
        default=None, validation_alias="CENTERCLASSIFICATIONTYPETXT"
    )
    recall_initiation_dt: _FdaNullableDate = Field(
        default=None, validation_alias="RECALLINITIATIONDT"
    )
    center_classification_dt: _FdaNullableDate = Field(
        default=None, validation_alias="CENTERCLASSIFICATIONDT"
    )
    termination_dt: _FdaNullableDate = Field(default=None, validation_alias="TERMINATIONDT")
    enforcement_report_dt: _FdaNullableDate = Field(
        default=None, validation_alias="ENFORCEMENTREPORTDT"
    )
    determination_dt: _FdaNullableDate = Field(default=None, validation_alias="DETERMINATIONDT")
    initial_firm_notification_txt: _FdaNullableStr = Field(
        default=None, validation_alias="INITIALFIRMNOTIFICATIONTXT"
    )
    distribution_area_summary_txt: _FdaNullableStr = Field(
        default=None, validation_alias="DISTRIBUTIONAREASUMMARYTXT"
    )
    voluntary_type_txt: _FdaNullableStr = Field(default=None, validation_alias="VOLUNTARYTYPETXT")
    product_description_txt: _FdaNullableStr = Field(
        default=None, validation_alias="PRODUCTDESCRIPTIONTXT"
    )
    product_short_reason_txt: _FdaNullableStr = Field(
        default=None, validation_alias="PRODUCTSHORTREASONTXT"
    )
    product_distributed_quantity: _FdaNullableStr = Field(
        default=None, validation_alias="PRODUCTDISTRIBUTEDQUANTITY"
    )
