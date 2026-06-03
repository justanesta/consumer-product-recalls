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
    """Normalize FDA's dual null sentinels (null and '') before date parsing (finding J).

    Storage-forced (TIMESTAMPTZ NULL cannot hold the empty string), so '' → None
    stays in bronze per ADR 0027.
    """
    if v is None or v == "":
        return None
    return _parse_fda_date(v)


# Annotated types used by FdaRecord fields — BeforeValidator runs before strict mode
# so string-to-int and string-to-datetime coercions happen before Pydantic type-checks.
# Per ADR 0027, only storage-forced transforms live here. Empty-string-to-None
# normalization on Optional[str] fields moved to silver staging (nullif(col, '')).
_FdaInt = Annotated[int, BeforeValidator(_to_int)]
_FdaNullableInt = Annotated[int | None, BeforeValidator(_to_nullable_int)]
_FdaStrId = Annotated[str, BeforeValidator(_to_str)]
_FdaDate = Annotated[datetime, BeforeValidator(_parse_fda_date)]
_FdaNullableDate = Annotated[datetime | None, BeforeValidator(_parse_nullable_fda_date)]


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
    - Optional[str] fields preserve the source's null/'' representation verbatim.
      FDA uses both null and '' as null sentinels for the same fields across records
      (finding J); silver staging normalizes via nullif(col, '') per ADR 0027.
    - strict=True + extra='forbid' catches schema drift at ingest (ADR 0014).
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # Core identifiers — non-nullable; validation failures quarantine the row.
    # PRODUCTID is the identity key, RECALLEVENTID groups products in an event, and
    # RID is the API-auto-injected position counter — all three are always present.
    source_recall_id: _FdaStrId = Field(validation_alias="PRODUCTID")
    recall_event_id: _FdaInt = Field(validation_alias="RECALLEVENTID")
    rid: _FdaInt = Field(validation_alias="RID")

    # Formerly "core identifiers — non-nullable" (api_observations.md:374), now
    # nullable: the Phase 6a.5 full-corpus seed (filter:"[]") surfaces ~197 records
    # whose EVENTLMD is null (Finding H — the *lmd columns advance on edits only, so
    # un-edited records have null), which the prior eventlmdfrom-windowed extraction
    # never returned (a server-side >= comparison cannot match a null). The "core
    # never null" claim was an inference the windowing masked. Making these nullable
    # stops the no-window seed from silently quarantining those rows (migration 0020,
    # ADR 0014 permissive bronze). event_lmd uses _FdaNullableDate (storage-forced
    # '' → None per ADR 0027); the three str fields preserve '' verbatim (silver
    # normalizes via nullif). See project_scope/archive/fda-historical-seed-plan.md §0.1/§0.2.
    center_cd: str | None = Field(default=None, validation_alias="CENTERCD")
    product_type_short: str | None = Field(default=None, validation_alias="PRODUCTTYPESHORT")
    event_lmd: _FdaNullableDate = Field(default=None, validation_alias="EVENTLMD")
    firm_legal_nam: str | None = Field(default=None, validation_alias="FIRMLEGALNAM")

    # Nullable scalars — null and '' are preserved verbatim per ADR 0027
    # (silver staging normalizes via nullif(col, '')). Storage-forced exceptions:
    # firm_fei_num (INTEGER) and *_dt fields (TIMESTAMPTZ) cannot hold '' so
    # those validators still convert '' → None.
    firm_fei_num: _FdaNullableInt = Field(default=None, validation_alias="FIRMFEINUM")
    recall_num: str | None = Field(default=None, validation_alias="RECALLNUM")
    phase_txt: str | None = Field(default=None, validation_alias="PHASETXT")
    center_classification_type_txt: str | None = Field(
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
    initial_firm_notification_txt: str | None = Field(
        default=None, validation_alias="INITIALFIRMNOTIFICATIONTXT"
    )
    distribution_area_summary_txt: str | None = Field(
        default=None, validation_alias="DISTRIBUTIONAREASUMMARYTXT"
    )
    voluntary_type_txt: str | None = Field(default=None, validation_alias="VOLUNTARYTYPETXT")
    product_description_txt: str | None = Field(
        default=None, validation_alias="PRODUCTDESCRIPTIONTXT"
    )
    product_short_reason_txt: str | None = Field(
        default=None, validation_alias="PRODUCTSHORTREASONTXT"
    )
    product_distributed_quantity: str | None = Field(
        default=None, validation_alias="PRODUCTDISTRIBUTEDQUANTITY"
    )

    # Phase 6a.5 capture expansion (2026-05-31) — audit §7a SHIP fields. All
    # nullable: populations range from 0% (firm_line2_adr in the probe window)
    # through 15% (firm_surviving_*) to 100% (firm_city/country). codeinformation
    # is lot/serial free text — up to ~8.86M chars across full bronze (2026-06-03
    # probe; the earlier ~205k was a 100-record-window max). firm_surviving_fei is
    # an FEI (numeric, storage-forced to int like firm_fei_num). Bronze just lands
    # the bytes; silver mapping/naming for the firm/posted fields is the (b)
    # capture-expansion PR, while codeinformation's silver *parse* is deferred
    # further to project_scope/freetext-enrichment-backlog.md (post-6b).
    code_information: str | None = Field(default=None, validation_alias="CODEINFORMATION")
    firm_city_nam: str | None = Field(default=None, validation_alias="FIRMCITYNAM")
    firm_country_nam: str | None = Field(default=None, validation_alias="FIRMCOUNTRYNAM")
    firm_line1_adr: str | None = Field(default=None, validation_alias="FIRMLINE1ADR")
    firm_line2_adr: str | None = Field(default=None, validation_alias="FIRMLINE2ADR")
    firm_postal_cd: str | None = Field(default=None, validation_alias="FIRMPOSTALCD")
    firm_state_cd: str | None = Field(default=None, validation_alias="FIRMSTATECD")
    firm_state_prvnc_nam: str | None = Field(default=None, validation_alias="FIRMSTATEPRVNCNAM")
    firm_surviving_nam: str | None = Field(default=None, validation_alias="FIRMSURVIVINGNAM")
    firm_surviving_fei: _FdaNullableInt = Field(default=None, validation_alias="FIRMSURVIVINGFEI")
    posted_internet_dt: _FdaNullableDate = Field(default=None, validation_alias="POSTEDINTERNETDT")


class FdaPressReleaseRecord(BaseModel):
    """Bronze-layer schema for FDA press-release rows (Tier-3, capture-expansion (b) PR).

    Targets ``GET /search/pressreleaseurls/{eventid}`` — RESULT is a list of dicts with
    UPPERCASE keys (finding D), 4 columns per row. Press releases are **event-grain** and
    M:1 to the recall event, so ``source_recall_id`` here is RECALLEVENTID (the event),
    NOT PRODUCTID like ``FdaRecord``. The bronze identity is
    ``(source_recall_id, press_release_url)`` — one event can carry several releases.

    Not every event has a press release (the response may be 0 rows); the extractor treats
    an empty RESULT as a successful no-op, so a zero-PR event simply produces no rows here.
    ``strict=True`` + ``extra='forbid'`` is the JSON drift fence — an unexpected column
    (e.g. a 5th field FDA adds) quarantines the row (ADR 0014).
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # Event id (RECALLEVENTID) as TEXT — the work-list anchor + first identity field.
    # Stored as str (not int like FdaRecord.recall_event_id) to match the bronze
    # source_recall_id TEXT column and the (source_recall_id, press_release_url) identity.
    source_recall_id: _FdaStrId = Field(validation_alias="RECALLEVENTID")
    # Second identity field — required; a null URL is a meaningless PR row → quarantine.
    press_release_url: str = Field(validation_alias="PRESSRELEASEURL")
    # "State", "Firm", or "FDA" per the Definitions PDF. Nullable; silver nullifs ''.
    press_release_type: str | None = Field(default=None, validation_alias="PRESSRELEASETYPE")
    # MM/DD/YYYY → UTC midnight (finding H); '' / null → None (finding J), storage-forced.
    press_release_issued_dt: _FdaNullableDate = Field(
        default=None, validation_alias="PRESSRELEASEISSUEDT"
    )
