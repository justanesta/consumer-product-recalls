from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from src.schemas.usda import _parse_nullable_usda_date, _parse_usda_date


def _coerce_false_to_text(v: Any) -> Any:
    """Coerce the literal boolean ``False`` to the string ``"false"``.

    USDA FSIS Establishment Listing API uses ``false`` (JSON boolean) as a
    missing sentinel for `geolocation` and `county` (Finding C in
    documentation/usda/establishment_api_observations.md) — *not* ``null``,
    *not* ``""``. Bronze stores these as TEXT, so the JSON boolean must be
    coerced to *something* — the destination column type forces a choice.

    Per ADR 0027 §"Storage-type choice for the boolean-false sentinel case"
    (decided option 3, 2026-05-01), we coerce to the literal string
    ``"false"`` rather than ``None``. This preserves the source's signal
    cheaply (silver does ``nullif(col, 'false')``) and lets us detect a
    future API shift from ``false`` to ``null`` via
    ``count(*) where geolocation = 'false'``. Pass-through behavior for
    strings is unchanged.

    The strict identity check ``v is False`` matches only the singleton
    boolean and not other falsy values like ``""`` or ``0``, which might
    legitimately appear in other contexts.
    """
    if v is False:
        return "false"
    return v


# Annotated types — BeforeValidator runs before strict-mode type checks so the
# raw boolean false reaches our coercer first. Per ADR 0027, only storage-forced
# transforms live here. Empty-string normalization on Optional[str] fields and
# whitespace-strip on list[str] fields moved to silver staging.
_FsisFalseAsTextStr = Annotated[str, BeforeValidator(_coerce_false_to_text)]
_UsdaDate = Annotated[datetime, BeforeValidator(_parse_usda_date)]
_UsdaNullableDate = Annotated[datetime | None, BeforeValidator(_parse_nullable_usda_date)]


class UsdaFsisEstablishment(BaseModel):
    """Bronze-layer schema for USDA FSIS Establishment Listing records (ADR 0014).

    Targets the GET /fsis/api/establishments/v/1 flat-array response. Every
    extraction run is a full 7,945-record dump (no pagination, no ETag, no
    incremental cursor — see Findings A–G in
    documentation/usda/establishment_api_observations.md).

    Validation behaviors specific to this source:
    - `geolocation` and `county` use boolean ``false`` as a missing sentinel
      (Finding C); coerced to the literal string ``"false"`` by
      ``_coerce_false_to_text`` per ADR 0027 option 3. Silver staging does
      ``nullif(col, 'false')``.
    - `activities` and `dbas` are true JSON arrays; ragged leading-whitespace
      elements are preserved verbatim per ADR 0027 (silver staging trims via
      ``jsonb_array_elements_text → trim → jsonb_agg``).
    - Optional[str] fields preserve the source's '' representation verbatim
      (Finding C) per ADR 0027; silver staging normalizes via nullif(col, '').
    - `latest_mpi_active_date` is 100% populated on all records including
      inactive (Finding G), so it is required.
    - `status_regulated_est` is a two-value enum ('' = active MPI,
      'Inactive' = inactive); declared as plain ``str`` rather than
      ``Literal[...]`` so a future third value lands in quarantine instead of
      crashing validation.
    - strict=True + extra='forbid' catches schema drift at ingest (ADR 0014).

    The source's API field names map via ``validation_alias`` to snake_case
    Python field names matching the bronze column names. The identity column
    is ``source_recall_id`` (matching every other source's bronze schema and
    the ``rejected_table_columns()`` helper); its value is the FSIS
    ``establishment_id`` integer-as-string.
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # --- Required identity + demographics (Finding D — 0% empty) ---
    source_recall_id: str = Field(validation_alias="establishment_id")
    establishment_name: str
    establishment_number: str
    address: str
    # `city` was a Finding D blind spot — the cardinality probe didn't enumerate
    # it but the API returns it on every record. First live extraction
    # (2026-05-01) rejected 100% (7,945/7,945) on `extra_forbidden city`. Same
    # class of miss as `field_active_notice` in the recall schema. Treated as
    # required: 0% empty observed in the rejection sample of 7,945 records, so
    # making it nullable would obscure a future shape change.
    city: str
    state: str
    zip: str  # noqa: A003 — column name matches API; shadow of builtin is acceptable here
    # 100% populated on all records including inactive (Finding G).
    latest_mpi_active_date: _UsdaDate = Field(validation_alias="LatestMPIActiveDate")
    # Two-value enum: '' = active MPI, 'Inactive' = inactive (Finding C, exhaustive).
    status_regulated_est: str
    # True JSON arrays; ragged whitespace preserved per ADR 0027 (silver trims).
    # Never null / never absent on real records.
    activities: list[str]
    dbas: list[str]

    # --- Optional demographics (Finding D nullability rates in comments) ---
    # Optional[str] fields preserve '' verbatim per ADR 0027; silver does nullif.
    phone: str | None = Field(default=None)  # 3.9% empty
    duns_number: str | None = Field(default=None)  # 85.5% empty
    fips_code: str | None = Field(default=None)  # 4.3% empty
    # JSON-boolean false sentinel coerced to string "false" (storage-forced —
    # TEXT column can't hold a JSON boolean). Silver does nullif(col, 'false').
    county: _FsisFalseAsTextStr = Field(default="")  # 1.5% empty (false sentinel)
    geolocation: _FsisFalseAsTextStr = Field(default="")  # 1.5%+ empty (false sentinel)
    # Optional date (presence not enumerated in Finding D — treat as nullable).
    grant_date: _UsdaNullableDate = Field(default=None)
    # Optional administrative metadata; '' on inactive records preserved verbatim.
    size: str | None = Field(default=None)
    district: str | None = Field(default=None)
    circuit: str | None = Field(default=None)
