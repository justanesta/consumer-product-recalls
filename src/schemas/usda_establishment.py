from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from src.schemas.usda import _parse_nullable_usda_date, _parse_usda_date


def _normalize_false_sentinel(v: Any) -> str | None:
    """Normalize the literal boolean ``False`` to ``None``.

    USDA FSIS Establishment Listing API uses ``false`` (boolean) as a missing
    sentinel for `geolocation` and `county` (Finding C in
    documentation/usda/establishment_api_observations.md) — *not* ``null``,
    *not* ``""``. The strict identity check ``v is False`` matches only the
    singleton boolean and not other falsy values like ``""`` or ``0``, which
    might legitimately appear in other contexts.
    """
    if v is False:
        return None
    return v


def _strip_list_elements(v: Any) -> list[str]:
    """Trim leading/trailing whitespace from each list element.

    Per Finding C, the API serializes ``activities`` and ``dbas`` as true JSON
    arrays where elements after index 0 may carry a leading space (e.g.,
    ``["Meat Processing", " Poultry Processing"]``). The validator does not
    accept ``None`` — at the bronze boundary we prefer strict validation +
    quarantine (ADR 0013) over defensive coercion. If the API ever returns
    ``null``, the record fails validation and lands in
    ``usda_fsis_establishments_rejected`` for investigation rather than
    silently becoming an empty list. The explicit check raises ``ValueError``
    (which Pydantic wraps into ``ValidationError``) rather than letting a
    raw ``TypeError`` from list iteration propagate.
    """
    if v is None:
        raise ValueError("expected a list, got None")
    return [s.strip() for s in v]


# Annotated types — BeforeValidator runs before strict-mode type checks so the
# raw boolean false / ragged-whitespace strings reach our normalizers first.
_FsisFalseSentinelStr = Annotated[str | None, BeforeValidator(_normalize_false_sentinel)]
_FsisStrippedStrList = Annotated[list[str], BeforeValidator(_strip_list_elements)]
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
      (Finding C); normalized to ``None`` by ``_normalize_false_sentinel``.
    - `activities` and `dbas` are true JSON arrays with ragged leading
      whitespace on non-first elements (Finding C); each element is trimmed.
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
    state: str
    zip: str  # noqa: A003 — column name matches API; shadow of builtin is acceptable here
    # 100% populated on all records including inactive (Finding G).
    latest_mpi_active_date: _UsdaDate = Field(validation_alias="LatestMPIActiveDate")
    # Two-value enum: '' = active MPI, 'Inactive' = inactive (Finding C, exhaustive).
    status_regulated_est: str
    # True JSON arrays with ragged whitespace; never null / never absent on real records.
    activities: _FsisStrippedStrList
    dbas: _FsisStrippedStrList

    # --- Optional demographics (Finding D nullability rates in comments) ---
    phone: str | None = Field(default=None)  # 3.9% empty
    duns_number: str | None = Field(default=None)  # 85.5% empty
    fips_code: str | None = Field(default=None)  # 4.3% empty
    # boolean false sentinel → None
    county: _FsisFalseSentinelStr = Field(default=None)  # 1.5% empty (false sentinel)
    geolocation: _FsisFalseSentinelStr = Field(default=None)  # 1.5%+ empty (false sentinel)
    # Optional date (presence not enumerated in Finding D — treat as nullable).
    grant_date: _UsdaNullableDate = Field(default=None)
    # Optional administrative metadata; '' on inactive records (per observations
    # doc Implications section). Empty-string sentinel handling lives in the
    # silver layer if needed — bronze keeps the raw string.
    size: str | None = Field(default=None)
    district: str | None = Field(default=None)
    circuit: str | None = Field(default=None)
