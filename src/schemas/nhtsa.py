"""Bronze-layer Pydantic schema for NHTSA flat-file recall records (ADR 0014).

Targets the 29-field tab-delimited TSV inside FLAT_RCL_*.zip per
documentation/nhtsa/flat_file_observations.md (Phase 5c Step 1). RCL.txt
documents the field positions, types, and 18-year drift history. The
schema:

- Validates 29 fields (one row = one recall × make × model × year tuple).
- Coerces YYYYMMDD date strings to UTC midnight datetime via
  ``_parse_nhtsa_date`` (storage-forced for TIMESTAMPTZ columns per
  ADR 0027). Sentinel values like ODATE ``19010101`` parse to a real
  1901-01-01 datetime — the bronze layer preserves them; silver staging
  does the ``CASE WHEN col = '1901-01-01' THEN NULL`` mapping per ADR
  0027 (Finding H).
- Coerces ``"Yes"`` / ``"No"`` strings on field 28 (DO_NOT_DRIVE) and
  field 29 (PARK_OUTSIDE) to bool via ``_to_bool`` (storage-forced for
  BOOLEAN columns per ADR 0027). Note: USDA uses ``"True"`` / ``"False"``;
  NHTSA uses ``"Yes"`` / ``"No"``. Per the implementation_plan §509-542
  audit, the validators are deliberately not shared across sources —
  source-specific quirks dominate the would-be shared shape.
- Enforces ``StringConstraints(max_length=3)`` on FMVSS per Finding F
  (May 2025 column-width reduction); a regression to a wider value lands
  in quarantine.
- Marks fields added by NHTSA's drift events as nullable so historical
  archives parse without spurious quarantine: NOTES (post-2007),
  RCL_CMPT_ID (post-2008), MFR_COMP_NAME / MFR_COMP_DESC / MFR_COMP_PTNO
  (post-2020), DO_NOT_DRIVE / PARK_OUTSIDE (post-May-2025). DATEA is
  also nullable per Finding H Q2 (5 PRE_2010 records observed).
- Embedded HTML anchor tags in the four narrative fields
  (``desc_defect``, ``conequence_defect``, ``corrective_action``,
  ``notes``) are preserved verbatim per ADR 0027 (Finding E); silver
  staging strips/decodes.
- ``ConfigDict(extra='forbid', strict=True)`` per ADR 0014 — a 30th
  column from the next NHTSA drift event triggers schema-fail re-ingest
  rather than silent absorption.

Field naming: the extractor produces dicts keyed by lowercase RCL.txt
column names (``record_id``, ``campno``, ``maketxt``, ...). Pydantic
field names match the bronze column names — most are identical to the
input keys, except ``source_recall_id`` which absorbs ``record_id`` via
``validation_alias`` (mirrors USDA's
``field_recall_number → source_recall_id`` pattern).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StringConstraints


def _to_bool(v: Any) -> bool:
    """Coerce NHTSA's ``"Yes"`` / ``"No"`` string to Python bool.

    NHTSA fields 28-29 (DO_NOT_DRIVE, PARK_OUTSIDE) are documented as
    Yes/No per RCL.txt and observed verbatim in
    ``FLAT_RCL_POST_2010.txt`` (Finding E). USDA uses ``"True"`` /
    ``"False"`` instead — distinct validators per ADR 0027 + the
    §509-542 audit.
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        if v == "Yes":
            return True
        if v == "No":
            return False
    raise ValueError(f"Cannot coerce {v!r} to bool — expected 'Yes' or 'No'")


def _to_nullable_bool(v: Any) -> bool | None:
    """Same as ``_to_bool`` but treats null and ``""`` as missing.

    Storage-forced (BOOLEAN NULL cannot hold the empty string); empty
    string → None stays in bronze per ADR 0027.
    """
    if v is None or v == "":
        return None
    return _to_bool(v)


def _parse_nhtsa_date(v: Any) -> datetime:
    """Parse YYYYMMDD → UTC midnight datetime.

    NHTSA's date format per RCL.txt is an 8-digit string with no
    separators (e.g. ``"20240101"``). Sentinel values like ODATE
    ``19010101`` parse to real datetimes (1901-01-01) — bronze
    preserves them per ADR 0027; silver staging maps them to NULL.
    """
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str) and v:
        return datetime.strptime(v, "%Y%m%d").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as NHTSA date — expected YYYYMMDD")


def _parse_nullable_nhtsa_date(v: Any) -> datetime | None:
    """Same as ``_parse_nhtsa_date`` but treats null and ``""`` as missing.

    Storage-forced (TIMESTAMPTZ NULL cannot hold the empty string); empty
    string → None stays in bronze per ADR 0027.
    """
    if v is None or v == "":
        return None
    return _parse_nhtsa_date(v)


# Annotated types — BeforeValidator runs before strict-mode type checks
# so the source's string serializations get coerced before Pydantic's
# type rejection. Per ADR 0027, only storage-forced transforms live
# here; value-level normalization (empty-string → None on Optional[str]
# fields, sentinel-date mapping, HTML stripping) happens in
# stg_nhtsa_recalls.sql.
_NhtsaBool = Annotated[bool, BeforeValidator(_to_bool)]
_NhtsaNullableBool = Annotated[bool | None, BeforeValidator(_to_nullable_bool)]
_NhtsaDate = Annotated[datetime, BeforeValidator(_parse_nhtsa_date)]
_NhtsaNullableDate = Annotated[datetime | None, BeforeValidator(_parse_nullable_nhtsa_date)]
# FMVSS narrowed to CHAR(3) per Finding F (May 2025 width reduction).
# Apply BeforeValidator so the constraint runs against parsed strings;
# longer values land in quarantine via ValidationError.
_NhtsaFmvss = Annotated[str, StringConstraints(max_length=3)]


class NhtsaRecord(BaseModel):
    """Bronze-layer schema for one NHTSA flat-file row.

    See module docstring for the field-shape origins (RCL.txt + Findings
    E, F, H). The 28 domain fields below correspond to RCL.txt fields
    2-29; field 1 (RECORD_ID) is held by ``source_recall_id`` which the
    bronze loader's default ``identity_fields=("source_recall_id",)``
    references for hash dedup.

    Required vs nullable follows Finding F's drift history: every field
    that NHTSA has added at the right edge since 2007 is nullable so
    historical archives (pre-2007 records lack NOTES, etc.) parse
    without quarantine. The strict + extra=forbid combination still
    catches a future 30th column.
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # --- Identity (RCL.txt field 1) ---
    # validation_alias absorbs the extractor's lowercase RCL.txt key into
    # the canonical bronze column name (mirrors USDA's
    # field_recall_number → source_recall_id pattern). RECORD_ID is
    # documented in RCL.txt as a stable per-row natural key.
    source_recall_id: str = Field(validation_alias="record_id")

    # --- Required campaign/vehicle identifiers (RCL.txt fields 2-8, 11-12, 15) ---
    campno: str
    maketxt: str
    modeltxt: str
    yeartxt: str  # numeric-as-string per RCL.txt CHAR(4)
    mfgcampno: str | None = Field(default=None)  # nullable in source
    compname: str
    mfgname: str
    rcltype: str
    potaff: str  # number-affected — kept as string at bronze (silver casts to int)
    mfgtxt: str

    # --- Optional date (RCL.txt field 16, RCDATE — Part 573 received date) ---
    # The cleanest "when did the recall happen" measure per Finding H Q2.
    # Nullable per the 2026-05-05 sentinel-date probe: 5/81,714 PRE_2010
    # records have empty RCDATE — same records as the empty-DATEA cohort,
    # almost certainly from the 1979 bulk-load of pre-1979 historical
    # recalls. POST_2010 has 0 empty RCDATE rows. Marking required would
    # quarantine real recall records over a missing date field.
    rcdate: _NhtsaNullableDate = Field(default=None)

    # --- Required narrative fields (RCL.txt fields 20-22) ---
    # Embedded HTML preserved per Finding E + ADR 0027.
    desc_defect: str
    conequence_defect: str
    corrective_action: str

    # --- Nullable dates (RCL.txt fields 9, 10, 13, 17) ---
    # bgman / endman / odate optional in source per RCL.txt. ODATE uses
    # 19010101 as an unknown-date sentinel (Finding H) — preserved as
    # 1901-01-01 datetime; silver maps to NULL.
    bgman: _NhtsaNullableDate = Field(default=None)
    endman: _NhtsaNullableDate = Field(default=None)
    odate: _NhtsaNullableDate = Field(default=None)
    # DATEA nullable per Finding H Q2 (5/81,714 PRE_2010 records).
    datea: _NhtsaNullableDate = Field(default=None)

    # --- Nullable strings already present pre-2007 (RCL.txt fields 14, 18, 19) ---
    influenced_by: str | None = Field(default=None)
    rpno: str | None = Field(default=None)
    fmvss: _NhtsaFmvss | None = Field(default=None)

    # --- Drift-added nullable fields per Finding F ---
    # 2007-09-14: field 23 NOTES added.
    notes: str | None = Field(default=None)
    # 2008-03-14: field 24 RCL_CMPT_ID added. Opaque concatenated identifier
    # per Finding E — preserved as string at bronze.
    rcl_cmpt_id: str | None = Field(default=None)
    # 2020-03-23: fields 25-27 added (manufacturer-supplied component metadata).
    mfr_comp_name: str | None = Field(default=None)
    mfr_comp_desc: str | None = Field(default=None)
    mfr_comp_ptno: str | None = Field(default=None)
    # May 2025: fields 28-29 added — Yes/No strings → bool.
    do_not_drive: _NhtsaNullableBool = Field(default=None)
    park_outside: _NhtsaNullableBool = Field(default=None)
