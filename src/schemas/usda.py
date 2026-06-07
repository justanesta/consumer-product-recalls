from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field


def _to_bool(v: Any) -> bool:
    """
    Coerce USDA's capitalized string-bool to Python bool (Finding L).

    USDA returns "True" / "False" as strings on response output, while Drupal
    taxonomy filter input expects 1 / 0 integers. This validator handles only
    the response-side string form.
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        if v == "True":
            return True
        if v == "False":
            return False
    raise ValueError(f"Cannot coerce {v!r} to bool — expected 'True' or 'False'")


def _to_nullable_bool(v: Any) -> bool | None:
    """Same as _to_bool but treats null and '' as missing (None)."""
    if v is None or v == "":
        return None
    return _to_bool(v)


def _parse_usda_date(v: Any) -> datetime:
    """Parse YYYY-MM-DD → UTC midnight datetime."""
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str) and v:
        return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as USDA date — expected YYYY-MM-DD")


def _parse_nullable_usda_date(v: Any) -> datetime | None:
    """Same as _parse_usda_date but treats null and '' as missing.

    Storage-forced (TIMESTAMPTZ NULL cannot hold the empty string), so '' → None
    stays in bronze per ADR 0027.
    """
    if v is None or v == "":
        return None
    return _parse_usda_date(v)


def _to_str_list(v: Any) -> list[str] | None:
    """Coerce USDA's multi-value fields to ``list[str] | None`` (2026-06 API change, Finding S).

    FSIS flipped ten previously-scalar fields from comma-joined strings to native JSON
    arrays (e.g. ``field_states`` ``"California, Nevada"`` → ``["California", "Nevada"]``).
    This validator accepts BOTH shapes so the schema is robust to the change AND to
    R2-replay of pre-change payloads / cassettes (ADR 0028), which still carry scalars:

      - ``None``       → ``None``  (key omitted or null)
      - ``""``         → ``[]``    (the old empty-scalar sentinel = empty)
      - ``"a, b"``     → ``["a, b"]``  (old scalar wrapped **verbatim, not split** — silver
                                        owns any tokenization, per ADR 0027)
      - ``[]``         → ``[]``    (new empty array)
      - ``["a", "b"]`` → passthrough

    Storage-forced only (coerce the source's representation into the column's array type);
    no business normalization. Element-level ``str`` typing is enforced by the field's
    ``list[str]`` annotation under strict mode, so a non-string array element fails loudly.
    """
    if v is None:
        return None
    if isinstance(v, str):
        return [] if v == "" else [v]
    if isinstance(v, list):
        return v
    raise ValueError(f"Cannot coerce {v!r} to list[str] — expected str, list, or null")


# Annotated types — BeforeValidator runs before strict mode so the source's
# string serializations get coerced before Pydantic's type checks reject them.
# Per ADR 0027, only storage-forced transforms live here. Empty-string-to-None
# normalization on Optional[str] fields moved to silver staging (nullif(col, '')).
_UsdaBool = Annotated[bool, BeforeValidator(_to_bool)]
_UsdaNullableBool = Annotated[bool | None, BeforeValidator(_to_nullable_bool)]
_UsdaDate = Annotated[datetime, BeforeValidator(_parse_usda_date)]
_UsdaNullableDate = Annotated[datetime | None, BeforeValidator(_parse_nullable_usda_date)]
# 2026-06 API change (Finding S): ten multi-value fields became JSON arrays. Stored as
# jsonb in bronze; accepts scalar OR list for replay safety. See _to_str_list.
_UsdaStrList = Annotated[list[str] | None, BeforeValidator(_to_str_list)]


class UsdaFsisRecord(BaseModel):
    """
    Bronze-layer schema for USDA FSIS recall records (ADR 0014).

    Targets the GET /fsis/api/recall/v/1 flat-array response. Bilingual companion
    records are siblings (same field_recall_number, distinct langcode) — not nested.

    Key validation behaviors:
    - Boolean fields arrive as "True" / "False" strings (Finding L); coerced to bool.
    - Date fields use YYYY-MM-DD format; coerced to UTC midnight datetime.
    - Optional[str] fields preserve the source's '' representation verbatim per ADR
      0027 (Finding C — many fields use '' as a missing-value sentinel rather than
      omitting the key); silver staging normalizes via nullif(col, '').
    - langcode is the only enum-like field; Literal catches drift loudly.
    - field_recall_url is undocumented in the PDF (Finding H) but consistently
      returned by the live API; declared Optional[str] to absorb either presence.
    - field_en_press_release and field_press_release are 100% / 99.9% empty
      (Finding C) — kept in the schema for shape parity but excluded from the
      content hash by the loader (they will never drive a real change).
    - strict=True + extra='forbid' catches schema drift at ingest (ADR 0014).

    The source's API field names are kept as-is on the input side via
    validation_alias, but DB column names use the snake_case Python field names
    (no `field_` prefix) — same convention as CpscRecord and FdaRecord.
    """

    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # --- Required identifiers and lifecycle (0% empty per Finding C/D) ---
    source_recall_id: str = Field(validation_alias="field_recall_number")
    langcode: Literal["English", "Spanish"]
    title: str = Field(validation_alias="field_title")
    recall_date: _UsdaDate = Field(validation_alias="field_recall_date")
    recall_type: str = Field(validation_alias="field_recall_type")
    recall_classification: str = Field(validation_alias="field_recall_classification")

    # Required booleans — always populated per Finding C/D.
    # `field_active_notice` was originally treated as required, but Phase 5b first
    # extraction (2026-04-30) found 189/2001 (~9.4%) records with empty-string
    # values for it — a Finding C blind spot since the original empty-rate audit
    # did not probe `field_active_notice`. It is now Optional[bool]; see Finding
    # C addendum in documentation/usda/recall_api_observations.md.
    archive_recall: _UsdaBool = Field(validation_alias="field_archive_recall")
    has_spanish: _UsdaBool = Field(validation_alias="field_has_spanish")
    active_notice: _UsdaNullableBool = Field(default=None, validation_alias="field_active_notice")

    # --- Optional dates (Finding C: last_modified 42% empty, closed_date 8.4% empty) ---
    last_modified_date: _UsdaNullableDate = Field(
        default=None, validation_alias="field_last_modified_date"
    )
    # 2026-06 API change (Finding S): field_closed_date is no longer returned (absent on
    # 0/2006 records). Retained — default=None tolerates the absence, historical bronze
    # rows keep their values, and a restore needs no schema change. New rows: NULL (a
    # data-availability regression tracked in recall_api_observations.md Finding S).
    closed_date: _UsdaNullableDate = Field(default=None, validation_alias="field_closed_date")

    # --- Optional booleans (Finding C: related_to_outbreak 25% empty) ---
    related_to_outbreak: _UsdaNullableBool = Field(
        default=None, validation_alias="field_related_to_outbreak"
    )

    # --- Optional SCALAR strings — '' preserved verbatim per ADR 0027; silver normalizes ---
    closed_year: str | None = Field(default=None, validation_alias="field_closed_year")
    year: str | None = Field(default=None, validation_alias="field_year")
    risk_level: str | None = Field(default=None, validation_alias="field_risk_level")
    qty_recovered: str | None = Field(default=None, validation_alias="field_qty_recovered")
    summary: str | None = Field(default=None, validation_alias="field_summary")
    media_contact: str | None = Field(default=None, validation_alias="field_media_contact")
    # Undocumented field — observed Finding H. Kept Optional[str] in case it is
    # absent on some records (PDF docs do not list it, suggesting late addition).
    recall_url: str | None = Field(default=None, validation_alias="field_recall_url")

    # --- Multi-value ARRAY fields (2026-06 API change, Finding S) ---
    # FSIS flipped these ten fields from comma-joined scalars to JSON arrays. Stored as
    # jsonb in bronze (migration 0028); _UsdaStrList accepts scalar OR list for replay
    # safety. Silver staging (stg_usda_fsis_recalls) collapses them back to CSV for the
    # current downstream contract; exploiting the native arrays is deferred follow-up.
    recall_reason: _UsdaStrList = Field(default=None, validation_alias="field_recall_reason")
    processing: _UsdaStrList = Field(default=None, validation_alias="field_processing")
    states: _UsdaStrList = Field(default=None, validation_alias="field_states")
    establishment: _UsdaStrList = Field(default=None, validation_alias="field_establishment")
    labels: _UsdaStrList = Field(default=None, validation_alias="field_labels")
    product_items: _UsdaStrList = Field(default=None, validation_alias="field_product_items")
    distro_list: _UsdaStrList = Field(default=None, validation_alias="field_distro_list")
    company_media_contact: _UsdaStrList = Field(
        default=None, validation_alias="field_company_media_contact"
    )
    # Dead fields — historically 100% / 99.9% empty per Finding C, now empty arrays;
    # excluded from the content hash by the loader so they cannot trigger spurious changes.
    en_press_release: _UsdaStrList = Field(default=None, validation_alias="field_en_press_release")
    press_release: _UsdaStrList = Field(default=None, validation_alias="field_press_release")

    # Added by the 2026-06 API change (Finding S) — a scalar export form of the recall
    # number (observed identical to field_recall_number). Captured for parity; no silver
    # consumer yet. Optional so pre-change payloads (which lack it) still validate.
    recall_number_export: str | None = Field(
        default=None, validation_alias="field_recall_number_export"
    )
