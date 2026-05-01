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


def _normalize_str(v: Any) -> str | None:
    """Normalize empty-string sentinel to None (Finding C — many fields use '' for missing)."""
    if v is None or v == "":
        return None
    return v


def _parse_usda_date(v: Any) -> datetime:
    """Parse YYYY-MM-DD → UTC midnight datetime."""
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str) and v:
        return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as USDA date — expected YYYY-MM-DD")


def _parse_nullable_usda_date(v: Any) -> datetime | None:
    """Same as _parse_usda_date but treats null and '' as missing (Finding C)."""
    if v is None or v == "":
        return None
    return _parse_usda_date(v)


# Annotated types — BeforeValidator runs before strict mode so the source's
# string serializations get coerced before Pydantic's type checks reject them.
_UsdaBool = Annotated[bool, BeforeValidator(_to_bool)]
_UsdaNullableBool = Annotated[bool | None, BeforeValidator(_to_nullable_bool)]
_UsdaNullableStr = Annotated[str | None, BeforeValidator(_normalize_str)]
_UsdaDate = Annotated[datetime, BeforeValidator(_parse_usda_date)]
_UsdaNullableDate = Annotated[datetime | None, BeforeValidator(_parse_nullable_usda_date)]


class UsdaFsisRecord(BaseModel):
    """
    Bronze-layer schema for USDA FSIS recall records (ADR 0014).

    Targets the GET /fsis/api/recall/v/1 flat-array response. Bilingual companion
    records are siblings (same field_recall_number, distinct langcode) — not nested.

    Key validation behaviors:
    - Boolean fields arrive as "True" / "False" strings (Finding L); coerced to bool.
    - Date fields use YYYY-MM-DD format; coerced to UTC midnight datetime.
    - Empty string '' normalized to None on all Optional[str] fields (Finding C —
      many fields use '' as a missing-value sentinel rather than omitting the key).
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
    closed_date: _UsdaNullableDate = Field(default=None, validation_alias="field_closed_date")

    # --- Optional booleans (Finding C: related_to_outbreak 25% empty) ---
    related_to_outbreak: _UsdaNullableBool = Field(
        default=None, validation_alias="field_related_to_outbreak"
    )

    # --- Optional strings ('' → None per Finding C) ---
    closed_year: _UsdaNullableStr = Field(default=None, validation_alias="field_closed_year")
    year: _UsdaNullableStr = Field(default=None, validation_alias="field_year")
    risk_level: _UsdaNullableStr = Field(default=None, validation_alias="field_risk_level")
    recall_reason: _UsdaNullableStr = Field(default=None, validation_alias="field_recall_reason")
    processing: _UsdaNullableStr = Field(default=None, validation_alias="field_processing")
    states: _UsdaNullableStr = Field(default=None, validation_alias="field_states")
    establishment: _UsdaNullableStr = Field(default=None, validation_alias="field_establishment")
    labels: _UsdaNullableStr = Field(default=None, validation_alias="field_labels")
    qty_recovered: _UsdaNullableStr = Field(default=None, validation_alias="field_qty_recovered")
    summary: _UsdaNullableStr = Field(default=None, validation_alias="field_summary")
    product_items: _UsdaNullableStr = Field(default=None, validation_alias="field_product_items")
    distro_list: _UsdaNullableStr = Field(default=None, validation_alias="field_distro_list")
    media_contact: _UsdaNullableStr = Field(default=None, validation_alias="field_media_contact")
    company_media_contact: _UsdaNullableStr = Field(
        default=None, validation_alias="field_company_media_contact"
    )
    # Undocumented field — observed Finding H. Kept Optional[str] in case it is
    # absent on some records (PDF docs do not list it, suggesting late addition).
    recall_url: _UsdaNullableStr = Field(default=None, validation_alias="field_recall_url")
    # Dead fields — 100% / 99.9% empty per Finding C; excluded from content hash
    # by the loader so they cannot trigger spurious "changes" if FSIS ever populates them.
    en_press_release: _UsdaNullableStr = Field(
        default=None, validation_alias="field_en_press_release"
    )
    press_release: _UsdaNullableStr = Field(default=None, validation_alias="field_press_release")
