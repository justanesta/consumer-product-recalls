"""Bronze-layer Pydantic schema for USCG boat-manufacturer-directory records (ADR 0014).

Targets the 5-field listing table at
``https://uscgboating.org/content/manufacturers-identification.php`` per
``documentation/uscg/manufacturer_scraping_observations.md`` (Phase 5d Step 7
Step 1 Findings A & B).

The schema:

- Validates 5 domain fields per row (MIC, Company, Address, City, State) plus
  two lineage columns: the USCG internal sequential row id parsed from the
  detail URL (``uscg_directory_id``) and the full detail URL preserved for
  forensics (``detail_url``).
- No date coercion needed — the listing exposes no date columns. (The detail
  page may expose business-date fields per Finding C open question; deferred
  to a future per-row enrichment pass if ever pursued.)
- ``ConfigDict(extra='forbid', strict=True)`` per ADR 0014 — a new column or
  relabeled HTML field surfaces as a schema-fail re-ingest rather than silent
  absorption. The listing-page table-header check in
  ``UscgManufacturerExtractor._parse_listing_page`` is the first drift fence;
  this is the second.

Field naming: the extractor produces dicts keyed by lowercase parser-emitted
names (``"mic"``, ``"company"``, ``"address"``, ``"city"``, ``"state"``,
``"uscg_directory_id"``, ``"detail_url"``). Pydantic field names match those
keys directly, with two exceptions: ``source_recall_id`` absorbs the dict key
``"mic"`` via ``validation_alias`` (cross-source bronze identity convention;
mirrors CPSC ``RecallNumber``, NHTSA ``record_id``, USDA ``field_recall_number``,
USDA establishments ``establishment_id``, USCG recalls ``number``);
``company_name`` absorbs ``"company"`` to align with ``firm.sql`` terminology.

No ``populate_by_name=True`` — the schema is only ever constructed from
extractor dicts keyed by the validation aliases, never by Python field name.
Tests use the same alias-keyed shape.

Nullability per Step 1 observations:
- ``source_recall_id``: required (MIC is the regulatory natural key; every
  directory row has one per Finding B).
- ``detail_url``: required (Finding C — every row's MIC cell wraps an anchor;
  the absolutized href is always present).
- Everything else: nullable. Finding F surfaced three distinct missing-data
  conventions in the source (literal ``"UNK"``, literal ``"-"``, empty string);
  bronze preserves verbatim per ADR 0027, silver does the multi-pattern
  nullif coercion.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class UscgManufacturerRecord(BaseModel):
    """Bronze-layer schema for one USCG manufacturer-directory row.

    See module docstring for field-shape origins (Findings A-K in
    ``documentation/uscg/manufacturer_scraping_observations.md``). Listing-only
    extraction — the per-manufacturer detail page (Finding C) exists at
    ``manufacturers-identification-detail.php?id=N`` but is not walked in v1.
    Address-field truncation at ~30 chars is a known limitation per
    Finding F.1.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    # --- Identity ---
    # ``source_recall_id`` aliases ``"mic"`` from the listing parser's dict
    # (column header is "MIC"). MIC is a 3-character alphanumeric token per
    # USCG-2013-0133-0005; pure-digit MICs (101-126) belong to the engine-maker
    # reserved sub-namespace, everything else is 3-letter alpha. No pattern
    # constraint enforced at bronze — would quarantine real records on any
    # regulatory edge case; silver can enforce after Step 3 corpus probe
    # confirms uniform format.
    source_recall_id: str = Field(validation_alias="mic")

    # --- Listing-derived nullable fields ---
    # ``company_name`` aliases ``"company"`` (column header is "Company"; the
    # project-wide convention uses ``_name`` suffix to align with firm.sql).
    # Nullable per Finding F.3 — some rows use literal "-" placeholder for
    # redacted/withdrawn manufacturers; silver normalizes the sentinel.
    company_name: str | None = Field(default=None, validation_alias="company")

    # Listing-view address is visibly truncated at ~30 chars per Finding F.1
    # (source DB VARCHAR constraint). Bronze preserves verbatim — including
    # embedded newlines (Finding F.2, HONDA row example) and "UNK" / "-"
    # sentinels (Finding F.3). Full address only retrievable via detail-page
    # walk (not implemented in v1).
    address: str | None = Field(default=None)

    # City may be empty, "UNK", "-", or a real value per Findings F.3 / G.
    city: str | None = Field(default=None)

    # State is 2-letter (Finding G); mixes US states, US territories, AND
    # Canadian provinces (BC, ON, AB, NS, NL, QC). NOT constrainable to a
    # US-states Literal. Empty string is the missing-state convention for
    # some Canadian rows (Finding F.3 — different from "UNK" / "-" sentinels).
    state: str | None = Field(default=None)

    # --- Lineage / forensics ---
    # ``uscg_directory_id`` is the URL ``?id=`` query parameter from the
    # detail-page anchor — USCG's internal sequential row PK (Finding B).
    # Page-offset-deterministic across probed pages (id = page * 25 + row),
    # so likely unstable across re-crawls when records are added/removed
    # before the row in the alphabetical ordering. Excluded from content_hash
    # in ``UscgManufacturerExtractor.load_bronze`` to prevent re-crawl churn.
    uscg_directory_id: int | None = Field(default=None)

    # Full detail URL (anchor's href, absolutized to the base path
    # ``https://uscgboating.org/content/``). Preserved for forensics; excluded
    # from content_hash to defend against URL-scheme rewrites (mirrors
    # ``UscgScrapingExtractor``'s ``details_url`` hash exclusion).
    detail_url: str
