"""Bronze-layer Pydantic schema for USCG boating-recall records (ADR 0014).

Targets the 6-field listing table at
``https://uscgboating.org/content/recalls.php`` joined to the 13-field
details page at ``recalls-details.php?id=<recall_number>`` per
``documentation/uscg/scraping_observations.md`` (Phase 5d Step 1 Findings A & B).

The schema:

- Validates 19 domain fields per row (6 from listing + 13 from details
  page) plus the details URL preserved for lineage.
- Coerces date strings to UTC midnight datetime via two distinct
  ``BeforeValidator``s — listing uses ``YYYY-MM-DD`` (e.g. ``2026-03-03``),
  details uses ``M/D/YYYY`` (e.g. ``3/3/2026``). Format inconsistency
  documented at Finding F. Storage-forced per ADR 0027; sentinel-value
  detection (e.g. mapping ``"N/A"`` HIN to NULL) belongs in
  ``stg_uscg_recalls.sql``, not here.
- ``ConfigDict(extra='forbid', strict=True)`` per ADR 0014 — a new
  column or relabeled HTML field surfaces as a schema-fail re-ingest
  rather than silent absorption. The listing-page table-header check
  in ``UscgScrapingExtractor._parse_listing_page`` is the first drift
  fence; this is the second.

Field naming: the extractor produces dicts keyed by lowercase
human-readable names matching the HTML label text (with the colons
stripped, spaces converted to underscores, e.g. ``"Case Open Date"`` →
``"case_open_date"``). Pydantic field names match those keys directly,
with one exception: ``source_recall_id`` absorbs the dict key
``"number"`` via ``validation_alias`` to align with the cross-source
convention (CPSC ``RecallNumber``, NHTSA ``record_id``, USDA
``field_recall_number`` all do the same).

Note that listing-page ``"Company Name"`` and details-page
``"Company:"`` are the same value with different HTML labels — the
extractor normalizes both to dict key ``"company_name"`` so the schema
sees a single canonical field. Similarly listing-page ``"Opened On"``
(YYYY-MM-DD) is preserved separately from details-page ``"Case Open
Date"`` (M/D/YYYY) — they ARE the same date semantically but bronze
captures both raw observations and silver decides canonical.

No ``populate_by_name=True`` — the schema is only ever constructed from
extractor dicts keyed by the validation aliases, never by Python field
name. Tests use the same alias-keyed shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field


def _parse_uscg_listing_date(v: Any) -> datetime:
    """Parse ``YYYY-MM-DD`` (listing-page format) → UTC midnight datetime.

    Storage-forced for the TIMESTAMPTZ bronze column. The listing-page
    ``Opened On`` column uses this format per Finding F.
    """
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str) and v:
        return datetime.strptime(v, "%Y-%m-%d").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as USCG listing date — expected YYYY-MM-DD")


def _parse_nullable_uscg_listing_date(v: Any) -> datetime | None:
    """``_parse_uscg_listing_date`` but treats null and empty string as missing.

    Used by ``opened_on`` despite Finding A showing it populated in all
    sampled rows. The sample was only ~2.2% of the corpus (38 rows across
    pages 0 + 70 of 71); a defensive nullable variant avoids the
    "tighten via migration after a quarantine surprise" cost. Step 1.5
    corpus probe will confirm or invalidate the always-populated
    assumption; if confirmed, we can tighten in a follow-up migration with
    real evidence.
    """
    if v is None or v == "":
        return None
    return _parse_uscg_listing_date(v)


def _parse_uscg_details_date(v: Any) -> datetime:
    """Parse ``M/D/YYYY`` (details-page format) → UTC midnight datetime.

    Storage-forced for the TIMESTAMPTZ bronze column. The details page
    uses this format for all five dates (``Case Open Date``,
    ``Case Close Date``, ``Campaign Open Date``, ``Campaign Close Date``,
    ``Last Date``) per Finding F. Examples: ``"3/3/2026"``, ``"12/2/2025"``.
    Note: ``strptime("%m/%d/%Y")`` accepts both zero-padded and
    non-zero-padded month/day (e.g. ``"03/03/2026"`` and ``"3/3/2026"``).
    """
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str) and v:
        return datetime.strptime(v, "%m/%d/%Y").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as USCG details date — expected M/D/YYYY")


def _parse_nullable_uscg_details_date(v: Any) -> datetime | None:
    """``_parse_uscg_details_date`` but treats null and empty string as missing."""
    if v is None or v == "":
        return None
    return _parse_uscg_details_date(v)


# Annotated types. BeforeValidator runs before strict-mode type checks
# so source-string dates get coerced before Pydantic's type rejection.
# Per ADR 0027, only storage-forced transforms live here; value-level
# normalization (empty-string → None on Optional[str] fields, sentinel
# mapping like ``HIN="N/A"`` → NULL) happens in ``stg_uscg_recalls.sql``.
_UscgListingDate = Annotated[datetime, BeforeValidator(_parse_uscg_listing_date)]
_UscgNullableListingDate = Annotated[
    datetime | None, BeforeValidator(_parse_nullable_uscg_listing_date)
]
_UscgDetailsDate = Annotated[datetime, BeforeValidator(_parse_uscg_details_date)]
_UscgNullableDetailsDate = Annotated[
    datetime | None, BeforeValidator(_parse_nullable_uscg_details_date)
]


class UscgRecallRecord(BaseModel):
    """Bronze-layer schema for one USCG recall — merged listing + details row.

    See module docstring for field-shape origins (Findings A-N in
    ``documentation/uscg/scraping_observations.md``). The 19 domain
    fields below come from two HTML pages (listing + details);
    ``UscgScrapingExtractor.extract()`` fetches both and merges into
    a single dict that gets validated by this model.

    Required vs nullable:
    - ``source_recall_id``: required (always present in observed samples
      AND structurally the anchor field — there's no row without a
      recall number).
    - ``company_name``: required (always populated in 38/38 listing-page
      sample rows).
    - ``opened_on``: **nullable** despite being populated 38/38 in the
      sample. ~2.2% of the corpus isn't enough evidence to mark NOT NULL;
      Step 1.5 corpus probe will validate (and a follow-up migration can
      tighten if confirmed). See `documentation/uscg/scraping_observations.md`
      Finding A.
    - Everything else: nullable. Finding B confirmed many details fields
      are empty on individual recalls (e.g., ``severity`` and ``problem_2``
      were empty for both probed samples).
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    # --- Listing-derived required fields ---
    # ``source_recall_id`` aliases ``"number"`` from the listing parser's
    # dict (listing column header is "Number"). The same value appears
    # on the details page; the merge in ``extract()`` resolves any
    # divergence to the listing's value (no divergence observed in
    # Phase 5d Step 1 probes).
    source_recall_id: str = Field(validation_alias="number")
    company_name: str  # listing "Company Name" / details "Company:" — normalized to one key

    # ``opened_on`` nullable per Finding A scope caveat (38/38 populated
    # in sample, ~2.2% of corpus). Tighten via follow-up migration if
    # Step 1.5's corpus probe confirms 1763/1763 populated.
    opened_on: _UscgNullableListingDate = Field(default=None)

    # --- Listing-derived nullable fields ---
    mic: str | None = Field(default=None)
    model_name: str | None = Field(default=None)
    problem_1: str | None = Field(default=None)

    # --- Lineage column (preserved for forensics) ---
    # Excluded from content_hash via ``hash_exclude_fields`` in
    # ``UscgScrapingExtractor.load_bronze`` — defense against future
    # URL-scheme rewrites (e.g., if USCG migrates to ``recall/<id>``).
    details_url: str

    # --- Details-derived nullable fields ---
    # All optional per Finding B sample observations: ``problem_2``,
    # ``model_year``, ``severity`` are commonly empty.
    company_official: str | None = Field(default=None)
    model_year: str | None = Field(default=None)  # may be multi-year string; bronze stays str
    problem_2: str | None = Field(default=None)
    hin: str | None = Field(default=None)  # may be "N/A" — silver normalizes
    case_open_date: _UscgNullableDetailsDate = Field(default=None)
    disposition: str | None = Field(default=None)  # "Open" / "Closed"
    case_close_date: _UscgNullableDetailsDate = Field(default=None)
    # Bronze keeps ``units`` as string per ADR 0027 — observed values are
    # integer-like (``"20"``, ``"401"``) but USCG could ship thousands
    # separators or ranges in future; silver casts to int.
    units: str | None = Field(default=None)
    campaign_open_date: _UscgNullableDetailsDate = Field(default=None)
    boat_type: str | None = Field(default=None)  # numeric type code as string
    campaign_close_date: _UscgNullableDetailsDate = Field(default=None)
    severity: str | None = Field(default=None)
    # Finding E (deferred to Step 3): if ``last_date`` re-stamps on every
    # page render rather than tracking real recall lifecycle, add to
    # ``hash_exclude_fields``. Two consecutive probe fetches returned
    # byte-identical bytes (Finding D) so it's NOT a render-time
    # timestamp; treating as a legitimate lifecycle date for v1.
    last_date: _UscgNullableDetailsDate = Field(default=None)
