"""Bronze-layer Pydantic schema for USCG manufacturer **detail-page** records (ADR 0014).

Phase 5d Step 7 (detail) — Path B. Targets the per-manufacturer detail page at
``https://uscgboating.org/content/manufacturers-identification-detail.php?id=N``
(confirmed direct-GET despite its ``class="iframe"`` markup —
``documentation/uscg/manufacturer_scraping_observations.md`` §M.2). Sibling to
``src/schemas/uscg_manufacturer.py`` (the listing-only schema); this captures the
~20 fields the detail page exposes beyond the 5 listing fields, including the
source-native succession lineage (``past_company_1/2/3``, ``out_of_business``,
``in_business``, ``date_modified``) that drives the eventual SCD-2 firm dim and
the time-sensitive recall→manufacturer join (Phase 6 / ADR 0035 — NOT built on
this branch).

Field naming mirrors the validated probe parser's ``_LABEL_MAP``
(``scripts/uscg/probe_mic_reassignment_rate.py``), promoted to the production
extractor. Two fields use validation aliases (cross-source convention): the MIC
column header is ``"MIC"`` → parser key ``"mic"`` → ``source_recall_id``; the
``"Company"`` label → parser key ``"company"`` → ``company_name`` (aligns with
``firm.sql`` terminology, same as the listing schema).

Date handling (ADR 0027 storage-forced): the detail page uses ``M/D/YYYY`` for
``In Business`` / ``Out of Business`` / ``Date Modified`` — the same format as
the USCG recalls details page. A nullable ``BeforeValidator`` coerces to UTC
midnight ``datetime``. Per the project convention (implementation_plan.md "Name
validators per-source"), the parser is defined locally rather than imported from
``uscg.py``. **Value caveat (NOT a storage concern):** ``in_business`` is
contaminated by record-touch dates on active firms (MERCURY / VOLVO PENTA /
CATERPILLAR show ``in_business ≈ date_modified ≈ 2025/2026``; defunct 4WN shows a
real ``1972``) — silver must not treat it as a "founded" date in isolation (§M.6).

``ConfigDict(extra='forbid', strict=True)`` per ADR 0014: a new/relabeled detail
field surfaces as a schema-fail re-ingest. The extractor's RAISE-on-unknown-label
drift fence is the first fence; this is the second.

No ``populate_by_name`` — the schema is only constructed from extractor dicts
keyed by the validation aliases (and field names for the rest).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field


def _parse_nullable_mfr_detail_date(v: Any) -> datetime | None:
    """Parse ``M/D/YYYY`` (manufacturer detail-page format) → UTC midnight datetime.

    Treats ``None`` and empty string as missing. Same format as the USCG recalls
    details page; ``strptime("%m/%d/%Y")`` accepts both zero-padded and
    non-zero-padded month/day (``"5/29/2026"`` and ``"05/29/2026"``). Storage-forced
    per ADR 0027; sentinel/semantic normalization lives in
    ``stg_uscg_manufacturer_details.sql``.
    """
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo is not None else v.replace(tzinfo=UTC)
    if isinstance(v, str):
        return datetime.strptime(v, "%m/%d/%Y").replace(tzinfo=UTC)
    raise ValueError(f"Cannot parse {v!r} as USCG manufacturer detail date — expected M/D/YYYY")


_NullableMfrDetailDate = Annotated[
    datetime | None, BeforeValidator(_parse_nullable_mfr_detail_date)
]


class UscgManufacturerDetailRecord(BaseModel):
    """Bronze-layer schema for one USCG manufacturer detail page.

    Identity is ``source_recall_id`` (= MIC, the regulatory key, parsed from the
    detail page itself). ``uscg_directory_id`` + ``detail_url`` are lineage added
    by the extractor from the work-list (hash-excluded in ``load_bronze``). All
    domain fields are nullable — the detail page leaves many cells blank, and the
    source uses ``'-'`` / ``'UNK'`` / ``''`` / ``'-, -'`` sentinels preserved
    verbatim at bronze (ADR 0027) and normalized in staging.
    """

    # populate_by_name=True so quarantine-recovery can model_validate a dumped payload
    # (field names), not only the extractor's alias-keyed dicts. Non-breaking: ingestion
    # passes alias keys. See src/bronze/recovery.py.
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)

    # --- Identity ---
    source_recall_id: str = Field(validation_alias="mic")

    # --- Detail-page text fields (all nullable) ---
    company_name: str | None = Field(default=None, validation_alias="company")
    dba: str | None = Field(default=None)
    parent_company: str | None = Field(default=None)
    parent_mic: str | None = Field(default=None)
    # Source-native succession lineage. 'COMPANY (OOB YYYY)' / 'COMPANY (OOB)' /
    # bare 'COMPANY'. Only ~13 of 205 recycled MICs carry a parseable year (§M.6).
    past_company_1: str | None = Field(default=None)
    past_company_2: str | None = Field(default=None)
    past_company_3: str | None = Field(default=None)
    # Full UNTRUNCATED address (Path B payoff vs the listing's ~30-char
    # truncation, Finding F.1). May contain embedded newlines (Finding F.2).
    address: str | None = Field(default=None)
    city: str | None = Field(default=None)
    state: str | None = Field(default=None)
    # 9-digit hyphen-free US ZIPs + Canadian 6-char postal codes — stays str.
    zip: str | None = Field(default=None)
    country: str | None = Field(default=None)
    phone: str | None = Field(default=None)
    fax: str | None = Field(default=None)
    # Observed: 'In Business' / 'Inactive' / 'Federal or State Agency'. No
    # Literal at bronze — a new value trips the extractor drift fence instead.
    status: str | None = Field(default=None)
    # '-, -' sentinel ("no official recorded"); silver normalizes.
    company_official: str | None = Field(default=None)
    # Verbal vessel-type taxonomy (<br/>-concatenated run-on); verbatim at bronze.
    type: str | None = Field(default=None)
    additional_address: str | None = Field(default=None)

    # --- Detail-page date fields (M/D/YYYY → UTC midnight) ---
    # in_business is contaminated by record-touch dates (see module docstring).
    in_business: _NullableMfrDetailDate = Field(default=None)
    # out_of_business (top-level) = the CURRENT holder is defunct — the SCD
    # valid_to for the current interval. Distinct from a Past Company '(OOB)'
    # (a PRIOR holder ceased → the MIC was recycled). Do NOT conflate.
    out_of_business: _NullableMfrDetailDate = Field(default=None)
    # The Path B change signal — INCLUDED in content_hash (NOT hash-excluded).
    date_modified: _NullableMfrDetailDate = Field(default=None)

    # --- Lineage (added by the extractor; hash-excluded in load_bronze) ---
    # USCG internal page-offset row id (= alphabetical rank), used to construct
    # the detail URL; page-offset-deterministic so excluded from content_hash.
    uscg_directory_id: int | None = Field(default=None)
    # The detail URL fetched for this record. Excluded from content_hash
    # (URL-scheme-rewrite defense; mirrors the listing schema).
    detail_url: str
