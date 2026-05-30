"""uscg_manufacturer_details_bronze, uscg_manufacturer_details_rejected

Revision ID: 0017
Revises: 0016
Create Date: 2026-05-30

Phase 5d Step 7 (detail) — bronze landing for the USCG manufacturer
**detail-page** payload (Path B), per
``project_scope/phase-5d-uscg-manufacturers-detail.md`` and
``documentation/uscg/manufacturer_scraping_observations.md`` §M.

- **Architecture**: a SEPARATE source/table from the listing-only
  ``uscg_manufacturers_bronze`` (migration 0015). The detail extractor
  (``UscgManufacturerDetailExtractor``) sources a work-list of
  ``(source_recall_id, uscg_directory_id, detail_url)`` tuples from
  ``uscg_manufacturers_bronze`` and fetches one detail page per MIC at
  ``manufacturers-identification-detail.php?id=N`` (confirmed direct-GET,
  §M.2). It does NOT widen the listing table (different fetch grain,
  different content_hash field set, different cadence; 0015 is closed).

- **Identity**: ``source_recall_id`` (= MIC) — joinable to
  ``uscg_manufacturers_bronze`` on ``source_recall_id``. ``uscg_directory_id``
  is the page-offset-deterministic ``?id=`` parameter (lineage only;
  hash-excluded in the extractor's ``load_bronze``, same as 0015).

- **Detail payload columns** (the ~20 fields the detail page exposes beyond
  the 5 listing fields; §M.2). The succession lineage (``past_company_1/2/3``,
  ``out_of_business``, ``in_business``, ``date_modified``) is the whole point
  of Path B — it feeds the eventual SCD-2 ``firm_manufacturer_attributes``
  dim and the time-sensitive recall→manufacturer join (Phase 6 / ADR 0035;
  NOT built here).

- **Storage-forced types per ADR 0027**:
    - All strings → ``TEXT`` (no VARCHAR narrowing). ``zip`` stays TEXT — it
      holds 9-digit hyphen-free US ZIPs (e.g. ``"561640126"``) and Canadian
      6-char postal codes (e.g. ``"V8L3S1"``); a numeric type would corrupt both.
    - ``in_business`` / ``out_of_business`` / ``date_modified`` → TIMESTAMPTZ
      (the schema's M/D/YYYY ``BeforeValidator`` coerces; same storage-forced
      pattern as ``uscg_recalls_bronze`` dates). ``in_business`` is contaminated
      by record-touch dates on active firms (§M.6) — a VALUE caveat for silver,
      not a storage concern.
    - ``uscg_directory_id`` → ``INTEGER``.

- **No DB CHECK on ``status``** — observed values {``In Business``,
  ``Inactive``, ``Federal or State Agency``}; a new value must be surfaced by
  the extractor's RAISE-on-unknown-label drift fence, not a DB constraint that
  would error the bulk load.

- **No new ``extraction_runs`` / ``source_watermarks`` columns** — the existing
  forensic columns (migrations 0010-0014) cover the detail source identically.
  ``response_etag`` / ``response_last_modified`` / ``response_inner_content_sha256``
  stay NULL by source design (Finding E).

- **Companion seed migration 0018** adds the ``uscg_manufacturer_details`` row
  to ``source_watermarks`` so the ``extraction_runs.source`` FK is satisfied
  for run-record inserts (same pattern as 0016 for ``uscg_manufacturers``).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations._columns import rejected_table_columns

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "uscg_manufacturer_details_bronze",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        # Dedup / lineage columns (ADR 0007)
        sa.Column("source_recall_id", sa.Text, nullable=False),
        sa.Column("content_hash", sa.Text, nullable=False),
        sa.Column(
            "extraction_timestamp",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("raw_landing_path", sa.Text, nullable=False),
        # --- Detail-page text fields (all nullable; bronze preserves verbatim
        #     per ADR 0027, silver normalizes sentinels '-', 'UNK', '', '-, -') ---
        sa.Column("company_name", sa.Text, nullable=True),
        sa.Column("dba", sa.Text, nullable=True),
        sa.Column("parent_company", sa.Text, nullable=True),
        sa.Column("parent_mic", sa.Text, nullable=True),
        sa.Column("past_company_1", sa.Text, nullable=True),
        sa.Column("past_company_2", sa.Text, nullable=True),
        sa.Column("past_company_3", sa.Text, nullable=True),
        # Full, UNTRUNCATED address (the Path B payoff vs the listing's
        # ~30-char truncation, Finding F.1). May contain embedded newlines.
        sa.Column("address", sa.Text, nullable=True),
        sa.Column("city", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=True),
        # TEXT — 9-digit hyphen-free US ZIPs + Canadian 6-char postal codes.
        sa.Column("zip", sa.Text, nullable=True),
        sa.Column("country", sa.Text, nullable=True),
        sa.Column("phone", sa.Text, nullable=True),
        sa.Column("fax", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=True),
        # '-, -' sentinel observed ("no official recorded"); silver normalizes.
        sa.Column("company_official", sa.Text, nullable=True),
        # Verbal vessel-type taxonomy (<br/>-concatenated run-on); verbatim.
        sa.Column("type", sa.Text, nullable=True),
        sa.Column("additional_address", sa.Text, nullable=True),
        # --- Detail-page date fields (M/D/YYYY coerced to TIMESTAMPTZ) ---
        sa.Column("in_business", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("out_of_business", sa.TIMESTAMP(timezone=True), nullable=True),
        # The Path B change signal — INCLUDED in content_hash (not excluded).
        sa.Column("date_modified", sa.TIMESTAMP(timezone=True), nullable=True),
        # --- Lineage (hash-excluded in load_bronze, same as 0015) ---
        sa.Column("uscg_directory_id", sa.Integer, nullable=True),
        sa.Column("detail_url", sa.Text, nullable=False),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id.
    op.execute(
        "CREATE INDEX ix_uscg_manufacturer_details_bronze_id_ts "
        "ON uscg_manufacturer_details_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Geographic slices (state/zip) are first-class on the detail table (unlike
    # the listing, whose address was truncated text). Low-cardinality btree.
    op.execute(
        "CREATE INDEX ix_uscg_manufacturer_details_bronze_state "
        "ON uscg_manufacturer_details_bronze (state)"
    )

    op.create_table("uscg_manufacturer_details_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("uscg_manufacturer_details_rejected")
    op.execute("DROP INDEX IF EXISTS ix_uscg_manufacturer_details_bronze_state")
    op.execute("DROP INDEX IF EXISTS ix_uscg_manufacturer_details_bronze_id_ts")
    op.drop_table("uscg_manufacturer_details_bronze")
