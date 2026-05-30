"""uscg_manufacturers_bronze, uscg_manufacturers_rejected

Revision ID: 0015
Revises: 0014
Create Date: 2026-05-30

Schema targets the listing-only extraction documented in
``documentation/uscg/manufacturer_scraping_observations.md`` (Phase 5d Step 7
Step 1):

- **Architecture**: HTML scrape, single bronze table. Incremental path walks
  ~651 listing pages of the manufacturer directory at
  ``https://uscgboating.org/content/manufacturers-identification.php``. NO
  per-manufacturer detail-page fetches (Finding C decision — listing-only
  for v1; address column truncated at ~30 chars per Finding F.1 source-DB
  VARCHAR constraint, documented limitation). ADR 0007 content-hash dedup
  absorbs no-op runs; Finding J ``Records Found`` short-circuit (shared with
  USCG recalls via migration 0014 — ``source_watermarks.last_records_count``
  and ``extraction_runs.was_short_circuited`` are generic at the column
  level and accept any source) skips the walk when the directory hasn't
  changed since last run.

- **Identity**: ``source_recall_id`` (= MIC, USCG's regulatory 3-character
  alphanumeric Manufacturer Identification Code per USCG-2013-0133-0005).
  Single-column identity. Validated in Finding B — the URL ``id`` query
  parameter (captured as ``uscg_directory_id``) is a separate page-offset-
  deterministic internal row number, NOT a stable surrogate key.

- **Nullability**:
    - **Required**: ``source_recall_id`` (MIC is always present per Finding B).
      ``detail_url`` (every row's MIC cell wraps an anchor per Finding B; the
      absolutized href is always present).
    - **Nullable**: ``company_name``, ``address``, ``city``, ``state``
      (per Finding F.3 — sentinel patterns ``"UNK"``, ``"-"``, empty string
      appear in the corpus; bronze preserves verbatim, silver normalizes via
      multi-pattern nullif). ``uscg_directory_id`` (defensive — parse failure
      on the href ``id=`` query parameter leaves NULL).

- **Storage-forced types per ADR 0027**:
    - All strings → ``TEXT``. No bools. No ``VARCHAR(N)`` narrowing.
    - ``uscg_directory_id`` → ``INTEGER`` (USCG's internal sequential row PK).

- **No new ``extraction_runs`` columns**. The forensic columns (migrations
  0010-0014) cover the manufacturer directory's needs identically to USCG
  recalls. ``response_inner_content_sha256`` stays NULL (HTML has no
  wrapper/inner distinction); ``response_etag`` / ``response_last_modified``
  stay NULL by source design (Finding E — server emits no validators,
  ``Cache-Control: no-store, no-cache, must-revalidate``).

- **No new ``source_watermarks`` columns**. ``last_records_count`` (column
  added in migration 0014 for USCG recalls' Finding J short-circuit) is
  generic at the table level — accepts any source. ``uscg_manufacturers``
  becomes its second consumer in v1.

- **Companion seed migration 0016** adds the ``uscg_manufacturers`` row to
  ``source_watermarks`` so the FK on ``extraction_runs.source`` is satisfied
  for run-record inserts (same pattern as 0008 for ``usda_establishments``).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations._columns import rejected_table_columns

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "uscg_manufacturers_bronze",
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
        # --- Listing-derived nullable fields (per Step 1 Finding F.3) ---
        sa.Column("company_name", sa.Text, nullable=True),
        sa.Column("address", sa.Text, nullable=True),
        sa.Column("city", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=True),
        # USCG internal sequential row PK parsed from the MIC anchor's
        # ``?id=`` query param. Page-offset-deterministic (Finding B), so
        # excluded from content_hash to prevent re-crawl churn when rows
        # are added/removed earlier in the alphabetical ordering.
        sa.Column("uscg_directory_id", sa.Integer, nullable=True),
        # Full detail URL — absolutized from each row's MIC anchor href.
        # Excluded from content_hash via ``hash_exclude_fields`` in
        # ``UscgManufacturerExtractor.load_bronze`` (defense against future
        # URL-scheme rewrites; mirrors recalls' ``details_url`` exclusion).
        sa.Column("detail_url", sa.Text, nullable=False),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id.
    # Also covers MIC lookups (source_recall_id IS the mic per Finding B).
    op.execute(
        "CREATE INDEX ix_uscg_manufacturers_bronze_id_ts "
        "ON uscg_manufacturers_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # State-based filter is the most common downstream analytical pattern
    # (e.g., "manufacturers in Florida" landing-page slices). Cardinality is
    # low (~50 US states + ~10 Canadian provinces); btree is fine.
    op.execute(
        "CREATE INDEX ix_uscg_manufacturers_bronze_state ON uscg_manufacturers_bronze (state)"
    )

    op.create_table("uscg_manufacturers_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("uscg_manufacturers_rejected")
    op.execute("DROP INDEX IF EXISTS ix_uscg_manufacturers_bronze_state")
    op.execute("DROP INDEX IF EXISTS ix_uscg_manufacturers_bronze_id_ts")
    op.drop_table("uscg_manufacturers_bronze")
