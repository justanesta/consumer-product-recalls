"""uscg_recalls_bronze, uscg_recalls_rejected

Revision ID: 0013
Revises: 0012
Create Date: 2026-05-16

Schema targets the listing + details merge documented in
``documentation/uscg/scraping_observations.md`` (Phase 5d Step 1):

- **Architecture**: HTML scrape, single bronze table. Incremental path
  fetches all 71 listing pages + 1,763 details pages every run. ADR 0007
  content-hash dedup absorbs no-op weeks. Deep-rescan path uses the same
  fetch shape but disables watermark advance (per
  ``UscgDeepRescanLoader`` — symmetry with NHTSA/FDA/USDA).

- **Identity**: ``source_recall_id`` (USCG's "Number" column,
  year-prefix encoding e.g. ``26MF0158`` / ``25CG0017``). Single-column
  identity validated in Step 1 Finding C — uniqueness probe across the
  full corpus deferred to Step 1.5 (after this migration lands + the
  first extraction completes).

- **Nullability** follows Step 1 sample probes + Step 3 first-extraction
  corpus evidence:
    - **Required**: ``source_recall_id`` (always present — anchor field).
    - **Nullable**: everything else. ``opened_on`` per Finding A scope
      caveat + Finding O Unix-epoch sentinel pattern (USCG listing
      renders ``"1970-01-01"`` for no-date case). ``company_name`` per
      Finding P (33/1763 ≈ 1.9% empty in corpus, mostly pre-2005;
      R2-byte-confirmed on ``920542T``). Two probed details pages had
      empty ``problem_2`` + ``severity`` + ``campaign_close_date``, plus
      ``model_year`` empty on the 26MF0158 sample — marking those
      required would quarantine real recalls.

- **Storage-forced types per ADR 0027**:
    - ``opened_on`` (listing), ``case_open_date`` / ``case_close_date`` /
      ``campaign_open_date`` / ``campaign_close_date`` / ``last_date``
      (all details) → ``TIMESTAMPTZ``. Listing format is ``YYYY-MM-DD``;
      details format is ``M/D/YYYY`` (Finding F). Two distinct
      ``BeforeValidator``s in ``src/schemas/uscg.py``.
    - ``units`` → ``TEXT`` (not ``INTEGER``); silver casts to int per
      ADR 0027.
    - All other strings → ``TEXT``. No bools. No ``VARCHAR(N)``
      narrowing observed yet — preserve as ``TEXT`` until a length
      invariant is documented.

- **No new ``extraction_runs`` columns**. USCG populates
  ``response_status_code`` / ``response_etag`` / ``response_last_modified``
  / ``response_body_sha256`` / ``response_headers`` from the page-0
  listing response (existing migration 0010 columns). USCG leaves
  ``response_inner_content_sha256`` NULL — HTML pages have no
  wrapper/inner distinction. The full HTML-forensics column redesign
  (implementation_plan item #9) is deferred until Step 3 surfaces real
  operational needs.

- **No ``source_watermarks`` seed**. Migration 0001's ``_SOURCES`` list
  already pre-seeded ``"uscg"`` (along with the other 4 sources) at
  project inception. The watermark row exists for USCG before this
  migration runs.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations._columns import rejected_table_columns

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "uscg_recalls_bronze",
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
        # --- Listing-derived fields (all nullable per Step 3 corpus evidence) ---
        # ``company_name`` nullable per Finding P — 33/1763 (~1.9%) of
        # corpus rows have empty ``Company:`` cells, mostly pre-2005.
        # R2 byte-inspection of ``920542T`` confirmed the cell is
        # literally empty (Step 3, 2026-05-17).
        sa.Column("company_name", sa.Text, nullable=True),
        # ``opened_on`` is listing's "Opened On" (YYYY-MM-DD format) —
        # listing parser observed it populated 38/38 in the Step 1
        # sample but Step 3 surfaced Finding O: USCG renders
        # ``"1970-01-01"`` as the listing-column sentinel for no-date
        # cases while leaving details' Case Open Date empty (R2 byte
        # confirmed on listing page 31, recalls 22MF0627-9). Distinct
        # from details' ``case_open_date`` (M/D/YYYY format) which
        # captures the same semantic value via a different encoding;
        # bronze keeps both for cross-format lineage; silver maps
        # ``1970-01-01`` → NULL.
        sa.Column("opened_on", sa.TIMESTAMP(timezone=True), nullable=True),
        # --- Listing-derived nullable fields ---
        sa.Column("mic", sa.Text, nullable=True),
        sa.Column("model_name", sa.Text, nullable=True),
        sa.Column("problem_1", sa.Text, nullable=True),
        # --- Lineage column ---
        # ``details_url`` is preserved for forensics — the recalls-details.php
        # link as scraped. Excluded from content_hash via
        # ``hash_exclude_fields`` in load_bronze to defend against future
        # URL-scheme rewrites.
        sa.Column("details_url", sa.Text, nullable=False),
        # --- Details-derived nullable fields ---
        sa.Column("company_official", sa.Text, nullable=True),
        sa.Column("model_year", sa.Text, nullable=True),
        sa.Column("problem_2", sa.Text, nullable=True),
        sa.Column("hin", sa.Text, nullable=True),
        sa.Column("case_open_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("disposition", sa.Text, nullable=True),
        sa.Column("case_close_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("units", sa.Text, nullable=True),
        sa.Column("campaign_open_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("boat_type", sa.Text, nullable=True),
        sa.Column("campaign_close_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("severity", sa.Text, nullable=True),
        sa.Column("last_date", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id.
    op.execute(
        "CREATE INDEX ix_uscg_recalls_bronze_id_ts "
        "ON uscg_recalls_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Analytical grouping by manufacturer code. Multiple recalls per MIC
    # is common (e.g., Volvo has multiple recall numbers); indexed for
    # downstream silver joins on manufacturer-entity resolution.
    op.execute("CREATE INDEX ix_uscg_recalls_bronze_mic ON uscg_recalls_bronze (mic)")

    op.create_table("uscg_recalls_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("uscg_recalls_rejected")
    op.execute("DROP INDEX IF EXISTS ix_uscg_recalls_bronze_mic")
    op.execute("DROP INDEX IF EXISTS ix_uscg_recalls_bronze_id_ts")
    op.drop_table("uscg_recalls_bronze")
