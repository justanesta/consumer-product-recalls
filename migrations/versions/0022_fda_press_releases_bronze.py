"""fda_press_releases_bronze, fda_press_releases_rejected

Revision ID: 0022
Revises: 0021
Create Date: 2026-06-03

Tier-3 press-release capture (capture-expansion (b) PR), per
``project_scope/silver-field-capture-expansion-plan.md`` (Part C) and
``documentation/fda/api_observations.md`` Finding K0 (press-release URLs are
lookup-endpoint-only — the bulk POST 406s them).

- **Architecture**: a SEPARATE, event-grain child table. The press-release
  extractor (``FdaPressReleaseExtractor``, W3) sources a work-list of distinct
  ``recall_event_id`` from ``fda_recalls_bronze`` and fetches one
  ``GET /search/pressreleaseurls/{eventid}`` per event — the repo's first
  per-record REST fan-out (the analog of ``UscgManufacturerDetailExtractor``).

- **Identity**: ``(source_recall_id, press_release_url)`` — composite, because an
  event can carry several press releases (M:1). ``source_recall_id`` here is the
  RECALLEVENTID (the event), NOT a product id; joinable to
  ``fda_recalls_bronze.recall_event_id`` and to silver ``recall_event`` via
  ``md5('FDA' || '|' || source_recall_id)``.

- **Empty-result reality**: most events have NO press release. The extractor
  treats an empty RESULT as a successful no-op, so empty events produce no rows
  here — the table holds rows only where press releases exist. The incremental
  work-list is therefore cursor-driven (events whose recall changed since the
  press-release watermark), NOT "events lacking a row" (which would re-fetch the
  empty majority every run). See the plan's Part C reasoning.

- **Storage-forced types per ADR 0027**: url / type → TEXT (verbatim; silver
  normalizes ''). ``press_release_issued_dt`` → TIMESTAMPTZ (the schema's
  MM/DD/YYYY BeforeValidator coerces, same as the other FDA dates).

- **Companion seed migration 0023** adds the ``fda_press_releases`` row to
  ``source_watermarks`` (satisfies the ``extraction_runs.source`` FK + holds the
  incremental event_lmd cursor).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations._columns import rejected_table_columns

revision: str = "0022"
down_revision: str | None = "0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fda_press_releases_bronze",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        # Dedup / lineage columns (ADR 0007). source_recall_id = RECALLEVENTID.
        sa.Column("source_recall_id", sa.Text, nullable=False),
        sa.Column("content_hash", sa.Text, nullable=False),
        sa.Column(
            "extraction_timestamp",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("raw_landing_path", sa.Text, nullable=False),
        # --- Press-release payload (GET /search/pressreleaseurls/{eventid}) ---
        # press_release_url is the 2nd identity field → NOT NULL (a null-URL PR row
        # is meaningless and quarantines at validation).
        sa.Column("press_release_url", sa.Text, nullable=False),
        sa.Column("press_release_type", sa.Text, nullable=True),  # State / Firm / FDA
        sa.Column("press_release_issued_dt", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per composite identity.
    op.execute(
        "CREATE INDEX ix_fda_press_releases_bronze_identity_ts "
        "ON fda_press_releases_bronze "
        "(source_recall_id, press_release_url, extraction_timestamp DESC)"
    )

    op.create_table("fda_press_releases_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("fda_press_releases_rejected")
    op.execute("DROP INDEX IF EXISTS ix_fda_press_releases_bronze_identity_ts")
    op.drop_table("fda_press_releases_bronze")
