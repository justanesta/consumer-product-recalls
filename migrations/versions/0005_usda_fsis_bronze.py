"""usda_fsis_recalls_bronze and usda_fsis_recalls_rejected tables

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-30

Schema targets the GET /fsis/api/recall/v/1 flat-array response. Bilingual
companion records (English + Spanish) are sibling rows distinguished by
langcode and sharing source_recall_id (= field_recall_number per Finding I).

Almost all fields are nullable: 42% of records have an empty
field_last_modified_date (Finding C); ~13 other fields have non-zero empty
rates. Only the always-populated lifecycle fields (recall number, langcode,
title, recall_date, recall_type, recall_classification, archive_recall,
has_spanish, active_notice) are NOT NULL.

source_watermarks.last_etag was provisioned in 0001 baseline; this migration
does not need to add it.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations._columns import rejected_table_columns

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "usda_fsis_recalls_bronze",
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
        # Required identifiers and lifecycle (Finding C — 0% empty)
        sa.Column("langcode", sa.Text, nullable=False),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("recall_date", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("recall_type", sa.Text, nullable=False),
        sa.Column("recall_classification", sa.Text, nullable=False),
        sa.Column("archive_recall", sa.Boolean, nullable=False),
        sa.Column("has_spanish", sa.Boolean, nullable=False),
        sa.Column("active_notice", sa.Boolean, nullable=False),
        # Optional dates
        sa.Column("last_modified_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("closed_date", sa.TIMESTAMP(timezone=True), nullable=True),
        # Optional booleans
        sa.Column("related_to_outbreak", sa.Boolean, nullable=True),
        # Optional strings
        sa.Column("closed_year", sa.Text, nullable=True),
        sa.Column("year", sa.Text, nullable=True),
        sa.Column("risk_level", sa.Text, nullable=True),
        sa.Column("recall_reason", sa.Text, nullable=True),
        sa.Column("processing", sa.Text, nullable=True),
        sa.Column("states", sa.Text, nullable=True),
        sa.Column("establishment", sa.Text, nullable=True),
        sa.Column("labels", sa.Text, nullable=True),
        sa.Column("qty_recovered", sa.Text, nullable=True),
        sa.Column("summary", sa.Text, nullable=True),
        sa.Column("product_items", sa.Text, nullable=True),
        sa.Column("distro_list", sa.Text, nullable=True),
        sa.Column("media_contact", sa.Text, nullable=True),
        sa.Column("company_media_contact", sa.Text, nullable=True),
        sa.Column("recall_url", sa.Text, nullable=True),
        # Dead fields kept for shape parity (Finding C — 100% / 99.9% empty)
        sa.Column("en_press_release", sa.Text, nullable=True),
        sa.Column("press_release", sa.Text, nullable=True),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id.
    # NOTE: USDA's natural identity is (source_recall_id, langcode) — bilingual
    # siblings share source_recall_id but are separate logical records. The bronze
    # loader's dedup key is source_recall_id only, so two distinct rows with the
    # same recall number but different langcode WILL collide on the dedup query.
    # This is acceptable for now: their content hashes will differ (different
    # title, summary, product_items, recall_url per Finding F), so both are
    # treated as "changed" relative to the most recent row of that ID. Phase 6
    # silver model resolves bilingual pairs explicitly.
    op.execute(
        "CREATE INDEX ix_usda_fsis_recalls_bronze_id_ts "
        "ON usda_fsis_recalls_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Watermark / freshness queries: find latest last_modified_date observed.
    op.execute(
        "CREATE INDEX ix_usda_fsis_recalls_bronze_last_modified "
        "ON usda_fsis_recalls_bronze (last_modified_date)"
    )

    # Bilingual pair lookup in silver: find both EN and ES rows by recall number.
    op.execute(
        "CREATE INDEX ix_usda_fsis_recalls_bronze_recall_lang "
        "ON usda_fsis_recalls_bronze (source_recall_id, langcode)"
    )

    op.create_table("usda_fsis_recalls_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("usda_fsis_recalls_rejected")
    op.execute("DROP INDEX IF EXISTS ix_usda_fsis_recalls_bronze_recall_lang")
    op.execute("DROP INDEX IF EXISTS ix_usda_fsis_recalls_bronze_last_modified")
    op.execute("DROP INDEX IF EXISTS ix_usda_fsis_recalls_bronze_id_ts")
    op.drop_table("usda_fsis_recalls_bronze")
