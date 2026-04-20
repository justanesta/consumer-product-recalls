"""cpsc_recalls_bronze and cpsc_recalls_rejected tables

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-20

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migrations._columns import rejected_table_columns

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "cpsc_recalls_bronze",
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
        # Required scalars
        sa.Column("recall_id", sa.Integer, nullable=True),
        sa.Column("recall_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_publish_date", sa.TIMESTAMP(timezone=True), nullable=True),
        # Optional scalars
        sa.Column("title", sa.Text, nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("url", sa.Text, nullable=True),
        sa.Column("consumer_contact", sa.Text, nullable=True),
        # Collections stored as JSONB (silver dbt models parse these)
        sa.Column("products", postgresql.JSONB, nullable=True),
        sa.Column("manufacturers", postgresql.JSONB, nullable=True),
        sa.Column("retailers", postgresql.JSONB, nullable=True),
        sa.Column("importers", postgresql.JSONB, nullable=True),
        sa.Column("distributors", postgresql.JSONB, nullable=True),
        sa.Column("manufacturer_countries", postgresql.JSONB, nullable=True),
        sa.Column("product_upcs", postgresql.JSONB, nullable=True),
        sa.Column("hazards", postgresql.JSONB, nullable=True),
        sa.Column("remedies", postgresql.JSONB, nullable=True),
        sa.Column("remedy_options", postgresql.JSONB, nullable=True),
        sa.Column("in_conjunctions", postgresql.JSONB, nullable=True),
        sa.Column("images", postgresql.JSONB, nullable=True),
        sa.Column("injuries", postgresql.JSONB, nullable=True),
    )

    # Supports BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id
    op.execute(
        "CREATE INDEX ix_cpsc_recalls_bronze_id_ts "
        "ON cpsc_recalls_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Supports incremental filtering by last_publish_date
    op.execute(
        "CREATE INDEX ix_cpsc_recalls_bronze_last_publish_date "
        "ON cpsc_recalls_bronze (last_publish_date)"
    )

    op.create_table("cpsc_recalls_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("cpsc_recalls_rejected")
    op.execute("DROP INDEX IF EXISTS ix_cpsc_recalls_bronze_last_publish_date")
    op.execute("DROP INDEX IF EXISTS ix_cpsc_recalls_bronze_id_ts")
    op.drop_table("cpsc_recalls_bronze")
