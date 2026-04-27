"""fda_recalls_bronze and fda_recalls_rejected tables

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-27

Schema targets the bulk POST /recalls/ object-array response shape. Columns mirror
the displaycolumns requested by FdaExtractor plus RID (auto-injected by the API).
Almost all fields nullable per api_observations.md finding M-extension — older records
(pre-2010) have null values for fields that became standard in later records.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from migrations._columns import rejected_table_columns

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fda_recalls_bronze",
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
        # Core identifiers — non-nullable in the bronze schema
        sa.Column("recall_event_id", sa.BigInteger, nullable=False),
        sa.Column("rid", sa.Integer, nullable=True),
        sa.Column("center_cd", sa.Text, nullable=False),
        sa.Column("product_type_short", sa.Text, nullable=False),
        # Incremental watermark column (ADR 0010: eventlmd >= watermark filter)
        sa.Column("event_lmd", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("firm_legal_nam", sa.Text, nullable=False),
        # Nullable scalars — see api_observations.md findings J, M, M-extension
        sa.Column("firm_fei_num", sa.BigInteger, nullable=True),
        sa.Column("recall_num", sa.Text, nullable=True),
        sa.Column("phase_txt", sa.Text, nullable=True),
        sa.Column("center_classification_type_txt", sa.Text, nullable=True),
        sa.Column("recall_initiation_dt", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("center_classification_dt", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("termination_dt", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("enforcement_report_dt", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("determination_dt", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("initial_firm_notification_txt", sa.Text, nullable=True),
        sa.Column("distribution_area_summary_txt", sa.Text, nullable=True),
        sa.Column("voluntary_type_txt", sa.Text, nullable=True),
        sa.Column("product_description_txt", sa.Text, nullable=True),
        sa.Column("product_short_reason_txt", sa.Text, nullable=True),
        # Free-text quantity — e.g. "2324 units", "4,291,797" (finding in api_observations.md)
        sa.Column("product_distributed_quantity", sa.Text, nullable=True),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id
    op.execute(
        "CREATE INDEX ix_fda_recalls_bronze_id_ts "
        "ON fda_recalls_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Incremental watermark filtering by event_lmd
    op.execute("CREATE INDEX ix_fda_recalls_bronze_event_lmd ON fda_recalls_bronze (event_lmd)")

    # Silver join from fda_recalls_bronze to fda_recalls_bronze by recall_event_id
    op.execute(
        "CREATE INDEX ix_fda_recalls_bronze_recall_event_id ON fda_recalls_bronze (recall_event_id)"
    )

    op.create_table("fda_recalls_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("fda_recalls_rejected")
    op.execute("DROP INDEX IF EXISTS ix_fda_recalls_bronze_recall_event_id")
    op.execute("DROP INDEX IF EXISTS ix_fda_recalls_bronze_event_lmd")
    op.execute("DROP INDEX IF EXISTS ix_fda_recalls_bronze_id_ts")
    op.drop_table("fda_recalls_bronze")
