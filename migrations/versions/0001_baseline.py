"""Baseline migration: source_watermarks and extraction_runs

Revision ID: 0001
Revises:
Create Date: 2026-04-19

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SOURCES = ["cpsc", "fda", "usda", "nhtsa", "uscg"]


def upgrade() -> None:
    source_watermarks = op.create_table(
        "source_watermarks",
        sa.Column("source", sa.Text, primary_key=True),
        sa.Column("last_successful_extract_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_cursor", sa.Text, nullable=True),
        sa.Column("last_etag", sa.Text, nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.bulk_insert(
        source_watermarks,
        [{"source": s} for s in _SOURCES],
    )

    op.create_table(
        "extraction_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "source",
            sa.Text,
            sa.ForeignKey("source_watermarks.source", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "status",
            sa.Text,
            nullable=False,
            comment="One of: running, success, failed, aborted",
        ),
        sa.Column("records_extracted", sa.Integer, nullable=True),
        sa.Column("records_inserted", sa.Integer, nullable=True),
        sa.Column("records_rejected", sa.Integer, nullable=True),
        sa.Column("run_id", sa.Text, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("raw_landing_path", sa.Text, nullable=True),
    )

    # Supports watermark cursor queries: latest run per source ordered by time.
    op.execute(
        "CREATE INDEX ix_extraction_runs_source_started_at "
        "ON extraction_runs (source, started_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_extraction_runs_source_started_at")
    op.drop_table("extraction_runs")
    op.drop_table("source_watermarks")
