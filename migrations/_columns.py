from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def rejected_table_columns() -> list[sa.Column]:
    """
    Standard column set for per-source _rejected tables (ADR 0013, T1 quarantine).
    Use in source-specific migrations:

        op.create_table("cpsc_recalls_rejected", *rejected_table_columns())
    """
    return [
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("source_recall_id", sa.Text, nullable=False),
        sa.Column("raw_record", postgresql.JSONB, nullable=False),
        sa.Column("failure_reason", sa.Text, nullable=False),
        sa.Column(
            "failure_stage",
            sa.Text,
            nullable=False,
            comment="One of: extract, land_raw, validate_records, invariants, load_bronze",
        ),
        sa.Column(
            "rejected_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("raw_landing_path", sa.Text, nullable=True),
    ]
