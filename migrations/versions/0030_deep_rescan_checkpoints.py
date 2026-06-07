"""deep_rescan_checkpoints — durable resume cursor for batched deep-rescan sweeps

Revision ID: 0030
Revises: 0029
Create Date: 2026-06-07

Generalizes the FDA press-release seed's resume mechanism (project_scope/
fda-press-release-seed-plan.md, S1). A batched deep-rescan lands+loads every N events and
co-commits its position here in the SAME transaction as the bronze load, so:

  - resume reads the cursor from the DB, not from grepping a log — empty events leave no
    bronze row, so the cursor was never recoverable from bronze (the old chunk-script gap);
  - the cursor can never lead committed rows (same txn), so a crash costs at most one
    partial batch.

Keyed ``(source, change_type)``. ``cursor`` is an opaque per-source JSONB so any future
cursor-ordered sweep can reuse the table. The FDA press-release seed stores
``{"init_dt": "YYYY-MM-DD", "event_id": <int>}`` (recent-first by recall_initiation_dt,
event_id tiebreak; NULL initiation dates coalesce to a min sentinel and sort last).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0030"
down_revision: str | None = "0029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deep_rescan_checkpoints",
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("change_type", sa.Text, nullable=False),
        # Opaque per-source resume cursor (see module docstring).
        sa.Column("cursor", postgresql.JSONB, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="in_progress"),
        sa.Column("batches_done", sa.Integer, nullable=False, server_default="0"),
        sa.Column("events_processed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("rows_loaded", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "started_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("source", "change_type"),
        sa.CheckConstraint(
            "status in ('in_progress', 'complete')",
            name="ck_deep_rescan_checkpoints_status",
        ),
    )


def downgrade() -> None:
    op.drop_table("deep_rescan_checkpoints")
