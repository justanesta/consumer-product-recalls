"""Add change_type column to extraction_runs

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-01

Per ADR 0027 (bronze keeps storage-forced transforms only) and ADR 0028
(backfill / re-extraction semantics), distinguishes routine cron runs from
re-baseline / hash-helper / historical-seed waves so downstream history
models (Phase 6 recall_event_history) can filter parser-driven re-versions
out of edit detection.

Allowed values are enforced at the database level via a CHECK constraint
(per ADR 0028 §Negative consequences). Adding a new value in the future is
a one-line CHECK-constraint update in a follow-up migration.

The 'routine' default fills in for every existing extraction_runs row
without a separate backfill statement; existing cron workflows that never
pass --change-type continue marking runs correctly.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_ALLOWED_CHANGE_TYPES = (
    "routine",
    "schema_rebaseline",
    "hash_helper_rebaseline",
    "historical_seed",
)


def upgrade() -> None:
    op.add_column(
        "extraction_runs",
        sa.Column(
            "change_type",
            sa.Text,
            nullable=False,
            server_default=sa.text("'routine'"),
            comment=(
                "Categorizes the extraction wave: routine (default cron), "
                "schema_rebaseline (Pydantic normalizer change re-versions bronze), "
                "hash_helper_rebaseline (hashing.py change re-versions bronze), "
                "historical_seed (one-time backfill of pre-existing records). "
                "Phase 6 recall_event_history filters non-routine values."
            ),
        ),
    )

    allowed = ", ".join(f"'{v}'" for v in _ALLOWED_CHANGE_TYPES)
    op.create_check_constraint(
        "ck_extraction_runs_change_type",
        "extraction_runs",
        f"change_type IN ({allowed})",
    )


def downgrade() -> None:
    op.drop_constraint("ck_extraction_runs_change_type", "extraction_runs", type_="check")
    op.drop_column("extraction_runs", "change_type")
