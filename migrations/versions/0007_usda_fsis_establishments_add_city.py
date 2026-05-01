"""add city column to usda_fsis_establishments_bronze

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-01

The `city` field was a Finding D blind spot — the original cardinality probe in
documentation/usda/establishment_api_observations.md did not enumerate it, but
the live API returns it on every record. Phase 5b.2 first live extraction
(2026-05-01) rejected 100% of 7,945 records on `extra_forbidden city` per the
schema's `extra='forbid'` posture (ADR 0014). This migration adds the column to
bronze so the corrected schema can land records.

The column is created NOT NULL because (a) the rejection sample of 7,945
records confirmed 0% empty rate, and (b) the bronze table currently holds zero
rows so there is nothing to backfill. If a future record arrives with a missing
city, the schema validator quarantines it (ADR 0013) — that's the desired
signal, not a reason to relax the constraint.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "usda_fsis_establishments_bronze",
        sa.Column("city", sa.Text, nullable=False),
    )


def downgrade() -> None:
    op.drop_column("usda_fsis_establishments_bronze", "city")
