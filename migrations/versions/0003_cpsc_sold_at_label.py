"""add sold_at_label to cpsc_recalls_bronze

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-20

Field discovered during live cassette recording: the CPSC API returns SoldAtLabel
on every record but it was missing from the original schema, causing 100% rejection.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("cpsc_recalls_bronze", sa.Column("sold_at_label", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("cpsc_recalls_bronze", "sold_at_label")
