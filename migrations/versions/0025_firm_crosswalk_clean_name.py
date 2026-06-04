"""firm_crosswalk: add clean_name + extracted_dba (Phase 6b PR 6b.1)

Revision ID: 0025
Revises: 0024
Create Date: 2026-06-03

PR 6b.1 folds CPSC firm-name CLEANING into the same crosswalk the 6b.4 RapidFuzz
resolver writes — one Python "name resolution" surface (raw -> clean -> cluster ->
canonical). The clean half lands now: ``recalls resolve-firms`` maps each distinct
CPSC firm name through ``src.enrichment.firm_normalization`` and writes the result
here, with ``canonical_firm_id = md5(upper(trim(clean_name)))`` so cleaning alone
already merges raw variants ("Fisher-Price of East Aurora, N.Y." + "Fisher-Price").
6b.4 later replaces ``canonical_firm_id`` with the fuzzy cluster representative — no
further schema change.

Adds two nullable columns to the (empty, 0024-created) table:
- ``clean_name``   — the canonical legal name from ``clean_firm_name`` (display/debug;
  ``canonical_firm_id`` is its md5).
- ``extracted_dba``— the DBA brand from ``extract_firm_dba`` (feeds
  ``firm.alternate_names``); NULL for the majority with no DBA.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025"
down_revision: str | None = "0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("firm_crosswalk", sa.Column("clean_name", sa.Text, nullable=True))
    op.add_column("firm_crosswalk", sa.Column("extracted_dba", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("firm_crosswalk", "extracted_dba")
    op.drop_column("firm_crosswalk", "clean_name")
