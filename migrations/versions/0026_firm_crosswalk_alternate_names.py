"""firm_crosswalk: replace extracted_dba with alternate_names (Phase 6b PR 6b.4)

Revision ID: 0026
Revises: 0025
Create Date: 2026-06-04

PR 6b.4 scoped the deterministic parenthetical strip OUT of the cross-source cleaner
(ADR 0037 — a blanket strip proved too blunt; RapidFuzz handles paren-variants). The
flip side is that brand-bearing parentheticals ("Deere & Company (John Deere)") are now
CAPTURED as alternate names rather than discarded. A firm can have several aliases (the
DBA brand + one or more paren brands), so the single ``extracted_dba`` text column is
replaced by a jsonb array ``alternate_names``.

- drop ``extracted_dba`` (text, single) — subsumed by ``alternate_names``.
- add ``alternate_names`` (jsonb, nullable) — the resolver writes a JSON array of aliases
  (or NULL when none); silver ``firm.sql`` flattens + de-dupes them across a canonical.

The crosswalk is rebuilt by ``recalls resolve-firms`` (truncate-and-reload), so no data
backfill is needed — the next resolver run repopulates the new column.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0026"
down_revision: str | None = "0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("firm_crosswalk", "extracted_dba")
    op.add_column(
        "firm_crosswalk",
        sa.Column("alternate_names", postgresql.JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("firm_crosswalk", "alternate_names")
    op.add_column("firm_crosswalk", sa.Column("extracted_dba", sa.Text, nullable=True))
