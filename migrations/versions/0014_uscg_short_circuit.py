"""USCG short-circuit support columns (Phase 5d Step 6)

Revision ID: 0014
Revises: 0013
Create Date: 2026-05-17

Adds two nullable columns supporting the USCG Finding J short-circuit:

- ``source_watermarks.last_records_count`` (INTEGER, nullable) — the
  ``Records Found: NNNN`` total observed on the last successful USCG
  ``extract`` run. Used as Gate 1 of the page-0 precheck: if the current
  total equals the prior total, no recalls were added or removed.

- ``extraction_runs.was_short_circuited`` (BOOLEAN, nullable) — flags
  rows where ``UscgScrapingExtractor.extract()`` short-circuited based on
  the two-gate precheck (count + listing-row-ID membership). Distinguishes
  a ~3-second short-circuit from a ~36-min full walk that happened to find
  0 deltas; required so operations can answer "when did USCG last do a
  full walk?" (the weekly safety-net deep-rescan cadence verification).

Both columns are nullable because USCG is the only source consumer in v1
(other sources keep both as NULL). The CHECK constraints / NOT NULL
defaults that would imply a project-wide invariant are deliberately not
added — extending these signals to other sources is a future-source
decision.

Forward-only by convention. The downgrade body exists for completeness.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "source_watermarks",
        sa.Column(
            "last_records_count",
            sa.Integer,
            nullable=True,
            comment=(
                "Last 'Records Found: NNNN' total observed on a successful run. "
                "USCG-only in v1 (Phase 5d Step 6 short-circuit); other sources "
                "keep NULL. Gate 1 of the page-0 precheck."
            ),
        ),
    )
    op.add_column(
        "extraction_runs",
        sa.Column(
            "was_short_circuited",
            sa.Boolean,
            nullable=True,
            comment=(
                "True when extract() returned [] via the page-0 precheck "
                "(USCG Finding J Step 6). NULL for sources that don't implement "
                "a short-circuit (CPSC/FDA/USDA/NHTSA). Use this to distinguish "
                "short-circuited runs from full-walk-with-0-inserts runs and "
                "to compute the empirical hit-rate after a few weeks of "
                "daily cadence."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("extraction_runs", "was_short_circuited")
    op.drop_column("source_watermarks", "last_records_count")
