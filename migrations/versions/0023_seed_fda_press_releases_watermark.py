"""seed source_watermarks row for fda_press_releases

Revision ID: 0023
Revises: 0022
Create Date: 2026-06-03

``extraction_runs.source`` is a FK to ``source_watermarks.source`` (baseline
0001). The press-release extractor (``FdaPressReleaseExtractor``, W3) needs its
``fda_press_releases`` row to (a) write run records and (b) hold the incremental
cursor (the max ``event_lmd`` processed, so the next incremental work-list is the
events whose recall changed since). Mirrors 0018 (``uscg_manufacturer_details``)
and 0016 (``uscg_manufacturers``).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0023"
down_revision: str | None = "0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO source_watermarks (source) VALUES ('fda_press_releases') "
            "ON CONFLICT (source) DO NOTHING"
        )
    )


def downgrade() -> None:
    # ondelete='RESTRICT' on extraction_runs.source means this DELETE fails if any
    # extraction_runs rows reference fda_press_releases — correct; downgrading would
    # orphan run history.
    op.execute(sa.text("DELETE FROM source_watermarks WHERE source = 'fda_press_releases'"))
