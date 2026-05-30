"""seed source_watermarks row for uscg_manufacturers

Revision ID: 0016
Revises: 0015
Create Date: 2026-05-30

The baseline migration (0001) hardcoded a five-source list
``["cpsc", "fda", "usda", "nhtsa", "uscg"]`` and seeded ``source_watermarks``
with one row per source. ``extraction_runs.source`` is a FK back to that table,
so any extractor whose ``source_name`` isn't in ``source_watermarks`` cannot
write its run record (the FK insert silently fails — captured in
``_record_run``'s broad except clause; surfaced as the
``extraction_run.record_failed`` warning during first extraction).

This migration adds the ``uscg_manufacturers`` row so the run-recording write
succeeds (mirrors 0008 for ``usda_establishments``). ``uscg_manufacturers``
populates the ``last_records_count`` column (added in migration 0014 for the
Finding J short-circuit) but does NOT use ``last_cursor`` / ``last_etag`` —
the source emits no ETag / Last-Modified per Finding E of
``manufacturer_scraping_observations.md``.

Future sources will need the same one-row seed migration. A general fix
(replacing the FK with a soft enum or dropping the FK in favor of a CHECK
constraint) is filed as an architectural follow-up in implementation_plan.md.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO source_watermarks (source) VALUES ('uscg_manufacturers') "
            "ON CONFLICT (source) DO NOTHING"
        )
    )


def downgrade() -> None:
    # ondelete='RESTRICT' on extraction_runs.source means this DELETE will fail
    # if any extraction_runs rows reference uscg_manufacturers. That's correct —
    # downgrading would orphan run history.
    op.execute(sa.text("DELETE FROM source_watermarks WHERE source = 'uscg_manufacturers'"))
