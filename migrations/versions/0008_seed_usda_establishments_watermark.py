"""seed source_watermarks row for usda_establishments

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-01

The baseline migration (0001) hardcoded a five-source list
``["cpsc", "fda", "usda", "nhtsa", "uscg"]`` and seeded ``source_watermarks``
with one row per source. ``extraction_runs.source`` is a FK back to that table,
so any extractor whose ``source_name`` isn't in ``source_watermarks`` cannot
write its run record (the FK insert silently fails — captured in
``_record_run``'s broad except clause; surfaced as the
``extraction_run.record_failed`` warning during Phase 5b.2 first extraction on
2026-05-01).

This migration adds the ``usda_establishments`` row so the run-recording write
succeeds. ``usda_establishments`` doesn't actually use the watermark columns
(``last_cursor`` / ``last_etag``) — the API has no incremental cursor or ETag
(Finding A) — but the row needs to exist for the FK.

Future sources will need the same one-row seed migration. A general fix
(replacing the FK with a soft enum or dropping the FK in favor of a CHECK
constraint) is filed as an architectural follow-up in implementation_plan.md.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO source_watermarks (source) VALUES ('usda_establishments') "
            "ON CONFLICT (source) DO NOTHING"
        )
    )


def downgrade() -> None:
    # ondelete='RESTRICT' on extraction_runs.source means this DELETE will fail
    # if any extraction_runs rows reference usda_establishments. That's the
    # correct behavior — downgrading would orphan run history.
    op.execute(sa.text("DELETE FROM source_watermarks WHERE source = 'usda_establishments'"))
