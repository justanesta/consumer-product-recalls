"""seed source_watermarks row for uscg_manufacturer_details

Revision ID: 0018
Revises: 0017
Create Date: 2026-05-30

``extraction_runs.source`` is a FK back to ``source_watermarks.source`` (per
the baseline migration 0001). Any extractor whose ``source_name`` isn't in
``source_watermarks`` cannot write its run record (the FK insert fails — caught
in ``_record_run``'s broad except and surfaced as the
``extraction_run.record_failed`` warning during first extraction).

This migration adds the ``uscg_manufacturer_details`` row so the run-recording
write succeeds — mirrors 0016 (``uscg_manufacturers``) and 0008
(``usda_establishments``). The detail source uses ``last_successful_extract_at``
for freshness; it does NOT use ``last_records_count`` (no ``Records Found``
footer on the detail page) or ``last_cursor`` / ``last_etag`` (Finding E — the
source emits no validators).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO source_watermarks (source) VALUES ('uscg_manufacturer_details') "
            "ON CONFLICT (source) DO NOTHING"
        )
    )


def downgrade() -> None:
    # ondelete='RESTRICT' on extraction_runs.source means this DELETE fails if
    # any extraction_runs rows reference uscg_manufacturer_details — correct;
    # downgrading would orphan run history.
    op.execute(sa.text("DELETE FROM source_watermarks WHERE source = 'uscg_manufacturer_details'"))
