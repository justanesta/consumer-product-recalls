"""Add extraction_runs.replayed_from_run_id (Phase 6d re-ingest lineage)

Revision ID: 0029
Revises: 0028
Create Date: 2026-06-07

Phase 6d re-ingest (R2 replay, ADR 0028 Mechanism B) writes a `schema_rebaseline`
`extraction_runs` row per replayed payload. By `change_type` alone that is indistinguishable
from a normal `recalls extract|deep-rescan --change-type=schema_rebaseline` re-baseline — both
are `schema_rebaseline`. This nullable column records, on a re-ingest run, the `run_id` of the
ORIGINAL run whose landed payload was replayed. It:

  - makes re-ingest runs unambiguously identifiable (`replayed_from_run_id IS NOT NULL`) — used
    by `scripts/sql/_pipeline/verify_reingest_lineage.sql` and any re-baseline audit (a normal
    extract re-baseline correctly writes a USDA presence manifest; a re-ingest must not, and this
    column is how you tell which run is which);
  - lets re-ingest skip payloads it has already replayed (idempotency-skip), so re-running a
    window does not pile up duplicate rebaseline runs (override with `--force`).

NULL for every routine / extract-re-baseline / deep-rescan run (none are replays). Nullable, no
backfill — existing rows are not replays.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0029"
down_revision: str | None = "0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "extraction_runs"
_COLUMN = "replayed_from_run_id"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column(_TABLE, _COLUMN)
