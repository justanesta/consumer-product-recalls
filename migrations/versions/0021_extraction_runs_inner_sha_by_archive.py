"""Add extraction_runs.response_inner_content_sha256_by_archive (JSONB)

Revision ID: 0021
Revises: 0020
Create Date: 2026-06-02

NHTSA deep-rescan downloads two archives (PRE_2010 + POST_2010); the existing
``response_inner_content_sha256`` column holds only the POST_2010 inner SHA
(canonical, incremental-path parity). This adds a JSONB ``{archive_url: inner_sha256}``
map so a deep-rescan run records BOTH archives' inner-content SHAs in a
SQL-queryable form.

Consumed by the NHTSA deep-rescan short-circuit (W6 — see
project_scope/deep-rescan-reliability-plan.md and
documentation/audit/deep_rescan_reliability_audit.md): a no-change run compares both
fresh inner SHAs to the prior run's map and skips parse/validate/load. Backfilled for
existing deep-rescan runs from their R2 manifests by
scripts/nhtsa/backfill_inner_sha_by_archive.py.

Forward-only by convention; the downgrade body exists for completeness.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0021"
down_revision: str | None = "0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_inner_content_sha256_by_archive",
            postgresql.JSONB,
            nullable=True,
            comment=(
                "Map of {archive_url: SHA-256 of decompressed inner content}. "
                "Populated by NHTSA deep-rescan runs (NhtsaDeepRescanLoader), which "
                "pull PRE_2010 + POST_2010. The single-valued "
                "response_inner_content_sha256 column stays as the POST_2010 / "
                "incremental-path value; this map is the authoritative both-archive "
                "record consumed by the deep-rescan short-circuit (W6)."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("extraction_runs", "response_inner_content_sha256_by_archive")
