"""Add etag_audit to extraction_runs.change_type CHECK constraint

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-10

Per the audit-run pattern documented in Finding A addendum (2026-05-10) of
documentation/usda/establishment_api_observations.md and Finding P addendum
of documentation/usda/recall_api_observations.md: an `etag_audit` run is a
periodic unconditional GET (skips If-None-Match / If-Modified-Since) used to
directly verify that the server's ETag-validation honesty matches the
inferential green-light from the passive viability study. Tagging the run
with change_type='etag_audit' makes it filterable in
scripts/sql/_pipeline/etag_audit_check.sql, which compares the audit run's
body sha against the most recent prior 200's body sha — flagging any
intervening 304s where the body subsequently differs as a confirmed
false-304.

ADR 0028 §Negative consequences anticipated this exact pattern: "Adding a
new value in the future is a one-line CHECK-constraint update in a
follow-up migration."
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_ALLOWED_CHANGE_TYPES = (
    "routine",
    "schema_rebaseline",
    "hash_helper_rebaseline",
    "historical_seed",
    "etag_audit",
)


def upgrade() -> None:
    op.drop_constraint("ck_extraction_runs_change_type", "extraction_runs", type_="check")
    allowed = ", ".join(f"'{v}'" for v in _ALLOWED_CHANGE_TYPES)
    op.create_check_constraint(
        "ck_extraction_runs_change_type",
        "extraction_runs",
        f"change_type IN ({allowed})",
    )


def downgrade() -> None:
    op.drop_constraint("ck_extraction_runs_change_type", "extraction_runs", type_="check")
    prior_allowed = ", ".join(f"'{v}'" for v in _ALLOWED_CHANGE_TYPES if v != "etag_audit")
    op.create_check_constraint(
        "ck_extraction_runs_change_type",
        "extraction_runs",
        f"change_type IN ({prior_allowed})",
    )
