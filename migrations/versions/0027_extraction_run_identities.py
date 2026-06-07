"""extraction_run_identities — per-run presence manifest (ADR 0026)

Revision ID: 0027
Revises: 0026
Create Date: 2026-06-06

Phase 6c history/lifecycle (``project_scope//archive/phase-6c-execution-plan.md`` commit 6c.0).
Bronze (ADR 0007) is a Type-4 store of *what content* we saw, but it cannot signal a
*retraction*: a record absent from a run's response produces zero new bronze rows,
identical to "content unchanged, dedup skipped." The presence manifest records, per
successful run, the set of recall identities actually returned, so silver
(``recall_lifecycle``, 6c.2) can derive ``is_currently_active`` / ``was_ever_retracted``
(ADR 0026 Option A). Written by ``Extractor._record_run`` in the same transaction as the
``extraction_runs`` row its ``run_id`` references.

USDA-only initially (``DedupContract.default_track_presence``) — the only source with
confirmed implicit retraction + non-atomic bilingual updates (ADR 0026 acceptance).

Two deliberate deviations from ADR 0026's literal schema sketch, both forced by the
current code / Postgres semantics:

  * **Surrogate ``id`` PK + composite UNIQUE, not ``PRIMARY KEY (run_id, source,
    source_recall_id, langcode)``.** ``langcode`` is nullable (NULL for non-bilingual
    sources), and Postgres forbids a nullable column in a PRIMARY KEY. We follow the
    project's table convention (surrogate ``id`` PK, as on every ``*_bronze`` table and
    ``extraction_runs``) and express the identity uniqueness as a UNIQUE constraint. For
    USDA ``langcode`` is always populated, so the constraint is fully effective; the
    application-level dedup in ``build_presence_manifest_rows`` is the primary guard.

  * **A UNIQUE constraint on ``extraction_runs.run_id`` is added first** so the manifest's
    ``run_id`` FK has a unique target. ``run_id`` is a per-run uuid4
    (``src/extractors/_base.py`` ``run()``), already logically unique; this promotes it to
    the enforced logical key (the integer ``id`` stays the surrogate PK). The FK uses
    ``ON DELETE CASCADE`` — purging an ``extraction_runs`` row drops its manifest rows with
    it (the manifest is meaningless without its run).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027"
down_revision: str | None = "0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # FK-target prerequisite: make extraction_runs.run_id a unique key. uuid4 per run, so
    # no existing duplicates; UNIQUE permits the (rare, legacy) NULL run_ids as distinct.
    op.create_unique_constraint("uq_extraction_runs_run_id", "extraction_runs", ["run_id"])

    op.create_table(
        "extraction_run_identities",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "run_id",
            sa.Text,
            sa.ForeignKey("extraction_runs.run_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("source_recall_id", sa.Text, nullable=False),
        # Nullable: NULL for single-key sources, populated for USDA bilingual siblings
        # (ADR 0006 / Finding F). Part of the identity uniqueness below.
        sa.Column("langcode", sa.Text, nullable=True),
        sa.UniqueConstraint(
            "run_id",
            "source",
            "source_recall_id",
            "langcode",
            name="uq_eri_identity",
        ),
    )

    # Reverse lookup: "in which runs did this recall identity appear?" — the access path
    # recall_lifecycle uses to compute first_seen / last_seen / is_currently_active.
    op.execute(
        "CREATE INDEX ix_eri_source_recall_lookup "
        "ON extraction_run_identities (source, source_recall_id, langcode)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_eri_source_recall_lookup")
    op.drop_table("extraction_run_identities")
    op.drop_constraint("uq_extraction_runs_run_id", "extraction_runs", type_="unique")
