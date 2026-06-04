"""firm_crosswalk — cross-source firm entity-resolution crosswalk

Revision ID: 0024
Revises: 0023
Create Date: 2026-06-03

Phase 6b firm entity resolution (``project_scope/phase-6b-execution-plan.md``,
PR 6b.0 substrate). The HYBRID normalization engine (Lane F / D1) keeps
deterministic name-cleaning in dbt-SQL macros but runs the genuine edit-distance
clustering in a tested Python ``recalls resolve-firms`` CLI (PR 6b.4, USER-run),
because Neon has no dbt-python runtime and pg_trgm/fuzzystrmatch are not enabled.

- **Why a table (not a dbt seed)**: the crosswalk is rebuilt by re-running the
  CLI as new firms land, so it must be a writable, reproducible artifact — not a
  frozen CSV. Registered as the dbt source ``enrichment.firm_crosswalk``
  (``dbt/models/staging/_sources.yml``); the silver ``firm`` / ``recall_event_firm``
  models LEFT JOIN it for the additive ``canonical_firm_id`` (wired in PR 6b.4).

- **Grain**: one row per ``firm_id`` (= ``md5(normalized_name)`` from ``firm.sql``).
  ``firm_id`` is the natural primary key (a firm maps to exactly one cluster), so
  no surrogate ``id`` column (unlike the bronze tables, which carry many
  content-hash rows per identity).

- **canonical_firm_id**: the cluster representative's ``firm_id``. A firm that
  belongs to no multi-member cluster IS its own canonical (the resolver writes
  ``canonical_firm_id = firm_id``), so the silver-side
  ``coalesce(x.canonical_firm_id, md5(normalized_name))`` is a no-op for
  singletons. Per Critical Decision #2, ``firm.firm_id`` stays
  ``md5(normalized_name)``; FEI/MIC/establishment_number remain deterministic
  anchors/edges, NOT identity (ADR 0002 declines to prescribe a firm_identifier
  table; CPSC + NHTSA carry no structured id).

- **resolver_version**: the CLI stamps its blocking-key + threshold config here so
  a crosswalk rebuild is auditable and the chosen ``rapidfuzz_high`` cutoff (set
  from the PR 6b.4 residual gate) is recorded with the rows it produced.

Created empty here; populated by ``recalls resolve-firms`` in PR 6b.4. Until then
the dbt source has 0 rows and the (6b.4) LEFT JOIN resolves to the per-name
``firm_id`` — a clean no-op.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "firm_crosswalk",
        # firm_id = md5(normalized_name) from silver firm.sql — the natural key
        # (one cluster per firm), so it is the PRIMARY KEY directly.
        sa.Column("firm_id", sa.Text, primary_key=True),
        sa.Column("canonical_firm_id", sa.Text, nullable=False),
        sa.Column("canonical_name", sa.Text, nullable=False),
        # match_confidence: the shared namespaced vocabulary also stamped on
        # recall_event_firm (e.g. 'fei_exact', 'rapidfuzz_high', 'singleton').
        sa.Column("match_confidence", sa.Text, nullable=False),
        # match_score: the RapidFuzz similarity (0-100) for fuzzy merges; NULL for
        # deterministic (FEI/singleton) rows.
        sa.Column("match_score", sa.Numeric, nullable=True),
        sa.Column("resolver_version", sa.Text, nullable=False),
        sa.Column(
            "resolved_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # Reverse lookup: all firm_ids that collapse to a given canonical (the cluster
    # members), used by the silver firm regroup in PR 6b.4.
    op.execute("CREATE INDEX ix_firm_crosswalk_canonical ON firm_crosswalk (canonical_firm_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_firm_crosswalk_canonical")
    op.drop_table("firm_crosswalk")
