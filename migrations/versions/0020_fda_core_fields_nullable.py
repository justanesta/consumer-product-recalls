"""drop NOT NULL on four FDA "core identifier" fields before the historical seed

Revision ID: 0020
Revises: 0019
Create Date: 2026-05-31

The Phase 6a.5 FDA historical seed pulls the full corpus via the no-window
`filter:"[]"` bulk POST (project_scope/archive/fda-historical-seed-plan.md §3) instead of
an `eventlmdfrom` date window. That window silently excluded the ~197 records whose
`EVENTLMD` is null — per api_observations.md Finding H (line 148) the `*lmd` columns
"advance on edits only … un-edited records have null", and a server-side `>=`
comparison cannot match a null. Those rows were never *absent* from the corpus, only
*invisible* to every windowed extraction to date; the full-corpus seed surfaces them.

`api_observations.md:374` lists EVENTLMD / CENTERCD / PRODUCTTYPESHORT / FIRMLEGALNAM
among "core identifiers" assumed never-null and made `nullable=False` in 0004
(lines 43, 44, 46, 47). That assumption is an inference the windowing masked, and the
197 null-EVENTLMD rows falsify it. With those columns NOT NULL, even the `filter:"[]"`
seed would *silently quarantine* the 197 (0.15% « the 5% rejection threshold, and
`records_landed` reports the fetched count, not the inserted count) — the exact
records the full-corpus seed exists to capture. Dropping NOT NULL lets every row land.

This is the "permissive bronze / strict silver" policy (ADR 0014): bronze stores the
source's representation verbatim; the storage-forced `'' → None` coercion these fields
inherit via `_FdaNullableDate` is the ADR 0027 rule. dbt staging/silver enforce the
real constraints (a warn-tripwire on the staging event_lmd null count; silver
coalesces published_at) — see plan §0.1.

Lands BEFORE the seed: `fda_recalls_bronze` holds zero rows on production `main`
(migration 0019 docstring), so DROP NOT NULL is trivially safe — no backfill, no
content-hash churn. The `ix_fda_recalls_bronze_event_lmd` btree (0004:74) is a plain
index with no partial/expression clause, so a now-nullable column indexes fine; it is
left untouched. The downgrade re-adds NOT NULL and will fail if nulls exist by then —
acceptable for a rare reverse migration.
"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "0020"
down_revision: str | None = "0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "fda_recalls_bronze"

# The four 0004 "core identifiers" (lines 43, 44, 46, 47) the no-window seed
# would otherwise silently quarantine. event_lmd is TIMESTAMPTZ; the other three
# are Text. Type is passed to alter_column so Alembic emits a complete ALTER even
# on backends that need the existing type.
# `Any` for the type slot: sa.types.TypeEngine is invariant in its parameter, so a
# heterogeneous (TIMESTAMP / Text) tuple won't satisfy TypeEngine[object] (mirrors
# migration 0019's _COLUMNS annotation).
_COLS: tuple[tuple[str, Any], ...] = (
    ("event_lmd", sa.TIMESTAMP(timezone=True)),
    ("center_cd", sa.Text()),
    ("product_type_short", sa.Text()),
    ("firm_legal_nam", sa.Text()),
)


def upgrade() -> None:
    for name, type_ in _COLS:
        op.alter_column(_TABLE, name, existing_type=type_, nullable=True)


def downgrade() -> None:
    # Re-imposes NOT NULL; fails if any null rows exist (e.g. a seeded corpus) —
    # acceptable for a rare reverse migration.
    for name, type_ in _COLS:
        op.alter_column(_TABLE, name, existing_type=type_, nullable=False)
