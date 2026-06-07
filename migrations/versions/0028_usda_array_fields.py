"""USDA recall API change: 10 multi-value fields scalar→array + recall_number_export

Revision ID: 0028
Revises: 0027
Create Date: 2026-06-06

The 2026-06 USDA FSIS recall API (`/fsis/api/recall/v/1`) made a breaking shape change,
caught when `recalls extract usda` rejected 2006/2006 records at validate (the schema's
`extra='forbid' + strict=True` posture). Diagnosed via
`scripts/sql/usda_recalls/bronze/diagnose_validation_failures.sql`:

  - **Ten fields flipped scalar → JSON array** (Q6, uniform across all 2006 records):
    recall_reason, processing, states, establishment, labels, product_items, distro_list,
    company_media_contact, en_press_release, press_release. These columns move TEXT → JSONB
    so bronze stores the source's arrays faithfully (ADR 0007). The matching schema change
    (`src/schemas/usda.py` `_UsdaStrList`) accepts scalar OR list for R2-replay safety.
  - **One new field added:** `field_recall_number_export` (scalar) → a new `recall_number_export`
    TEXT column.
  - **One field dropped:** `field_closed_date` is no longer returned (0/2006). No DDL change —
    the column stays for historical rows; new rows land NULL (tracked in
    recall_api_observations.md Finding S).

Existing bronze data: the TYPE conversion wraps each non-null scalar into a 1-element jsonb
array (`"x"` → `["x"]`, NOT split — silver owns tokenization) and maps `''` → `[]`, matching
`_to_str_list` exactly so migrated-in-place rows and replay-reingested rows agree.

Re-version note: every USDA record's content_hash will change on the next successful extract
(text-hash → list-hash), producing a one-time re-version wave. Run that recovery extract as
`recalls deep-rescan usda --change-type=schema_rebaseline` so Phase 6c's recall_event_history
excludes it from edit detection (ADR 0027).

View dependency: the dbt staging view `stg_usda_fsis_recalls` SELECTs these columns, so the
migration drops it before the type change (Postgres blocks ALTER TYPE under a view). It is
recreated by the next `dbt build` — run that after this migration and the recovery extract.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0028"
down_revision: str | None = "0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The ten fields FSIS flipped scalar → array (Finding S).
_ARRAY_FIELDS: tuple[str, ...] = (
    "recall_reason",
    "processing",
    "states",
    "establishment",
    "labels",
    "product_items",
    "distro_list",
    "company_media_contact",
    "en_press_release",
    "press_release",
)
_TABLE = "usda_fsis_recalls_bronze"
# The dbt staging view (materialized='view', schema=public per generate_schema_name)
# SELECTs these bronze columns, and Postgres refuses ALTER COLUMN TYPE while a view
# depends on the column. Drop it before the type changes; `dbt build` recreates it
# (now jsonb-aware) on the next run. CASCADE covers any view-on-view dependents; all
# silver models are tables and the SCD-2 snapshots live in silver_snapshots (off this
# view), so nothing stateful is touched.
_STG_VIEW = "stg_usda_fsis_recalls"


def upgrade() -> None:
    op.execute(f"DROP VIEW IF EXISTS {_STG_VIEW} CASCADE")
    for col in _ARRAY_FIELDS:
        # null → null · '' → [] · 'x' → ['x'] (verbatim, un-split) — mirrors _to_str_list.
        op.alter_column(
            _TABLE,
            col,
            type_=postgresql.JSONB(),
            postgresql_using=(
                f"case when {col} is null then null "
                f"when {col} = '' then '[]'::jsonb "
                f"else to_jsonb(array[{col}]) end"
            ),
        )
    op.add_column(_TABLE, sa.Column("recall_number_export", sa.Text(), nullable=True))


def downgrade() -> None:
    op.execute(f"DROP VIEW IF EXISTS {_STG_VIEW} CASCADE")
    op.drop_column(_TABLE, "recall_number_export")
    for col in _ARRAY_FIELDS:
        # Inverse: [] → '' · ['a','b'] → 'a, b' · null → null.
        op.alter_column(
            _TABLE,
            col,
            type_=sa.Text(),
            postgresql_using=(
                f"case when {col} is null then null "
                f"when jsonb_array_length({col}) = 0 then '' "
                f"else array_to_string(array(select jsonb_array_elements_text({col})), ', ') end"
            ),
        )
