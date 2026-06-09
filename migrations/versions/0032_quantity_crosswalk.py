"""quantity_crosswalk — raw recall-quantity string -> parsed (value, unit, category, basis).

The C13 free-text quantity enrichment. ``recalls parse-quantities`` is the I/O around the pure
``src.enrichment.quantity.parse_quantity``: it reads the distinct FDA + USDA quantity strings
(``product_distributed_quantity`` / ``qty_recovered``) from the ``stg_*`` views, parses each, and
truncate-reloads this table. Silver ``recall_product`` LEFT JOINs it on ``number_of_units`` (the raw
string, byte-identical to staging) for the four ``quantity_*`` columns.

Mirrors firm_crosswalk (migration 0024, ADR 0037): a Python-enrichment table created empty here,
declared as a dbt ``source`` (``enrichment.quantity_crosswalk``), 0 rows until the CLI runs. No
freshness block — rebuilt on demand in the transform cron, not on a cadence.

Revision ID: 0032
Revises: 0031
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0032"
down_revision: str | None = "0031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "quantity_crosswalk",
        # the raw quantity string is the join key (= recall_product.number_of_units for FDA/USDA).
        sa.Column("raw_quantity", sa.Text, primary_key=True),
        sa.Column("quantity_value", sa.Numeric, nullable=True),
        sa.Column("quantity_unit", sa.Text, nullable=True),
        sa.Column("quantity_category", sa.Text, nullable=True),
        # basis is always present (per_product / total_all_products / unknown).
        sa.Column("quantity_basis", sa.Text, nullable=False),
    )
    # `recalls parse-quantities` truncate-reloads this table; after the C31 repoint the runtime
    # connects as recalls_app (SELECT/INSERT/UPDATE via default privileges, but NOT TRUNCATE). Grant
    # TRUNCATE on just this rebuilt-each-run derived table so the reload works under the restricted
    # role. Guarded so a role-less environment (pre-C31 dev) is a clean no-op. (firm_crosswalk needs
    # the same grant — gap tracked for the C31 grant script.)
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_app') THEN
                GRANT TRUNCATE ON quantity_crosswalk TO recalls_app;
            END IF;
        END $$;
        """
    )


def downgrade() -> None:
    op.drop_table("quantity_crosswalk")
