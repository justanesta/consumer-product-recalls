"""usda_fsis_establishments_bronze and usda_fsis_establishments_rejected tables

Revision ID: 0006
Revises: 0005
Create Date: 2026-05-01

Schema targets the GET /fsis/api/establishments/v/1 flat-array response
(Findings A–G in documentation/usda/establishment_api_observations.md). The
endpoint returns 7,945 records in one response — no pagination, no ETag,
weekly Mon/Tue update cadence. There is no incremental cursor; every run is a
full dump and content-hash dedup (ADR 0007) handles idempotency.

Required (NOT NULL) fields per Finding D's empirical 0% empty rate:
establishment_id (→ source_recall_id slot), establishment_name, address,
state, zip, establishment_number, latest_mpi_active_date (100% populated on
ALL 7,945 records including inactive — Finding G), status_regulated_est
(two-value enum: '' for active MPI, 'Inactive' otherwise), activities,
dbas (both true JSON arrays per Finding C; nullability would obscure the
boundary between "no documented activities" and "field missing").

Nullable fields: phone (3.9% empty), duns_number (85.5%), county (1.5% +
boolean false sentinel — Finding C), fips_code (4.3%), geolocation (1.5%
+ boolean false sentinel), grant_date, size, district, circuit. The boolean
false sentinels are normalized to NULL by the Pydantic schema before this
table is loaded.

Identity column is `source_recall_id` (matches every other source's bronze
schema and the rejected_table_columns helper); the value is the FSIS
establishment_id integer-as-string (e.g. "6163082").
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migrations._columns import rejected_table_columns

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "usda_fsis_establishments_bronze",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        # Dedup / lineage columns (ADR 0007)
        sa.Column("source_recall_id", sa.Text, nullable=False),
        sa.Column("content_hash", sa.Text, nullable=False),
        sa.Column(
            "extraction_timestamp",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("raw_landing_path", sa.Text, nullable=False),
        # Required identifiers and demographics (Finding D — 0% empty)
        sa.Column("establishment_name", sa.Text, nullable=False),
        sa.Column("establishment_number", sa.Text, nullable=False),
        sa.Column("address", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("zip", sa.Text, nullable=False),
        # Required lifecycle (Finding G — 100% populated on all records)
        sa.Column("latest_mpi_active_date", sa.TIMESTAMP(timezone=True), nullable=False),
        # Required status enum: '' = active MPI, 'Inactive' = inactive
        sa.Column("status_regulated_est", sa.Text, nullable=False),
        # Required JSON arrays (Finding C — true arrays; whitespace stripped in schema)
        sa.Column("activities", postgresql.JSONB, nullable=False),
        sa.Column("dbas", postgresql.JSONB, nullable=False),
        # Optional demographic fields
        sa.Column("phone", sa.Text, nullable=True),
        sa.Column("duns_number", sa.Text, nullable=True),
        # county / geolocation: boolean false sentinel in API → normalized to NULL
        sa.Column("county", sa.Text, nullable=True),
        sa.Column("fips_code", sa.Text, nullable=True),
        sa.Column("geolocation", sa.Text, nullable=True),
        sa.Column("grant_date", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("size", sa.Text, nullable=True),
        sa.Column("district", sa.Text, nullable=True),
        sa.Column("circuit", sa.Text, nullable=True),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id.
    op.execute(
        "CREATE INDEX ix_usda_fsis_establishments_bronze_id_ts "
        "ON usda_fsis_establishments_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Common downstream filter: "active MPI only" views in silver / gold.
    op.execute(
        "CREATE INDEX ix_usda_fsis_establishments_bronze_status "
        "ON usda_fsis_establishments_bronze (status_regulated_est)"
    )

    # Functional index supporting the silver join from
    # stg_usda_fsis_recalls.establishment → establishment_name on
    # upper(trim(...)) (Phase 5b.2 Step 5). Trim is not added to the index
    # because real data shows 0% leading/trailing whitespace on this field;
    # add it later if a join finding contradicts that.
    op.execute(
        "CREATE INDEX ix_usda_fsis_establishments_bronze_name_upper "
        "ON usda_fsis_establishments_bronze (upper(establishment_name))"
    )

    op.create_table("usda_fsis_establishments_rejected", *rejected_table_columns())


def downgrade() -> None:
    op.drop_table("usda_fsis_establishments_rejected")
    op.execute("DROP INDEX IF EXISTS ix_usda_fsis_establishments_bronze_name_upper")
    op.execute("DROP INDEX IF EXISTS ix_usda_fsis_establishments_bronze_status")
    op.execute("DROP INDEX IF EXISTS ix_usda_fsis_establishments_bronze_id_ts")
    op.drop_table("usda_fsis_establishments_bronze")
