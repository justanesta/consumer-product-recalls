"""nhtsa_recalls_bronze, nhtsa_recalls_rejected, response_inner_content_sha256

Revision ID: 0011
Revises: 0010
Create Date: 2026-05-05

Schema targets the 29-field tab-delimited TSV inside FLAT_RCL_*.zip per
documentation/nhtsa/flat_file_observations.md (Phase 5c Step 1):

- Architecture: Option A (TSV-only). Incremental path downloads
  FLAT_RCL_POST_2010.zip (~14 MB compressed, 240,126 records); historical
  seed pulls both PRE_2010 + POST_2010 via the deep-rescan workflow
  (~322k total rows, dating back to 1966-01-19 by RCDATE).

- Identity: source_recall_id maps to RECORD_ID (TSV field 1) — NHTSA's
  stable per-row surrogate key documented in RCL.txt. CAMPNO (the
  public-facing recall ID) is captured in its own indexed column for
  analytical grouping but is NOT unique per row (one campaign produces
  multiple TSV rows, one per make × model × year affected).

- Nullability follows Finding F's 18-year drift history. The bronze
  schema's extra='forbid' (ADR 0014) catches a 30th column at validation
  time; columns added on the right edge of the row in 2007/2008/2020/2025
  must be nullable so historical archives with the older shape parse
  without quarantine:
    - notes (post-2007)
    - rcl_cmpt_id (post-2008)
    - mfr_comp_name / mfr_comp_desc / mfr_comp_ptno (post-2020)
    - do_not_drive / park_outside (post-May-2025)
  Plus rcdate AND datea are nullable per the 2026-05-05 sentinel-date
  probe (Finding H follow-up): 5/81,714 PRE_2010 records have empty
  RCDATE and DATEA — almost certainly the same records, from the 1979
  bulk-load of pre-1979 historical recalls. POST_2010 has 0 empty
  rows for either field, so the daily incremental path is unaffected;
  the relaxation only matters for the deep-rescan / historical-seed
  path. Marking these required would quarantine 5 real recall records
  over a missing date field.

- Storage-forced typed columns per ADR 0027:
    - bgman / endman / odate / rcdate / datea → TIMESTAMPTZ (parsed from
      YYYYMMDD by the Pydantic schema's _parse_nhtsa_date)
    - do_not_drive / park_outside → BOOLEAN (coerced from "Yes"/"No"
      strings by _to_bool)
    - fmvss → VARCHAR(3) per Finding F (May 2025 width reduction)
  Sentinel-date mapping (ODATE 19010101 → NULL) is value-level
  normalization — deferred to stg_nhtsa_recalls.sql per ADR 0027.

- New universal column on extraction_runs:
  ``response_inner_content_sha256``. Populated only by flat-file
  extractors; REST sources leave it null. Distinct from the wrapper-level
  ``response_body_sha256`` (migration 0010) because for ZIPs the wrapper
  bytes are non-deterministic across re-archives (Finding J — daily
  re-zip with non-deterministic metadata produces different wrapper
  bytes for identical inner content). The inner-content SHA-256 is the
  authoritative "did the data change?" oracle for ZIPs and incidentally
  closes Finding H Q1 (update cadence) over ~7 days of accumulated
  history.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migrations._columns import rejected_table_columns

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "nhtsa_recalls_bronze",
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
        # 28 NHTSA domain columns. RCL.txt's field 1 (RECORD_ID) is held by
        # the universal source_recall_id column above — the schema's
        # `source_recall_id` field uses validation_alias="record_id" to absorb
        # the value from the extractor's lowercase-keyed dict, mirroring the
        # USDA pattern where field_recall_number → source_recall_id. Order
        # matches RCL.txt fields 2–29.
        sa.Column("campno", sa.Text, nullable=False),
        sa.Column("maketxt", sa.Text, nullable=False),
        sa.Column("modeltxt", sa.Text, nullable=False),
        sa.Column("yeartxt", sa.Text, nullable=False),
        sa.Column("mfgcampno", sa.Text, nullable=True),
        sa.Column("compname", sa.Text, nullable=False),
        sa.Column("mfgname", sa.Text, nullable=False),
        sa.Column("bgman", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("endman", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("rcltype", sa.Text, nullable=False),
        sa.Column("potaff", sa.Text, nullable=False),
        sa.Column("odate", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("influenced_by", sa.Text, nullable=True),
        sa.Column("mfgtxt", sa.Text, nullable=False),
        # Nullable per the 2026-05-05 sentinel-date probe (Finding H
        # follow-up) — 5 PRE_2010 records have null RCDATE.
        sa.Column("rcdate", sa.TIMESTAMP(timezone=True), nullable=True),
        # Nullable per Finding H Q2 — 5 PRE_2010 records have null DATEA.
        sa.Column("datea", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("rpno", sa.Text, nullable=True),
        # CHAR(3) per Finding F (May 2025 width reduction).
        sa.Column("fmvss", sa.String(length=3), nullable=True),
        sa.Column("desc_defect", sa.Text, nullable=False),
        sa.Column("conequence_defect", sa.Text, nullable=False),
        sa.Column("corrective_action", sa.Text, nullable=False),
        # Field 23 NOTES: nullable per Finding F (added 2007-09-14; pre-2007
        # historical records lack it).
        sa.Column("notes", sa.Text, nullable=True),
        # Field 24 RCL_CMPT_ID: nullable per Finding F (added 2008-03-14).
        sa.Column("rcl_cmpt_id", sa.Text, nullable=True),
        # Fields 25-27: nullable per Finding F (added 2020-03-23).
        sa.Column("mfr_comp_name", sa.Text, nullable=True),
        sa.Column("mfr_comp_desc", sa.Text, nullable=True),
        sa.Column("mfr_comp_ptno", sa.Text, nullable=True),
        # Fields 28-29: nullable per Finding F (added May 2025).
        sa.Column("do_not_drive", sa.Boolean, nullable=True),
        sa.Column("park_outside", sa.Boolean, nullable=True),
    )

    # BronzeLoader._fetch_existing_hashes(): latest row per source_recall_id.
    op.execute(
        "CREATE INDEX ix_nhtsa_recalls_bronze_id_ts "
        "ON nhtsa_recalls_bronze (source_recall_id, extraction_timestamp DESC)"
    )

    # Analytical grouping by recall campaign — CAMPNO is the public-facing
    # NHTSA ID and the natural unit of "this is one recall." A single CAMPNO
    # produces many TSV rows (one per affected make × model × year) so this
    # is not unique. Indexed for downstream silver joins.
    op.execute("CREATE INDEX ix_nhtsa_recalls_bronze_campno ON nhtsa_recalls_bronze (campno)")

    op.create_table("nhtsa_recalls_rejected", *rejected_table_columns())

    # New universal forensic column on extraction_runs. Populated only by
    # flat-file extractors (NHTSA today, USCG tooling later) via the
    # FlatFileExtractor._capture_flatfile_response analog of
    # RestApiExtractor._capture_response. REST sources leave it null.
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_inner_content_sha256",
            sa.Text,
            nullable=True,
            comment=(
                "SHA-256 of the decompressed inner content for flat-file "
                "sources, lowercase hex. Authoritative 'did the data change?' "
                "oracle when the wrapper hash is non-deterministic across "
                "re-archives (NHTSA ZIPs — see Finding J in "
                "documentation/nhtsa/flat_file_observations.md). Null for "
                "REST sources, which use response_body_sha256 (migration "
                "0010) as their oracle."
            ),
        ),
    )

    # Mute unused-import warning when the migration body is short.
    _ = postgresql  # noqa: F841


def downgrade() -> None:
    op.drop_column("extraction_runs", "response_inner_content_sha256")
    op.drop_table("nhtsa_recalls_rejected")
    op.execute("DROP INDEX IF EXISTS ix_nhtsa_recalls_bronze_campno")
    op.execute("DROP INDEX IF EXISTS ix_nhtsa_recalls_bronze_id_ts")
    op.drop_table("nhtsa_recalls_bronze")
