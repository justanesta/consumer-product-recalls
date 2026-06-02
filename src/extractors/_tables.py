"""Shared SQLAlchemy Table objects for the cross-source operational tables.

``source_watermarks`` and ``extraction_runs`` are single physical Postgres tables written
by every extractor, yet each extractor module used to redeclare its own ``sa.Table`` for
them — 8 copies each, with drifting column subsets (CPSC/FDA omitted ``last_etag`` /
``last_successful_extract_at`` / ``response_inner_content_sha256`` / ``was_short_circuited``).
Those subsets are not wrong at runtime (inserts are dict-keyed, so omitted columns just
default to NULL) but they are a maintenance trap: adding a column to the physical table
means remembering to touch 8 files.

This module declares each table ONCE with the **full column union**. Each extractor module
imports them under its existing private names::

    from src.extractors._tables import (
        extraction_runs as _extraction_runs,
        source_watermarks as _source_watermarks,
    )

so all existing references (``_source_watermarks``, ``_extraction_runs``) and the
per-module monkeypatch in tests/unit/test_record_run_response_fields.py keep working
against the one shared object.

Per-source ``*_bronze`` / ``*_rejected`` tables intentionally stay in their own modules —
they are genuinely per-source and ``src.bronze.recovery`` imports them by name.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# Own MetaData for the shared operational tables. Each extractor keeps its own
# ``_metadata`` for its bronze/rejected tables; nothing shares a MetaData with a
# duplicate table name, so there is no collision.
metadata = sa.MetaData()

# Column union of every source's former source_watermarks declaration. CPSC/FDA used only
# (source, last_cursor, updated_at); ETag sources added last_etag /
# last_successful_extract_at; USCG sources added last_records_count (Finding J
# short-circuit total). Sources that don't write a given column simply leave it NULL.
source_watermarks = sa.Table(
    "source_watermarks",
    metadata,
    sa.Column("source", sa.Text, primary_key=True),
    sa.Column("last_cursor", sa.Text),
    sa.Column("last_etag", sa.Text),
    sa.Column("last_successful_extract_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("last_records_count", sa.Integer),
)

# Column union of every source's former extraction_runs declaration. CPSC/FDA omitted
# response_inner_content_sha256 and was_short_circuited; NHTSA added the former; USCG
# sources added the latter. Sources that don't write a given column leave it NULL.
extraction_runs = sa.Table(
    "extraction_runs",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source", sa.Text),
    sa.Column("started_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("finished_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("status", sa.Text),
    sa.Column("records_extracted", sa.Integer),
    sa.Column("records_inserted", sa.Integer),
    sa.Column("records_rejected", sa.Integer),
    sa.Column("run_id", sa.Text),
    sa.Column("error_message", sa.Text),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("change_type", sa.Text),
    sa.Column("response_status_code", sa.Integer),
    sa.Column("response_etag", sa.Text),
    sa.Column("response_last_modified", sa.Text),
    sa.Column("response_body_sha256", sa.Text),
    sa.Column("response_headers", postgresql.JSONB),
    # NHTSA deep-rescan writes the POST_2010 inner-file SHA here (canonical =
    # rolling-current archive, matching the incremental path). BOTH archives' inner
    # SHAs (PRE_2010 + POST_2010) are recorded in the by-archive map below — that map
    # is what the deep-rescan short-circuit (W6) gates on, since the ZIP wrapper / ETag
    # churns daily (Finding J). See documentation/audit/deep_rescan_reliability_audit.md.
    sa.Column("response_inner_content_sha256", sa.Text),
    # {archive_url: inner_sha256} map (migration 0021) — NHTSA deep-rescan's both-archive
    # change oracle; consumed by the W6 short-circuit. NULL for every other source/path.
    sa.Column("response_inner_content_sha256_by_archive", postgresql.JSONB),
    sa.Column("was_short_circuited", sa.Boolean),
)
