-- Diagnostic — inspect what content changed across consecutive bronze
-- snapshots for a specific (source_recall_id, langcode) pair.
--
-- Default target: PHA-04092026-01 / English — the canonical Public
-- Health Alert cited in ADR 0026 and surfaced as the only post-rebaseline
-- content-edit transition by `assert_field_last_modified_date_advances_on_edit.sql`
-- (see U3 section in `documentation/usda/bilingual_and_lmd_findings.md`).
--
-- To inspect a different recall, edit the two `\set` lines at the top.
-- The `\x auto` directive switches psql to expanded display for the
-- payload row — without it, the JSONB blob is unreadable.
--
-- What to look for:
--   * Diff on a peripheral field (media_contact, summary text rewording)
--     → benign edit; consistent with FSIS doing minor copy-edits without
--       advancing last_modified_date.
--   * Diff on a structural field (recall_classification, product_items,
--     recall_reason, risk_level, recall_type) → U3 violation is real
--     even at small sample size; Phase 6 cannot use last_modified_date.
--   * Diff on multiple fields at once → likely a substantive recall
--     amendment that should have advanced last_modified_date but didn't.
--
-- Companion script for FDA (different vulnerability class):
-- `scripts/sql/fda/bronze/diagnose_silent_edit_attribution.sql`.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\set recall_id 'PHA-04302026-01'
\set langcode 'English'

\echo
\echo '=== Q1: snapshot metadata for the target recall ==='
\echo 'Side-by-side view of extraction_timestamp, content_hash, last_modified_date'
\echo 'across all bronze rows for the (recall_id, langcode) pair. Compare hashes'
\echo 'and last_modified_date to confirm which transitions are content edits.'

select
    row_number() over (order by extraction_timestamp) as snapshot_n,
    extraction_timestamp,
    last_modified_date,
    content_hash
from usda_fsis_recalls_bronze
where trim(source_recall_id) = :'recall_id'
  and langcode = :'langcode'
order by extraction_timestamp;

\echo
\echo '=== Q2: full payload per snapshot (expanded display) ==='
\echo 'Full bronze record minus dedup/lineage columns. With \x auto, each row'
\echo 'renders as a vertical key:value block — diff visually by snapshot_n.'

\x auto

select
    row_number() over (order by extraction_timestamp) as snapshot_n,
    extraction_timestamp,
    to_jsonb(b)
        - 'id'
        - 'content_hash'
        - 'extraction_timestamp'
        - 'raw_landing_path'
        as payload
from usda_fsis_recalls_bronze b
where trim(source_recall_id) = :'recall_id'
  and langcode = :'langcode'
order by extraction_timestamp;

\x off
