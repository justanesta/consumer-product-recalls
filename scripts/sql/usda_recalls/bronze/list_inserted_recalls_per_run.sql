-- For every USDA recalls run that inserted >0 bronze rows, list the
-- (source_recall_id, langcode) pair(s) it inserted. Lets you map a
-- "wave" date observed in recent_runs.sql to the specific recall(s)
-- that drove it, then feed each into:
--
--   scripts/sql/usda_recalls/bronze/diagnose_payload_drift_for_recall.sql
--     (edit \set recall_id at top)
--   scripts/usda_recalls/inspect_raw_landing_for_recall.py --recall-id <id>
--
-- Particularly useful for single-row waves (e.g., 2026-05-05, 2026-05-12,
-- 2026-05-17 are all "loaded=1" days worth investigating for the
-- upstream-erasure pattern surfaced on 2026-05-17 / PHA-04302026-01).
--
-- Pairing strategy: bronze rows do not carry a run_id column, so we
-- pair by extraction_timestamp falling within run.started_at and
-- run.finished_at (same approach as diagnose_wave_field_drivers.sql).
-- Since USDA's BronzeLoader inserts only changed (content_hash) rows,
-- any bronze row landed within a run's window IS one of that run's
-- newly-versioned insertions — no further filtering needed.
--
-- No parameters. Run as:
--   psql -f scripts/sql/usda_recalls/bronze/list_inserted_recalls_per_run.sql

\pset null '<NULL>'

\echo
\echo '=== USDA waves: which recall(s) drove each insertion ==='
\echo 'One row per inserted (source_recall_id, langcode) per run.'
\echo 'Use the source_recall_id values to drive diagnose_payload_drift_for_recall.sql'
\echo 'or scripts/usda_recalls/inspect_raw_landing_for_recall.py.'

select
    r.started_at::date as run_date,
    r.started_at as run_started_at,
    r.id as run_id,
    r.records_inserted as run_records_inserted,
    trim(b.source_recall_id) as source_recall_id,
    b.langcode,
    b.extraction_timestamp as bronze_extraction_ts,
    substring(b.content_hash for 8) as bronze_hash_prefix
from extraction_runs r
join usda_fsis_recalls_bronze b
    on b.extraction_timestamp >= r.started_at
   and (r.finished_at is null or b.extraction_timestamp <= r.finished_at)
where r.source = 'usda'
  and r.records_inserted <= 50
  and r.status = 'success'
  and r.records_inserted > 0
order by r.started_at desc, source_recall_id, langcode;
