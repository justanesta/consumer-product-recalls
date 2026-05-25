-- List NHTSA extraction_runs with full run_id UUIDs for feeding into
-- scripts/sql/nhtsa/bronze/explore_incremental_delta.sql (which accepts
-- `-v run_id=<uuid>` to scope its decomposition to a single wave).
--
-- The cross-source scripts/sql/_pipeline/recent_runs.sql intentionally
-- omits the UUID column to keep its multi-source snapshot readable. Use
-- this one whenever you need to pick out specific NHTSA run_ids for
-- downstream wave analysis.
--
-- Shows the last 14 days of NHTSA runs (covers a comfortable retro window
-- for catching up after a daily-update streak). Adjust the interval inline
-- if you need to look further back.
--
-- Run as:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/list_recent_run_ids.sql

\pset null '<NULL>'

\echo
\echo === NHTSA extraction_runs, last 14 days ===
\echo Copy a run_id and feed it to explore_incremental_delta.sql as:
\echo   psql ... -v run_id="'<run_id>'" -f scripts/sql/nhtsa/bronze/explore_incremental_delta.sql

select
    run_id,
    started_at,
    status,
    change_type,
    records_extracted                                   as fetched,
    records_inserted                                    as loaded,
    records_rejected                                    as rejected,
    raw_landing_path
from extraction_runs
where source     = 'nhtsa'
  and started_at >= now() - interval '14 days'
order by started_at desc;
