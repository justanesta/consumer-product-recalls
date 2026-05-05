-- DEVELOPMENT ONLY — truncates NHTSA bronze + rejected + extraction_runs
-- history so the dev Neon branch can be re-seeded with a smaller subset
-- via `recalls extract nhtsa --since YYYY-MM-DD`.
--
-- Use this when the free-tier 0.54 GB Neon allowance is bumping against
-- a full 240k-row NHTSA bronze. The dev workflow is:
--   1. Run this script (frees ~300 MB).
--   2. Re-extract a recent slice: `recalls extract nhtsa --since 2024-01-01`
--      (~30k rows, ~50 MB).
--   3. Continue developing through Phase 6 / 7.
--   4. Production historical seed in Phase 7 uses the deep-rescan path,
--      which has no `--since` filter and lands the full ~322k-row corpus.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/truncate_for_dev_subset.sql
--
-- WILL NOT RUN against production. As a tripwire, the script will refuse
-- to proceed if the database has any non-routine extraction_runs row
-- (i.e. anything tagged historical_seed / schema_rebaseline /
-- hash_helper_rebaseline) — those mark a real production seed event and
-- should never be wiped accidentally.

\set ON_ERROR_STOP on

\echo
\echo '================================================================'
\echo 'Tripwire — block truncation if any non-routine NHTSA run exists.'
\echo 'A historical_seed / schema_rebaseline / hash_helper_rebaseline'
\echo 'row marks a real production seed event and should never be'
\echo 'wiped accidentally.'
\echo '================================================================'

do $$
declare
    n_nonroutine int;
begin
    select count(*) into n_nonroutine
    from extraction_runs
    where source = 'nhtsa' and change_type <> 'routine';
    if n_nonroutine > 0 then
        raise exception
            'ABORT: % non-routine NHTSA extraction_runs row(s) found. '
            'Inspect: select id, started_at, change_type from extraction_runs '
            'where source = ''nhtsa'' and change_type <> ''routine''.',
            n_nonroutine;
    end if;
    raise notice 'tripwire passed: no non-routine NHTSA runs detected';
end $$;

\echo
\echo '================================================================'
\echo 'Truncating nhtsa_recalls_bronze + nhtsa_recalls_rejected,'
\echo 'deleting NHTSA extraction_runs history, resetting watermark.'
\echo '================================================================'

begin;

truncate table nhtsa_recalls_bronze, nhtsa_recalls_rejected;

delete from extraction_runs where source = 'nhtsa';

update source_watermarks
set last_successful_extract_at = null,
    last_etag                  = null,
    last_cursor                = null,
    updated_at                 = now()
where source = 'nhtsa';

commit;

\echo
\echo '================================================================'
\echo 'Done. Re-seed with:'
\echo "  recalls extract nhtsa --since 2024-01-01"
\echo "(or your chosen start date — RCDATE >= --since is the filter.)"
\echo
\echo 'Note: the wrapper ZIP landed during the previous run still lives'
\echo 'in R2 under the path stored in extraction_runs.raw_landing_path'
\echo 'before this truncate. R2 lifecycle / cleanup is a separate task'
\echo '(operator decision: keep for re-ingest forensics, or delete to'
\echo 'reclaim ~14 MB).'
\echo '================================================================'
