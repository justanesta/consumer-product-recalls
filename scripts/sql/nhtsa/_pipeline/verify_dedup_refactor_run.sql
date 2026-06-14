-- verify_dedup_refactor_run.sql — did the staged-dedup refactor behave on the
-- daily run that already happened?
--
-- Verifies the conservative fetch-only restructure of
-- BronzeLoader._fetch_existing_hashes (the `_fetch_existing_hashes_staged`
-- TEMP-table JOIN, src/bronze/loader.py:341) from the run's OWN telemetry in
-- `extraction_runs` + bronze — no log reading required. It is the retrospective
-- analog of plan-Verification step 3 ("shadow extract: records_inserted == the
-- expected small delta") applied to the daily run you just executed.
--
-- It does NOT exercise the staged path itself (that needs a change day or the
-- DB-backed pytest differential). It tells you (a) whether the staged path even
-- RAN today, and if so (b) whether it stayed fast and inserted the right delta
-- rather than re-inserting the whole staged set (the dedup-divergence signature).
--
-- Run as the runtime role (doubles as proof recalls_app's run succeeded):
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/verify_dedup_refactor_run.sql
--
-- Companion: spot_check_extraction_runs.sql (forensic columns / inner-SHA cadence),
-- list_recent_run_ids.sql (pick run_ids for explore_incremental_delta.sql).

\pset null '<NULL>'

\echo
\echo '================================================================'
\echo '1. Last 5 NHTSA runs — the refactor lens'
\echo '----------------------------------------------------------------'
\echo '  short_circuited = t  -> inner-SHA matched prior; parse+dedup were'
\echo '       SKIPPED. The staged JOIN did NOT run this time (inserted=0 is'
\echo '       expected). This run validates the short-circuit, not the join;'
\echo '       to exercise the staged path use a change day or the pytest diff.'
\echo '  short_circuited = f  -> full walk; the staged fetch RAN. Then:'
\echo '       status=success      -> recalls_app could CREATE TEMP + ANALYZE'
\echo '                              (the new TEMPORARY-privilege dependency).'
\echo '       duration ~1-2 min   -> the perf win landed (was ~17 min; total'
\echo '                              run duration ~= load_bronze, download is ~13s).'
\echo '       insert_pct (inserted/fetched):'
\echo '            low (few %)     -> HEALTHY dedup (only changed/new rows kept).'
\echo '            ~100%           -> OVER-INSERT red flag: nearly every fetched'
\echo '                               row missed its bronze twin = dedup'
\echo '                               divergence (UNLESS this is a legitimate'
\echo '                               first-load / rebaseline run).'
\echo '================================================================'

select
    started_at::timestamp(0)                              as started_at,
    (finished_at - started_at)                            as duration,
    status,
    change_type,
    was_short_circuited                                   as short_circuited,
    records_extracted                                     as fetched,
    records_inserted                                      as inserted,
    records_rejected                                      as rejected,
    case
        when records_extracted is null or records_extracted = 0 then null
        else round(100.0 * records_inserted / records_extracted, 1)
    end                                                   as insert_pct
from extraction_runs
where source = 'nhtsa'
order by started_at desc
limit 5;

\echo
\echo '================================================================'
\echo '2. Bronze-side cross-check — rows actually appended per run'
\echo '   (load() stamps ONE extraction_timestamp on all rows it inserts,'
\echo '    so the top row = the latest run''s real insert count. A staged'
\echo '    JOIN that diverged would append ~the whole fetched set here.)'
\echo '================================================================'

select
    extraction_timestamp::timestamp(0)                    as extraction_ts,
    count(*)                                              as rows_appended
from nhtsa_recalls_bronze
group by extraction_timestamp
order by extraction_timestamp desc
limit 8;

\echo
\echo '================================================================'
\echo '3. Bronze totals sanity'
\echo '================================================================'

select
    (select count(*) from nhtsa_recalls_bronze)                          as bronze_rows,
    (select count(*) from nhtsa_recalls_rejected)                        as rejected_rows,
    (select max(extraction_timestamp) from nhtsa_recalls_bronze)         as latest_bronze_ts,
    (select last_successful_extract_at from source_watermarks
       where source = 'nhtsa')                                           as watermark;
