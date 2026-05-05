-- Spot-check extraction_runs after `recalls extract nhtsa` runs.
--
-- Confirms the new flat-file forensic columns (introduced in migration
-- 0011) populate correctly, and gives a working query against the
-- inner-content hash for the Finding H Q1 cadence study (closes
-- implicitly once ~7 days of data accumulate).
--
-- Why two hashes? `response_body_sha256` (universal, migration 0010)
-- captures the wrapper ZIP bytes — NON-deterministic across days for
-- NHTSA per Finding J (daily re-zip with non-deterministic metadata).
-- `response_inner_content_sha256` (NHTSA + future flat-file sources,
-- migration 0011) captures the decompressed TSV bytes — the
-- authoritative "did the data change?" oracle.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/spot_check_extraction_runs.sql

\echo
\echo '================================================================'
\echo '1. Most recent NHTSA run — confirms all forensic columns populate'
\echo '================================================================'

select
    id,
    started_at,
    finished_at,
    status,
    change_type,
    records_extracted,
    records_inserted,
    records_rejected,
    raw_landing_path,
    response_status_code,
    response_etag,
    response_last_modified,
    -- Truncate hashes for readability; full values in queries 3-4 below.
    left(response_body_sha256, 16)          as wrapper_hash_prefix,
    left(response_inner_content_sha256, 16) as inner_hash_prefix
from extraction_runs
where source = 'nhtsa'
order by started_at desc
limit 1;

\echo
\echo '================================================================'
\echo '2. Forensic-column population check — counts non-null per column'
\echo '   across all NHTSA runs. Each successful run should populate'
\echo '   ALL of these (incl. response_inner_content_sha256 — that is'
\echo '   the column landed by migration 0011 and unique to flat-file).'
\echo '================================================================'

select
    count(*)                                              as total_runs,
    count(*) filter (where status = 'success')            as successful_runs,
    count(response_status_code)                           as has_status,
    count(response_etag)                                  as has_etag,
    count(response_last_modified)                         as has_last_modified,
    count(response_body_sha256)                           as has_wrapper_hash,
    count(response_inner_content_sha256)                  as has_inner_hash,
    count(response_headers)                               as has_full_headers
from extraction_runs
where source = 'nhtsa';

\echo
\echo '================================================================'
\echo '3. Day-over-day inner-content stability — closes Finding H Q1'
\echo '   (cadence) over ~7 days of accumulated runs. The wrapper hash'
\echo '   shifts daily even when content is unchanged (Finding J); the'
\echo '   inner hash transitions track REAL upstream content updates.'
\echo
\echo '   Read: each row = one run. If wrapper_hash changes but'
\echo '   inner_hash matches the prior run, NHTSA re-zipped identical'
\echo '   data (no real change). If inner_hash transitions, content'
\echo '   actually changed that day.'
\echo '================================================================'

select
    started_at::date                                       as run_date,
    started_at::time(0)                                    as run_time,
    change_type,
    left(response_body_sha256, 16)                         as wrapper_hash,
    left(response_inner_content_sha256, 16)                as inner_hash,
    case
        when lag(response_inner_content_sha256) over (order by started_at)
             = response_inner_content_sha256
        then 'unchanged'
        when lag(response_inner_content_sha256) over (order by started_at) is null
        then 'first_run'
        else 'CHANGED'
    end                                                    as inner_transition,
    case
        when lag(response_body_sha256) over (order by started_at)
             = response_body_sha256
        then 'unchanged'
        when lag(response_body_sha256) over (order by started_at) is null
        then 'first_run'
        else 'changed'
    end                                                    as wrapper_transition
from extraction_runs
where source = 'nhtsa'
  and status = 'success'
  and response_inner_content_sha256 is not null
order by started_at desc
limit 14;

\echo
\echo '================================================================'
\echo '4. Bronze sanity — row count + most recent extraction timestamp'
\echo '================================================================'

select
    (select count(*) from nhtsa_recalls_bronze)                     as bronze_rows,
    (select count(*) from nhtsa_recalls_rejected)                   as rejected_rows,
    (select max(extraction_timestamp) from nhtsa_recalls_bronze)    as latest_bronze_ts,
    (select last_successful_extract_at from source_watermarks
      where source = 'nhtsa')                                       as watermark_freshness;

\echo
\echo '================================================================'
\echo '5. Quarantine breakdown — first run should have 0 rejected'
\echo '   records on the incremental path (POST_2010 has 0 empty'
\echo '   RCDATE; 5 expected on the deep-rescan PRE_2010 if you'
\echo '   ran the historical seed. Any other counts are surprises.)'
\echo '================================================================'

select
    failure_stage,
    count(*) as count,
    -- Sample one failure_reason per stage for quick triage.
    min(failure_reason) as sample_reason
from nhtsa_recalls_rejected
group by failure_stage
order by count desc;
