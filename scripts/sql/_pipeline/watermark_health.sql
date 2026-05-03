-- Watermark progression check.
-- Detects watermarks that didn't advance after the latest successful run —
-- the silent-failure mode where the API filter parameter stops applying and
-- the same window gets re-fetched every run.
--
-- last_cursor semantics differ per source:
--   cpsc, fda            — date string the next request uses as a date filter.
--   usda, usda_establishments — repurposed for the prior response's
--                          Last-Modified HTTP header, used in If-Modified-Since.
--                          (Per Finding D in usda observations: USDA has no
--                          usable date filter, so the cursor isn't a date.)
--
-- last_etag is populated for sources that use ETag-based conditional GETs
-- (USDA recalls + establishments per Finding A/N). NULL for CPSC / FDA.
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/watermark_health.sql

\pset null '<NULL>'

-- 1. Current watermark state per source.
select
    source,
    last_cursor,
    case when last_etag is null then '<NULL>' else left(last_etag, 16) || '...' end as last_etag_prefix,
    last_successful_extract_at,
    updated_at,
    now() - updated_at as time_since_watermark_update
from source_watermarks
order by source;

-- 2. Cross-check: for each source, did the watermark update during its
--    latest successful run? If updated_at < latest run's started_at, the
--    watermark is stuck — either the source returned no new records (benign)
--    or the watermark code path broke (worth investigating).
with latest_successful_run as (
    select distinct on (source) source, run_id, started_at, finished_at,
                                records_extracted, records_inserted
    from extraction_runs
    where status = 'completed'
    order by source, started_at desc
)
select
    sw.source,
    lr.run_id                                                    as latest_run_id,
    lr.started_at                                                as latest_run_started,
    lr.records_inserted                                          as latest_run_inserted,
    sw.updated_at                                                as watermark_updated,
    case
        when lr.started_at is null                          then 'no completed runs yet'
        when sw.updated_at >= lr.started_at                 then 'advanced this run'
        when lr.records_inserted = 0                        then 'stuck (no new records — likely benign)'
        else                                                     'STUCK — investigate (records inserted but watermark did not move)'
    end                                                          as watermark_status
from source_watermarks sw
left join latest_successful_run lr using (source)
order by sw.source;
