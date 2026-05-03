-- Watermark progression check.
-- Detects watermarks that didn't advance after the latest successful run —
-- the silent-failure mode where the API filter parameter stops applying and
-- the same window gets re-fetched every run.
--
-- last_cursor semantics differ per source:
--   cpsc, fda            — ISO date string sent as a server-side date filter
--                          on the next request. Real incremental.
--   usda                 — RFC 1123 Last-Modified HTTP header captured from
--                          the prior response. Currently captured for
--                          observation only — NOT yet sent as If-Modified-Since
--                          on subsequent requests (etag_enabled=False per
--                          Finding N, awaiting multi-day evidence of
--                          consistency before depending on it). "0 inserted"
--                          comes from bronze content-hash dedup, not a 304.
--                          Per Finding D, USDA has no usable date filter, so
--                          the cursor isn't a date even when ETag is enabled.
--   usda_establishments  — NULL by design. The extractor does not yet send
--                          conditional-GET headers — every run is a full
--                          dump and bronze content-hash dedup handles
--                          idempotency. Note: the API DOES emit ETag and
--                          Last-Modified under browser fingerprint (Finding
--                          A revision 2026-05-03 reversed the original
--                          "absent" claim); viability for enabling
--                          conditional GET is under study via
--                          etag_viability.sql alongside the recall endpoint.
--   nhtsa, uscg          — extractors not yet implemented (Phase 5c/5d);
--                          watermark rows pre-seeded.
--
-- last_etag — captured from the USDA recall response ETag header for the
-- multi-day consistency study (Finding N). NOT yet sent as If-None-Match on
-- subsequent requests until etag_enabled flips to True. Always NULL for cpsc,
-- fda (no ETag mechanism on those APIs) and usda_establishments (no ETag by
-- design).
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
    where status = 'success'
    order by source, started_at desc
)
select
    sw.source,
    lr.run_id                                                    as latest_run_id,
    lr.started_at                                                as latest_run_started,
    lr.records_inserted                                          as latest_run_inserted,
    sw.updated_at                                                as watermark_updated,
    case
        when lr.started_at is null
            then 'no successful runs yet'
        when sw.last_cursor is null
             and sw.last_etag is null
             and sw.last_successful_extract_at is null
            then 'no watermark by design (full-dump source)'
        when sw.updated_at >= lr.started_at
            then 'advanced this run'
        when lr.records_inserted = 0
            then 'stuck (no new records — likely benign)'
        else 'STUCK — investigate (records inserted but watermark did not move)'
    end                                                          as watermark_status
from source_watermarks sw
left join latest_successful_run lr using (source)
order by sw.source;
