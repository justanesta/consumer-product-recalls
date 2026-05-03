-- ETag viability study for the USDA recall API.
--
-- Goal: empirically determine whether USDA's ETag (and Last-Modified) headers
-- are a reliable signal that the underlying recall data has changed, before
-- flipping etag_enabled=True (Finding N).
--
-- The bronze content-hash dedup is the ground-truth oracle for "did the data
-- change?" (records_inserted + response_body_sha256 cover inserts/updates/
-- deletes byte-exactly). Correlating that ground truth against the captured
-- ETag/Last-Modified reveals four cases:
--
--   ETag  same  + body  same → consistent, ETag honest
--   ETag  diff  + body  diff → consistent, ETag honest
--   ETag  diff  + body  same → false-200: ETag drifted without real change
--                              (likely cache-eviction; you'd over-fetch)
--   ETag  same  + body  diff → false-304: data changed but ETag stayed put
--                              (you would MISS updates if etag_enabled flips on)
--
-- A clean run of this study (2+ weeks, ≥1 real-update period, zero false-304s)
-- is the gate for flipping etag_enabled=True. The analysis machinery is
-- universal — runs against any source that captures response_* columns. Pass
-- the source as a psql variable: `psql -f etag_viability.sql -v src=usda`
-- (defaults to 'usda' if unset).
--
-- Captured columns (added in migration 0010):
--   response_status_code, response_etag, response_last_modified,
--   response_body_sha256, response_headers (jsonb)
--
-- Run as:  psql -f scripts/sql/_pipeline/etag_viability.sql

\pset null '<NULL>'

-- Default the source variable to 'usda' if not passed via -v src=...
\if :{?src}
\else
    \set src 'usda'
\endif

\echo
\echo === Source under study: :src ===
\echo

-- 1. Change-signal verdict (the headline query).
--    For every successful run, compare ETag and body hash against the prior
--    successful run. SUSPECT verdicts mean the ETag is misleading.
with transitions as (
    select
        started_at,
        response_status_code,
        response_etag,
        response_body_sha256,
        records_inserted,
        records_extracted,
        lag(response_etag)        over w as prev_etag,
        lag(response_body_sha256) over w as prev_body
    from extraction_runs
    where source = :'src' and status = 'success'
    window w as (order by started_at)
)
select
    started_at,
    response_status_code                          as status,
    case
        when prev_etag is null
            then '(first run — no prior to compare)'
        when response_body_sha256 =  prev_body
         and response_etag        =  prev_etag
            then 'consistent: nothing changed'
        when response_body_sha256 != prev_body
         and response_etag        != prev_etag
            then 'consistent: both changed'
        when response_body_sha256 =  prev_body
         and response_etag        != prev_etag
            then 'SUSPECT false-200: etag drifted, body unchanged (cache-layer artifact?)'
        when response_body_sha256 != prev_body
         and response_etag        =  prev_etag
            then 'SUSPECT false-304: body changed, etag stable (would miss updates)'
    end                                           as verdict,
    records_inserted,
    response_etag,
    prev_etag
from transitions
order by started_at desc;

-- 2. ETag format inspection — strong/weak, shape, value distribution.
--    A weak ETag (W/ prefix) is "semantically equivalent" only — not byte-exact.
--    Numeric-looking ETags often indicate a timestamp; hex usually indicates
--    a content hash from origin or a cache fingerprint.
select
    response_etag,
    response_etag like 'W/%'                      as is_weak,
    response_etag ~ '^"[0-9]+"$'                  as looks_unix_timestamp,
    response_etag ~ '^"[0-9a-f]+"$'               as looks_hex_hash,
    length(response_etag)                         as chars,
    count(*)                                      as occurrences,
    min(started_at)                               as first_seen,
    max(started_at)                               as last_seen
from extraction_runs
where source = :'src' and response_etag is not null
group by response_etag
order by occurrences desc, response_etag;

-- 3. Origin vs CDN fingerprinting from response headers.
--    `Server: AkamaiGHost` outs Akamai directly. `X-Cache: TCP_HIT` proves the
--    response came from cache, not origin. Non-zero `Age` confirms cache-served.
--    Combined with verdicts in query 1, you can say "ETags from cache-served
--    responses behave reliably/unreliably."
select
    started_at,
    response_status_code                          as status,
    response_headers ->> 'server'                 as server,
    response_headers ->> 'x-cache'                as x_cache,
    response_headers ->> 'age'                    as age_sec,
    response_headers ->> 'via'                    as via,
    response_headers ->> 'cache-control'          as cache_control,
    response_etag
from extraction_runs
where source = :'src' and status = 'success'
order by started_at desc
limit 30;

-- 4. Intra-day stability — your "multiple runs same day" use case.
--    distinct_etags > distinct_bodies on a given day = ETag drifts within a day
--    on identical content (cache-layer artifact). distinct_etags = distinct_bodies
--    = 1 across multiple runs = stable, encouraging signal.
select
    started_at::date                                  as day,
    count(*)                                          as runs,
    count(distinct response_etag)                     as distinct_etags,
    count(distinct response_body_sha256)              as distinct_bodies,
    sum(records_inserted)                             as total_inserted,
    min((response_headers ->> 'age')::int)            as min_age_sec,
    max((response_headers ->> 'age')::int)            as max_age_sec
from extraction_runs
where source = :'src' and status = 'success'
group by 1
order by 1 desc;

-- 5. Summary verdict — the single-row green-light decision.
--    After 14+ days of capture INCLUDING days with real upstream updates,
--    interpret as:
--      false_304_count = 0  → safe to flip etag_enabled=True
--      false_304_count > 0  → leave disabled (would silently miss updates)
--      false_200_count > 0  → safe to enable; you'll over-fetch sometimes,
--                             bronze hash dedup absorbs it
with transitions as (
    select
        response_etag,
        response_body_sha256,
        lag(response_etag)        over w as prev_etag,
        lag(response_body_sha256) over w as prev_body
    from extraction_runs
    where source = :'src' and status = 'success'
    window w as (order by started_at)
),
verdicts as (
    select
        case
            when prev_etag is null                        then null
            when response_body_sha256 =  prev_body
             and response_etag        =  prev_etag       then 'consistent_unchanged'
            when response_body_sha256 != prev_body
             and response_etag        != prev_etag       then 'consistent_changed'
            when response_body_sha256 =  prev_body
             and response_etag        != prev_etag       then 'false_200'
            when response_body_sha256 != prev_body
             and response_etag        =  prev_etag       then 'false_304'
        end as v
    from transitions
)
select
    count(*) filter (where v is not null)                       as total_transitions,
    count(*) filter (where v = 'consistent_unchanged')          as consistent_unchanged,
    count(*) filter (where v = 'consistent_changed')            as consistent_changed,
    count(*) filter (where v = 'false_200')                     as false_200_count,
    count(*) filter (where v = 'false_304')                     as false_304_count,
    case
        when count(*) filter (where v is not null) < 7          then 'INSUFFICIENT DATA — need ≥7 transitions, ideally 14+ days including a real update'
        when count(*) filter (where v = 'false_304') > 0        then 'DO NOT ENABLE — false-304 detected; would miss updates'
        when count(*) filter (where v = 'false_200') > 0        then 'SAFE TO ENABLE — false-200 only (over-fetch sometimes; bronze absorbs)'
        else                                                         'SAFE TO ENABLE — fully consistent across observed window'
    end                                                          as recommendation
from verdicts;
