-- Verify the etag_viability.sql phantom false-304 hypothesis for usda_establishments.
--
-- Context: scripts/sql/_pipeline/etag_viability.sql consistently flags the
-- 2026-05-10 16:05:52 usda_establishments 200 run as SUSPECT false-304 (and
-- consequently the day's recommendation flips to "DO NOT ENABLE"). The
-- diagnosis on 2026-05-10 (Finding A addendum in
-- documentation/usda/establishment_api_observations.md) was: the immediately
-- prior 2026-05-10 13:19:14 routine 304 was captured BEFORE the
-- _capture_response fix landed, and that 304 row persists sha256("") in its
-- response_body_sha256 column. The viability script's verdict CASE compares
-- the audit 200's real body hash against that sentinel, sees them differ
-- while ETag stays stable, and fires SUSPECT false-304.
--
-- This script verifies the hypothesis is still load-bearing today (the
-- addendum was filed 2 weeks ago — rows may have been touched, or the fix
-- may have backfilled them since). It prints all usda_establishments runs
-- between 2026-05-08 and 2026-05-11 inclusive, with the full
-- response_body_sha256 plus a derived classification column.
--
-- Expected pattern if the phantom hypothesis still holds:
--   5/8  20:00 200 | real_X        | etag "1777998738"  (prior plateau)
--   5/9  17:47 200 | real_Y        | etag "1778270406"  (new plateau begins)
--   5/10 13:19 304 | sha256(empty) | etag "1778270406"  (pre-fix sentinel — smoking gun)
--   5/10 16:05 200 | real_Y        | etag "1778270406"  (audit run; body matches 5/9)
--   5/10 17:50 304 | NULL          | etag "1778270406"  (post-fix — NULL body)
--   5/11 13:06 200 | real_Z        | etag "1778499930"  (real upstream change; new etag)
--
-- Falsification criteria (any one of these means the phantom hypothesis is wrong
-- and a different mechanism is producing the false-304 verdict):
--   * 5/10 13:19 304's body_sha256 is NULL (= fix DID backfill; phantom should be gone)
--   * 5/10 13:19 304's body_sha256 differs from sha256("") and is non-null
--   * 5/10 16:05 200's body_sha256 differs from 5/9 17:47 200's body_sha256
--     (= real body change happened between the 5/9 baseline and the 5/10 audit;
--      false-304 would then be genuine, not phantom — a far more serious finding)
--
-- Run as:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/_pipeline/verify_etag_viability_phantom.sql

\pset null '<NULL>'

\echo
\echo === usda_establishments rows 2026-05-08 through 2026-05-11 ===
\echo Classification compares response_body_sha256 against the known
\echo pre-fix sentinel sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

select
    started_at,
    response_status_code                                                            as status,
    response_etag,
    response_body_sha256,
    case
        when response_body_sha256 is null
            then 'NULL (post-fix 304 or unpopulated)'
        when response_body_sha256 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
            then 'sha256("") — pre-fix 304 sentinel'
        else 'real body hash'
    end                                                                             as body_classification,
    records_inserted,
    change_type
from extraction_runs
where source     = 'usda_establishments'
  and started_at >= '2026-05-08'
  and started_at <  '2026-05-12'
order by started_at;

\echo
\echo === Body-hash equivalence check across the 5/9 baseline and 5/10 audit ===
\echo If the audit run (5/10 16:05) saw the same body as the 5/9 17:47 baseline,
\echo the false-304 verdict is purely the sha256("") sentinel artifact — phantom
\echo confirmed. If the body hashes differ, the false-304 may be genuine and
\echo demands deeper investigation.

with rows_of_interest as (
    select started_at, response_status_code, response_body_sha256
    from extraction_runs
    where source = 'usda_establishments'
      and (
          started_at = (select started_at from extraction_runs
                        where source = 'usda_establishments'
                          and started_at::date = '2026-05-09'
                          and response_status_code = 200
                        order by started_at desc limit 1)
       or started_at = (select started_at from extraction_runs
                        where source = 'usda_establishments'
                          and started_at::date = '2026-05-10'
                          and response_status_code = 200
                        order by started_at limit 1)
      )
),
distinct_body_count as (
    -- Postgres window functions do not support DISTINCT; compute the
    -- distinct count once in its own CTE and cross-join it back in.
    select count(distinct response_body_sha256) as n_distinct
    from rows_of_interest
)
select
    r.started_at,
    r.response_status_code      as status,
    r.response_body_sha256,
    case
        when d.n_distinct = 1
            then 'MATCH — phantom hypothesis holds'
        else 'DIFFER — phantom hypothesis falsified; investigate'
    end                         as equivalence_verdict
from rows_of_interest r
cross join distinct_body_count d
order by r.started_at;
