-- ETag audit-run integrity check.
--
-- Companion to scripts/sql/_pipeline/etag_viability.sql. The viability
-- script can only observe ETag *generation* honesty — does the server's
-- ETag drift on identical content? — because while etag_enabled was off,
-- the extractor never sent If-None-Match and the server never had reason
-- to return a 304. Once etag_enabled is on, observed 304s are always
-- correct *as far as the script can tell* (no body to compare). A real
-- false-304 (server returns 304 but underlying data has changed) is
-- structurally undetectable from a 304 row alone.
--
-- An audit run closes that gap. Operationally:
--
--   recalls extract usda --change-type=etag_audit
--   recalls extract usda_establishments --change-type=etag_audit
--
-- The CLI sets etag_enabled=False for that run, forcing an unconditional
-- GET (no If-None-Match / If-Modified-Since). The server returns 200 with
-- the current body, the extractor captures response_body_sha256, and
-- bronze content-hash dedup absorbs the body if unchanged.
--
-- This script then compares each etag_audit run's body sha against the
-- most recent prior non-audit 200 ("baseline"), counts any 304s in the
-- intervening window, and produces a verdict:
--
--   audit_body = baseline_body, intervening_304s ≥ 0
--       → ETag honest; the 304s correctly represented unchanged content.
--   audit_body ≠ baseline_body, intervening_304s = 0
--       → Legitimate update happened *after* the baseline run; no
--         conditional-GET round-trip in between, so no false-304 evidence.
--   audit_body ≠ baseline_body, intervening_304s ≥ 1
--       → POTENTIAL FALSE-304: at least one 304 lied about content
--         being unchanged. Can't be 100% certain without timing details
--         of when the upstream change occurred, but warrants investigation
--         and potentially flipping etag_enabled back to False on the
--         affected source until resolved.
--
-- Run as:  psql -f scripts/sql/_pipeline/etag_audit_check.sql -v src=usda
-- Defaults to 'usda' if -v src=... is omitted.

\pset null '<NULL>'

\if :{?src}
\else
    \set src 'usda'
\endif

\echo
\echo === ETag audit-run integrity: :src ===
\echo

-- For each etag_audit run, find the immediately prior non-audit 200 with a
-- body sha (the "baseline"), then count the 304s sitting between them.
-- LATERAL joins keep the per-row lookups co-located with the audit row.
with audit_runs as (
    select
        started_at        as audit_started_at,
        response_etag     as audit_etag,
        response_body_sha256 as audit_body
    from extraction_runs
    where source = :'src'
      and status = 'success'
      and change_type = 'etag_audit'
      and response_status_code = 200
      and response_body_sha256 is not null
)
select
    a.audit_started_at,
    b.started_at                                        as baseline_started_at,
    a.audit_etag,
    b.response_etag                                     as baseline_etag,
    coalesce(i304.cnt, 0)                               as intervening_304s,
    case
        when b.started_at is null
            then 'NO BASELINE — first audit run / no prior 200 captured'
        when a.audit_body = b.response_body_sha256
            then 'CONSISTENT — body unchanged since baseline'
        else 'CHANGED — body differs from baseline'
    end                                                  as body_verdict,
    case
        when b.started_at is null                       then null
        when a.audit_body = b.response_body_sha256      then 'no false-304 evidence'
        when coalesce(i304.cnt, 0) = 0
            then 'legitimate update (no intervening 304s — change happened post-baseline)'
        else 'POTENTIAL FALSE-304: body changed and 304(s) intervened — investigate'
    end                                                  as final_verdict
from audit_runs a
left join lateral (
    select started_at, response_etag, response_body_sha256
    from extraction_runs prev
    where prev.source = :'src'
      and prev.status = 'success'
      and prev.change_type <> 'etag_audit'
      and prev.response_status_code = 200
      and prev.response_body_sha256 is not null
      and prev.started_at < a.audit_started_at
    order by prev.started_at desc
    limit 1
) b on true
left join lateral (
    select count(*) as cnt
    from extraction_runs c
    where c.source = :'src'
      and c.status = 'success'
      and c.response_status_code = 304
      and c.started_at > b.started_at
      and c.started_at < a.audit_started_at
) i304 on true
order by a.audit_started_at desc;

-- Coverage summary: how many audit runs has this source seen, and how
-- recent is the most recent one? An audit cadence drifting too far past
-- the last run weakens the guarantee.
select
    count(*)                                      as audit_runs,
    min(started_at)                               as first_audit,
    max(started_at)                               as last_audit,
    now() - max(started_at)                       as time_since_last_audit
from extraction_runs
where source = :'src'
  and status = 'success'
  and change_type = 'etag_audit';
