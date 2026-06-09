-- PROBE — the USCG MIC "swing set": recalled MICs that have a prior holder but NO (OOB) marker.
--
-- WHY: ADR 0035's firm time-sensitivity flag (recall_event_firm.uscg_event_firms) fires on
-- mic_has_prior_holder OR mic_oob_recycled — the BROAD 365 set (vs the high-confidence 205
-- OOB-only). The difference is THIS swing set (~160 recalled MICs): a prior holder is on record,
-- but none of the three Past Company slots carries the `(OOB)` marker. We dump it to decide whether
-- to keep the broad flag (365) or tighten to OOB-only (205):
--   * real UNMARKED recycles / active-prior reassignments -> keep BROAD 365 (a real misattribution risk)
--   * brand/DBA annotations & same-entity renames          -> tightening to OOB-only 205 gains support
-- Decision home: documentation/decisions/0035-cross-source-scd2-silver-dimensions.md §4 + the 6b.5 thread.
--
-- Mirrors scripts/sql/cross_source/scd_monitors/assert_mic_holder_stable.sql: raw bronze,
-- latest-per-MIC, all three past_company slots. OOB DETECTION BROADENED 2026-06-05: the first run
-- (paren-only `~ '\(OOB'`) surfaced a SECOND notation — a dash form `- OOB` ("ARLINGTON BOAT WORKS
-- - OOB") — so this now matches word-boundary `~ '\yOOB\y'` (also excludes `(previous name)` renames).
-- The original leak is why those dash-OOB recycles showed up IN the swing; with the fix they move OUT.
-- NOTE: the silver model (firm_uscg_attributes.mic_oob_recycled) uses the same `\yOOB\y`;
-- assert_mic_holder_stable.sql's published "205" is still paren-only and undercounts. Past Company sentinels
-- ('-' / 'UNK' / '' / whitespace) are NULLed to match the silver staging view, so the dump shows
-- REAL priors — NOT sentinels. With word-boundary OOB the swing is 144 (= 365 has-prior - 221 OOB);
-- it came back EXACTLY 144 (paren-only OOB gave 160), i.e. no sentinel-only "priors" — all real.
--
-- Output: data/exploratory/uscg/mic_prior_holder_not_oob.csv (gitignored). Run from repo root:
--   mkdir -p data/exploratory/uscg   # if missing
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/scd_monitors/probe_mic_prior_holder_not_oob.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- One session-scoped view (no ON COMMIT DROP — psql autocommits each statement, which would
-- drop a temp table before the later reads; a temp VIEW persists for the session) so the three
-- queries below share one definition instead of repeating the CTEs.
-- DROP-IF-EXISTS first: on a POOLED connection (Neon's -pooler endpoint) the backend can be
-- returned to the pool with this temp view still attached, so a LATER psql run hits
-- "relation mic_swing already exists". The drop makes the script re-runnable on a reused backend.
drop view if exists mic_swing;
create temporary view mic_swing as
with recall_mics as (
  select distinct upper(trim(mic)) as mic
  from uscg_recalls_bronze
  where nullif(trim(mic), '') is not null
),
det as (
  select distinct on (upper(trim(source_recall_id)))
    upper(trim(source_recall_id))                                as mic,
    company_name                                                 as current_holder,
    nullif(nullif(nullif(trim(past_company_1), ''), '-'), 'UNK') as p1,
    nullif(nullif(nullif(trim(past_company_2), ''), '-'), 'UNK') as p2,
    nullif(nullif(nullif(trim(past_company_3), ''), '-'), 'UNK') as p3,
    out_of_business
  from uscg_manufacturer_details_bronze
  order by upper(trim(source_recall_id)), extraction_timestamp desc
)
select
  d.mic,
  d.current_holder,
  (d.out_of_business is not null)                                as current_holder_defunct,
  d.p1,
  d.p2,
  d.p3,
  -- HINT (not authoritative): a prior slot that reads like a brand / DBA annotation rather than a
  -- distinct corporate holder (e.g. '(BRAND NAME)', '(DBA)', 'DBA X') -> probably NOT a real
  -- recycle. Eyeball before trusting; substring match can false-positive on a real name.
  (    coalesce(upper(d.p1), '') like any (array['%BRAND%', '%DBA%'])
    or coalesce(upper(d.p2), '') like any (array['%BRAND%', '%DBA%'])
    or coalesce(upper(d.p3), '') like any (array['%BRAND%', '%DBA%'])) as has_brand_or_dba_marker
from det d
join recall_mics rm on rm.mic = d.mic
where coalesce(d.p1, d.p2, d.p3) is not null            -- has_prior_holder (cleaned)
  and not (                                             -- NOT oob_recycled (mirrors the silver model)
        coalesce(d.p1, '') ~ '\yOOB\y'
     or coalesce(d.p2, '') ~ '\yOOB\y'
     or coalesce(d.p3, '') ~ '\yOOB\y');

\echo '=== Q1: swing-set size (recalled MICs: prior holder present, NO (OOB) marker) ==='
-- Expected 144 (365 has_prior - 221 word-boundary OOB; paren-only gave 160). Measured 144 = no
-- sentinel shrinkage (all real priors) -> the residual is genuine prior firms + renames, not noise.
select
  count(*)                                          as swing_mics,
  count(*) filter (where has_brand_or_dba_marker)   as look_like_brand_dba,
  count(*) filter (where current_holder_defunct)    as current_holder_also_defunct
from mic_swing;

\echo ''
\echo '=== Q2: console sample (first 40; brand/DBA-looking first) ==='
select mic, current_holder, has_brand_or_dba_marker as brand_dba, p1, p2, p3
from mic_swing
order by has_brand_or_dba_marker desc, mic
limit 40;

\echo ''
\echo '=== Q3: full dump -> data/exploratory/uscg/mic_prior_holder_not_oob.csv ==='
\copy (select mic, current_holder, current_holder_defunct, has_brand_or_dba_marker, p1, p2, p3 from mic_swing order by has_brand_or_dba_marker desc, mic) to 'data/exploratory/uscg/mic_prior_holder_not_oob.csv' with (format csv, header true)

\echo 'done.'
