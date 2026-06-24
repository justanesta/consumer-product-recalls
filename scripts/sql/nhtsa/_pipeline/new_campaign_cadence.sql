-- NHTSA NEW-CAMPAIGN cadence — "how often does a genuinely-new recall arrive?"
--
-- Q context (2026-06-24): mart_recall_summary has surfaced no new NHTSA recall
-- for ~7 days. Is that gap an outlier vs NHTSA's real publishing rhythm?
--
-- This measures arrivals at the RECALL-EVENT grain (distinct campno), which is
-- what "a new recall surfaces in the mart" actually means. It is DISTINCT from
-- the existing `inner_content_cadence.sql` (which measures whether the flat file
-- changed AT ALL on a given day — edits included). A day can change content
-- (edits/amendments) without bringing a single new campaign; this script
-- separates the two.
--
-- Two independent date lenses, because each has a blind spot:
--   * rcdate (RCDATE = Part 573 report-received date = recall_event.announced_at).
--     The recall's TRUE date, but it is BACKDATED relative to publication and is
--     occasionally corrected backward (incremental_delta_findings.md Section M),
--     so the newest rcdate naturally lags "today" even on an active week.
--   * first-bronze-appearance (min extraction_timestamp per campno). The date WE
--     first saw the campaign. Immune to rcdate backdating, but only meaningful
--     for campnos that arrived AFTER the 2026-05-31 full-corpus seed (pre-seed
--     campnos all share first_seen = seed day) and is confounded by the monthly
--     PRE_2010 deep-rescan (old archive recalls first appear on the rescan day),
--     so Q3 cross-filters on rcdate recency to isolate genuinely-new recalls.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/new_campaign_cadence.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q0: headline — how stale is the newest NHTSA recall? ==='
\echo 'max_announced_days_ago is the gap the user observed (by announce/rcdate).'
\echo 'newest_new_arrival_days_ago is the last time a brand-new campno hit bronze'
\echo '(post-seed, with a recent rcdate so archive backfill is excluded).'

with mart_nhtsa as (
    select max(announced_at)::date as max_announced
    from mart_recall_summary
    where source = 'NHTSA'
),
new_arrivals as (
    select campno, min(extraction_timestamp)::date as first_seen, max(rcdate)::date as rcdate
    from nhtsa_recalls_bronze
    group by campno
    having min(extraction_timestamp) >= date '2026-06-01'   -- post full-corpus seed
       and max(rcdate) >= current_date - 90                  -- genuinely-new, not PRE_2010 backfill
)
select
    (select max_announced from mart_nhtsa)                          as max_announced_in_mart,
    current_date - (select max_announced from mart_nhtsa)           as max_announced_days_ago,
    (select max(first_seen) from new_arrivals)                      as newest_new_arrival,
    current_date - (select max(first_seen) from new_arrivals)       as newest_new_arrival_days_ago,
    (select count(*) from new_arrivals)                             as new_campaigns_since_seed;

\echo
\echo '=== Q1: new campaigns per ISO week, last 26 weeks (by rcdate / announce date) ==='
\echo 'distinct_new_campaigns = count of distinct campno whose announce date fell in'
\echo 'that week. Weeks with 0 are the natural quiet periods — note how common they'
\echo 'are. (A campno announced months ago that only got EDITED this week does NOT'
\echo 'appear here — that is the whole point.)'

select
    date_trunc('week', announced_at)::date           as iso_week_start,
    count(*)                                          as distinct_new_campaigns
from mart_recall_summary
where source = 'NHTSA'
  and announced_at >= current_date - 182
group by iso_week_start
order by iso_week_start;

\echo
\echo '=== Q2: gap distribution between consecutive recall (announce) dates, last 12 months ==='
\echo 'For the set of DISTINCT calendar dates on which any NHTSA recall was announced,'
\echo 'how many days separate one announce-date from the next? If 5-10 day gaps are'
\echo 'common, the observed 7-day gap is ordinary, not an anomaly. (NHTSA routine'
\echo 'extract is Mon-Fri, so weekend-spanning gaps of 3+ days are expected baseline.)'

with announce_days as (
    select distinct announced_at::date as d
    from mart_recall_summary
    where source = 'NHTSA'
      and announced_at >= current_date - 365
),
gaps as (
    select d, d - lag(d) over (order by d) as gap_days
    from announce_days
)
select
    gap_days,
    count(*)                                                   as occurrences,
    round(100.0 * count(*) / sum(count(*)) over (), 1)         as pct_of_gaps
from gaps
where gap_days is not null
group by gap_days
order by gap_days;

\echo
\echo '=== Q3: genuinely-new campaigns by first-bronze-appearance day, last 30 days ==='
\echo 'campno first seen in bronze in the last 30 days AND with an rcdate in the last'
\echo '90 days (so PRE_2010 deep-rescan archive backfill is excluded). This is the'
\echo 'arrival cadence immune to rcdate backdating. Empty rows for a day = no new'
\echo 'campaign arrived that day.'

select
    min_seen::date                          as first_seen_day,
    count(*)                                as new_campaigns,
    min(rcdate)::date                       as earliest_rcdate,
    max(rcdate)::date                       as latest_rcdate
from (
    select campno,
           min(extraction_timestamp) as min_seen,
           max(rcdate)               as rcdate
    from nhtsa_recalls_bronze
    group by campno
    having min(extraction_timestamp) >= current_date - 30
       and max(rcdate) >= current_date - 90
) g
group by first_seen_day
order by first_seen_day;

\echo
\echo '=== Interpretation ==='
\echo 'If Q2 shows multi-day gaps (5-10d) are a sizeable share, and Q1/Q3 show'
\echo 'weeks/days with 0 new campaigns are routine, then the 7-day gap is WITHIN'
\echo 'NHTSA cadence, not a pipeline fault. Confirm nothing is stuck with'
\echo 'verify_new_campaigns_reach_mart.sql (no bronze campno missing downstream)'
\echo 'and mart_freshness_by_source.sql (gold actually rebuilding daily).'
