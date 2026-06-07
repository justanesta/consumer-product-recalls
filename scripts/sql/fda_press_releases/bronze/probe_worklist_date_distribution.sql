-- Probe A — distribution of the PR work-list (distinct recall_event_id) across the
-- date fields we ALREADY capture in fda_recalls_bronze. Goal: decide whether any
-- captured date plausibly BOUNDS the ~50,509-event press-release sweep, and to roughly
-- what size, before committing to a 17h full sweep.
--
-- WHY THIS MATTERS (read before trusting any floor):
--   The press-release endpoint is per-event and the work-list is `select distinct
--   recall_event_id from fda_recalls_bronze` (src/extractors/fda_press_release.py:303-309).
--   recall_event_id ascending is a POOR recency proxy (ids 0..~76000 yielded ZERO PRs in
--   the seed; the first PR was at event 76385). We want a real date floor instead.
--
--   But each candidate date has a known defect — mark these as the source's documented
--   semantics, not inferences:
--     * event_lmd   — advances ON EDITS ONLY; un-edited records are NULL (Finding H,
--                     api_observations.md:536-540; ~197 null in full corpus per
--                     fda_null_eventlmd memo). It ALSO drifts forward when FDA re-touches
--                     an archived record (Finding M, recall_event.sql:119-120). So a floor
--                     on event_lmd both MISSES old-unedited events AND wrongly INCLUDES
--                     old events that were recently re-touched. Bad recency proxy.
--     * posted_internet_dt — NULL for recalls posted before 2022-10-25 (FDA Definitions
--                     PDF; field_audit_2026_w22.md:231). ~84% populated in a 2026 window.
--                     A `>= 2022-10-25` floor here EXCLUDES every pre-2022 recall — but the
--                     seed's one captured PR is a LEGACY CVM url (ucm542265.htm), proving
--                     pre-2022 recalls DO carry press releases. So this floor has a known
--                     recall hole exactly where legacy PRs live.
--     * recall_initiation_dt — the real-world announcement date; most populated, no
--                     edit-drift. Best recency proxy of the three, but has ~14 dropped-
--                     century typos (recall_event.sql:100-111) and some NULLs.
--
-- This probe quantifies all three so the bounding decision is data-driven, not assumed.
--
-- Usage (user runs):
--   set -a && . .env && set +a
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/fda_press_releases/bronze/probe_worklist_date_distribution.sql
--
-- All counts are over DISTINCT recall_event_id (the work-list grain), NOT product rows.
-- An event can span many product rows with differing dates, so each "per event" date is
-- collapsed to the event's MAX (most recent) value first — that is the value most relevant
-- to a recency floor and avoids double-counting an event across year buckets.

\set ON_ERROR_STOP on

-- Event-grain rollup: one row per distinct recall_event_id with the event's max of each
-- captured date. Reused by every query below.
\echo '=== Building event-grain date rollup (CTE reused below) ==='

-- Q1 — work-list size sanity: distinct events, and how many have ANY non-null value of
--      each date field. This is the denominator for everything else and re-confirms the
--      50,509 work_list_size from the seed logs.
\echo '=== Q1: work-list size + per-field non-null coverage (event grain) ==='
with ev as (
    select
        recall_event_id,
        max(event_lmd)             as event_lmd_max,
        max(posted_internet_dt)    as posted_internet_dt_max,
        max(recall_initiation_dt)  as recall_initiation_dt_max
    from fda_recalls_bronze
    where recall_event_id is not null
    group by recall_event_id
)
select
    count(*)                                                    as distinct_events,
    count(*) filter (where event_lmd_max is not null)           as has_event_lmd,
    count(*) filter (where posted_internet_dt_max is not null)  as has_posted_internet_dt,
    count(*) filter (where recall_initiation_dt_max is not null) as has_recall_initiation_dt,
    count(*) filter (where event_lmd_max is null
                       and posted_internet_dt_max is null
                       and recall_initiation_dt_max is null)    as has_no_usable_date
from ev;

-- Q2 — distribution of distinct events by event_lmd YEAR (NULL bucket explicit).
--      Watch for: a large NULL bucket (un-edited events a floor would drop) and a fat
--      recent tail (re-touched archives a floor would wrongly include).
\echo '=== Q2: distinct events by event_lmd year (NULL = un-edited, Finding H) ==='
with ev as (
    select recall_event_id, max(event_lmd) as d
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
)
select
    case when d is null then 'NULL (un-edited)' else extract(year from d)::text end as event_lmd_year,
    count(*) as events
from ev
group by 1
order by 1;

-- Q3 — distribution of distinct events by posted_internet_dt YEAR (NULL bucket explicit).
--      The NULL bucket ~ the pre-2022-10-25 cohort PLUS any non-backfilled rows; these are
--      precisely the events a `posted_internet_dt >= 2022-10-25` floor would EXCLUDE, and
--      where the seed's lone legacy PR (ucm542265.htm) lives. Size = the recall risk.
\echo '=== Q3: distinct events by posted_internet_dt year (NULL ~ pre-2022-10-25 cohort) ==='
with ev as (
    select recall_event_id, max(posted_internet_dt) as d
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
)
select
    case when d is null then 'NULL (pre-2022-10-25 or unposted)' else extract(year from d)::text end as posted_year,
    count(*) as events
from ev
group by 1
order by 1;

-- Q4 — distribution of distinct events by recall_initiation_dt YEAR (NULL + the
--      dropped-century typos bucketed separately). recall_initiation_dt is the best
--      recency proxy: real announcement date, no edit-drift. The <1940 bucket is the
--      ~14 known typos (recall_event.sql:100-111) — they carry a garbage year, so a
--      naive year floor must treat them as "unknown", not exclude them.
\echo '=== Q4: distinct events by recall_initiation_dt year (best recency proxy) ==='
with ev as (
    select recall_event_id, max(recall_initiation_dt) as d
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
)
select
    case
        when d is null then 'NULL'
        when d < '1940-01-01' then '<1940 (dropped-century typo)'
        else extract(year from d)::text
    end as initiation_year,
    count(*) as events
from ev
group by 1
order by 1;

-- Q5 — the recall_event_id-range / date cross-tab that explains WHY id-order failed.
--      Buckets the work-list by the same id ranges the seed swept and shows the max
--      recall_initiation_dt + posted_internet_dt coverage per range. If high ids do NOT
--      monotonically map to recent dates, that is the direct proof id-order is a bad proxy
--      (and tells us where the PR-bearing events actually sit by date).
\echo '=== Q5: id-range x date cross-tab (why ascending id-order mis-ranks recency) ==='
with ev as (
    select
        recall_event_id,
        max(recall_initiation_dt) as init_max,
        max(posted_internet_dt)   as posted_max
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
)
select
    width_bucket(recall_event_id, 0, 110000, 11) as id_bucket_10k,
    min(recall_event_id) as min_id,
    max(recall_event_id) as max_id,
    count(*)             as events,
    min(init_max)        as earliest_initiation,
    max(init_max)        as latest_initiation,
    count(*) filter (where posted_max is not null) as has_posted_internet_dt
from ev
group by 1
order by 1;
