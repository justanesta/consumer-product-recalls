-- Probe B — floor sizing + recall-hole estimation. Turns the distributions from
-- probe_worklist_date_distribution.sql into concrete WORK-LIST SIZES for each candidate
-- date floor, plus the count of events a floor would MISS (the recall risk), so the user
-- can pick a floor with eyes open on what it drops.
--
-- Decision frame: the full sweep is ~50,509 events @ ~1.21 s/event ≈ 17 h, pacing-bound.
-- Every event we can safely drop from the work-list removes ~1.21 s of wall-clock and one
-- API call. A floor is only worth it if (a) it shrinks the list materially AND (b) the
-- events it drops are very unlikely to carry a press release. (b) is the hard part: PRs
-- are sparse AND scattered (legacy CVM url proves old recalls have PRs too), so any floor
-- on a "recency" field has a recall hole. This probe quantifies BOTH sides.
--
-- The 2022-10-25 floor is the FDA Definitions-PDF cutoff for posted_internet_dt
-- (field_audit_2026_w22.md:231) — NOT an event_lmd cutoff. It is the natural candidate
-- floor BUT its blind spot is exactly the legacy-PR cohort. recall_initiation_dt has no
-- such semantic cliff, so we size floors on it too, at several candidate years.
--
-- Usage (user runs):
--   set -a && . .env && set +a
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/fda_press_releases/bronze/probe_worklist_floor_sizing.sql

\set ON_ERROR_STOP on

-- Q1 — the headline ask: distinct events with event_lmd >= 2022-10-25 vs total.
--      (Answers the brief's part (b) verbatim. Note event_lmd is the EDIT-drift field,
--      so this overcounts recents that are merely re-touched and undercounts via NULLs;
--      treat it as an upper-ish bound on "recently active by edit", not a recency floor.)
\echo '=== Q1: events with event_lmd >= 2022-10-25 vs total (brief part b) ==='
with ev as (
    select recall_event_id, max(event_lmd) as d
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
)
select
    count(*)                                            as total_events,
    count(*) filter (where d >= '2022-10-25')           as event_lmd_ge_2022_10_25,
    count(*) filter (where d <  '2022-10-25')           as event_lmd_lt_2022_10_25,
    count(*) filter (where d is null)                   as event_lmd_null,
    round(100.0 * count(*) filter (where d >= '2022-10-25') / nullif(count(*), 0), 1)
                                                        as pct_kept_by_floor
from ev;

-- Q2 — candidate floors side-by-side: for each (field, floor) pair, how big is the kept
--      work-list, and how many events does the floor DROP (split into "dropped with a
--      KNOWN older date" vs "dropped because the field is NULL"). The NULL-drop column is
--      the dangerous one: NULL != old. An event with NULL posted_internet_dt may be a
--      pre-2022 legacy recall WITH a press release (the seed's only PR is exactly this).
\echo '=== Q2: candidate floors — kept size + dropped (known-old vs NULL) ==='
with ev as (
    select
        recall_event_id,
        max(event_lmd)            as lmd,
        max(posted_internet_dt)   as posted,
        max(recall_initiation_dt) as init
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
),
floors(field, floor_dt) as (
    values
        ('posted_internet_dt', date '2022-10-25'),
        ('recall_initiation_dt', date '2022-01-01'),
        ('recall_initiation_dt', date '2020-01-01'),
        ('recall_initiation_dt', date '2015-01-01'),
        ('recall_initiation_dt', date '2012-01-01'),
        ('event_lmd', date '2022-10-25')
)
select
    f.field,
    f.floor_dt,
    count(*) filter (
        where (f.field = 'posted_internet_dt'   and e.posted >= f.floor_dt)
           or (f.field = 'recall_initiation_dt' and e.init   >= f.floor_dt)
           or (f.field = 'event_lmd'            and e.lmd    >= f.floor_dt)
    ) as kept_events,
    count(*) filter (
        where (f.field = 'posted_internet_dt'   and e.posted is not null and e.posted < f.floor_dt)
           or (f.field = 'recall_initiation_dt' and e.init   is not null and e.init   < f.floor_dt)
           or (f.field = 'event_lmd'            and e.lmd    is not null and e.lmd    < f.floor_dt)
    ) as dropped_known_old,
    count(*) filter (
        where (f.field = 'posted_internet_dt'   and e.posted is null)
           or (f.field = 'recall_initiation_dt' and e.init   is null)
           or (f.field = 'event_lmd'            and e.lmd    is null)
    ) as dropped_null_field
from floors f cross join ev e
group by f.field, f.floor_dt
order by f.field, f.floor_dt;

-- Q3 — a COMPOSITE "keep if recent by ANY captured date OR date-unknown" floor. This is
--      the precision/recall-balanced candidate: keep an event when its recall_initiation_dt
--      OR posted_internet_dt OR event_lmd is >= the floor, OR when ALL three are NULL
--      (date-unknown events are kept, not dropped — that is where legacy PRs hide). Shows
--      how much smaller than the full 50,509 this still gets us, vs a single-field floor.
\echo '=== Q3: composite OR floor (keep if any date recent OR all-NULL) at 2022-01-01 ==='
with ev as (
    select
        recall_event_id,
        max(event_lmd)            as lmd,
        max(posted_internet_dt)   as posted,
        max(recall_initiation_dt) as init
    from fda_recalls_bronze where recall_event_id is not null
    group by recall_event_id
)
select
    count(*) as total_events,
    count(*) filter (
        where coalesce(lmd, '-infinity') >= '2022-01-01'
           or coalesce(posted, '-infinity') >= '2022-01-01'
           or coalesce(init, '-infinity') >= '2022-01-01'
           or (lmd is null and posted is null and init is null)
    ) as kept_recent_or_unknown,
    count(*) filter (
        where coalesce(lmd, '-infinity') < '2022-01-01'
          and coalesce(posted, '-infinity') < '2022-01-01'
          and coalesce(init, '-infinity') < '2022-01-01'
          and not (lmd is null and posted is null and init is null)
    ) as dropped_all_dates_old
from ev;

-- Q4 — the legacy-PR canary. The seed found exactly one PR-bearing event so far: 76385,
--      whose url is a legacy CVM page (pre-CMS-migration). Show that event's captured
--      dates. If posted_internet_dt is NULL and recall_initiation_dt is old, this is the
--      concrete proof that a posted_internet_dt floor (or any recent-only floor) drops a
--      real PR — i.e. the floor's recall hole is not hypothetical.
\echo '=== Q4: legacy-PR canary — captured dates for the one known PR-bearing event ==='
select
    recall_event_id,
    min(recall_initiation_dt) as init_min,
    max(recall_initiation_dt) as init_max,
    max(posted_internet_dt)   as posted_max,
    max(event_lmd)            as event_lmd_max,
    count(*)                  as product_rows
from fda_recalls_bronze
where recall_event_id = 76385
group by recall_event_id;
