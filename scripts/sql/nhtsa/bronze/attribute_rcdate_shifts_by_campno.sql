-- Decompose rcdate amendments in a single NHTSA run by campno.
--
-- Why this exists: Section M.2 of `documentation/nhtsa/incremental_delta_findings.md`
-- describes the 2026-05-25 wave (run_id 217e753d-68f0-42cb-b285-5affafc6035d) where
-- `explore_incremental_delta.sql` Q3 reported 189 of 363 amendments touching `rcdate`
-- — a step-change from Section H.6's 2026-05-12 observation (12 of 235 = 6%) and
-- Sections I/J/K/L where rcdate did not appear as a driver field at all. The
-- inferred mechanism is "NHTSA archive-republish event (e.g., RCL_Annual_Rpts.txt
-- regeneration) rather than per-recall editorial correction." This script grounds
-- the inferential reading empirically.
--
-- Mechanism: mirrors `explore_incremental_delta.sql` Q3 — for each amendment row
-- in the target run, joins to the latest prior bronze row on the 11-tuple
-- identity, then projects (campno, old_rcdate, new_rcdate). Aggregates over
-- amendments where rcdate actually shifted.
--
-- Diagnostic interpretation:
--
--   ┌──────────────────────────────┬─────────────────────────┬──────────────────────────┐
--   │ Signal                       │ Archive-regen event     │ Per-recall corrections   │
--   ├──────────────────────────────┼─────────────────────────┼──────────────────────────┤
--   │ Q2 distinct_campnos_with_   │ Small (≤ ~50)           │ Large (≥ ~100)           │
--   │   shift                      │                         │                          │
--   │ Q1 n_distinct_pairs per     │ 1 per campno (uniform   │ Variable (one per amend) │
--   │   campno                     │   within a recall)      │                          │
--   │ Q3 shift direction          │ Mostly forward, modest  │ Mixed forward/backward,  │
--   │                              │   day-deltas            │   larger spread          │
--   │ Q3 NULL transitions          │ ~zero (regen preserves  │ Possible (correcting     │
--   │                              │   prior values)         │   missing rcdates)       │
--   └──────────────────────────────┴─────────────────────────┴──────────────────────────┘
--
-- Reconciliation context: rcdate is a payload attribute, not in any candidate
-- silver identity tuple (11-tuple per ADR 0030/0031, 6-tuple per ADR 0033). The
-- amendment cost is paid in bronze write volume, not silver fragmentation. This
-- script's purpose is to characterize the *mechanism*, not to inform any silver
-- design decision.
--
-- Usage:
--   psql ... -v run_id=<uuid> -f scripts/sql/nhtsa/bronze/attribute_rcdate_shifts_by_campno.sql
--   psql ... -f scripts/sql/nhtsa/bronze/attribute_rcdate_shifts_by_campno.sql
--       -> defaults to most recent successful NHTSA run

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Resolve run_id (mirrors explore_incremental_delta.sql convention).
\if :{?run_id}
\echo
\echo === Using passed run_id: :run_id ===
\else
select run_id as run_id
from extraction_runs
where source = 'nhtsa' and status = 'success'
order by started_at desc
limit 1
\gset
\echo
\echo === Defaulted to most recent successful nhtsa run: :run_id ===
\endif

select raw_landing_path as run_landing_path
from extraction_runs
where source = 'nhtsa' and run_id = :'run_id'
\gset

\echo Landing path: :run_landing_path
\echo

\echo '=== Q0: run shape recap ==='
with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
)
select
    (select count(*) from run_rows)                                            as rows_in_run,
    (select count(*) from run_rows r
      where exists (
          select 1 from nhtsa_recalls_bronze prior
          where prior.raw_landing_path <> r.raw_landing_path
            and prior.campno        is not distinct from r.campno
            and prior.maketxt       is not distinct from r.maketxt
            and prior.modeltxt      is not distinct from r.modeltxt
            and prior.yeartxt       is not distinct from r.yeartxt
            and prior.compname      is not distinct from r.compname
            and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
            and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
            and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
            and prior.mfr_comp_name is not distinct from r.mfr_comp_name
            and prior.endman        is not distinct from r.endman
            and prior.bgman         is not distinct from r.bgman
      )
    )                                                                          as amendments_in_run;

\echo
\echo '=== Q1: per-campno breakdown of rcdate shifts ==='
\echo Each row = one campno that had >=1 amendment whose new rcdate differs from
\echo the most-recent prior bronze row sharing the 11-tuple. n_distinct_pairs = 1
\echo means the campno had a uniform shift (single editorial action); > 1 means
\echo mixed shifts within the campno.

with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        r.campno,
        r.maketxt,
        prior.rcdate as old_rcdate,
        r.rcdate     as new_rcdate
    from run_rows r
    join nhtsa_recalls_bronze prior
      on prior.raw_landing_path <> r.raw_landing_path
     and prior.campno        is not distinct from r.campno
     and prior.maketxt       is not distinct from r.maketxt
     and prior.modeltxt      is not distinct from r.modeltxt
     and prior.yeartxt       is not distinct from r.yeartxt
     and prior.compname      is not distinct from r.compname
     and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
     and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
     and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
     and prior.mfr_comp_name is not distinct from r.mfr_comp_name
     and prior.endman        is not distinct from r.endman
     and prior.bgman         is not distinct from r.bgman
    order by r.id, prior.extraction_timestamp desc
)
select
    campno,
    string_agg(distinct maketxt, ', ' order by maketxt)                                       as makes,
    count(*)                                                                                  as n_rows_shifted,
    count(distinct (old_rcdate, new_rcdate))                                                  as n_distinct_pairs,
    min(old_rcdate)                                                                           as oldest_old_rcdate,
    max(new_rcdate)                                                                           as newest_new_rcdate,
    string_agg(
        distinct (coalesce(old_rcdate::text, '<NULL>') || ' -> ' || coalesce(new_rcdate::text, '<NULL>')),
        ' | ' order by (coalesce(old_rcdate::text, '<NULL>') || ' -> ' || coalesce(new_rcdate::text, '<NULL>'))
    )                                                                                         as shift_pairs
from paired
where old_rcdate is distinct from new_rcdate
group by campno
order by n_rows_shifted desc, campno;

\echo
\echo '=== Q2: aggregate signature ==='
\echo Use this to read the wave at a glance:
\echo   small distinct_campnos_with_shift + many rows_with_rcdate_shift = archive-regen
\echo   large distinct_campnos_with_shift + ~1:1 with rows_with_rcdate_shift = corrections

with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        r.campno,
        prior.rcdate as old_rcdate,
        r.rcdate     as new_rcdate
    from run_rows r
    join nhtsa_recalls_bronze prior
      on prior.raw_landing_path <> r.raw_landing_path
     and prior.campno        is not distinct from r.campno
     and prior.maketxt       is not distinct from r.maketxt
     and prior.modeltxt      is not distinct from r.modeltxt
     and prior.yeartxt       is not distinct from r.yeartxt
     and prior.compname      is not distinct from r.compname
     and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
     and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
     and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
     and prior.mfr_comp_name is not distinct from r.mfr_comp_name
     and prior.endman        is not distinct from r.endman
     and prior.bgman         is not distinct from r.bgman
    order by r.id, prior.extraction_timestamp desc
)
select
    count(*) filter (where old_rcdate is distinct from new_rcdate)                                 as rows_with_rcdate_shift,
    count(distinct campno) filter (where old_rcdate is distinct from new_rcdate)                   as distinct_campnos_with_shift,
    count(distinct (old_rcdate, new_rcdate)) filter (where old_rcdate is distinct from new_rcdate) as distinct_shift_pairs,
    -- CASE WHEN inside avg() avoids the round(avg(...)::numeric, N) filter (...) parse
    -- ambiguity — FILTER can only attach to the aggregate directly, not through a cast.
    round(avg(
        case when old_rcdate is distinct from new_rcdate
                  and old_rcdate is not null
                  and new_rcdate is not null
             then extract(year from new_rcdate) - extract(year from old_rcdate)
        end
    )::numeric, 2)                                                                                  as avg_year_delta
from paired;

\echo
\echo '=== Q3: shift-direction distribution ==='
\echo Whether shifts trend forward (correcting to a more-recent date),
\echo backward (historical re-dating), or move from/to NULL.

with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        prior.rcdate as old_rcdate,
        r.rcdate     as new_rcdate
    from run_rows r
    join nhtsa_recalls_bronze prior
      on prior.raw_landing_path <> r.raw_landing_path
     and prior.campno        is not distinct from r.campno
     and prior.maketxt       is not distinct from r.maketxt
     and prior.modeltxt      is not distinct from r.modeltxt
     and prior.yeartxt       is not distinct from r.yeartxt
     and prior.compname      is not distinct from r.compname
     and prior.rcl_cmpt_id   is not distinct from r.rcl_cmpt_id
     and prior.mfr_comp_ptno is not distinct from r.mfr_comp_ptno
     and prior.mfr_comp_desc is not distinct from r.mfr_comp_desc
     and prior.mfr_comp_name is not distinct from r.mfr_comp_name
     and prior.endman        is not distinct from r.endman
     and prior.bgman         is not distinct from r.bgman
    order by r.id, prior.extraction_timestamp desc
)
select
    case
        when old_rcdate is null and new_rcdate is not null then 'NULL -> populated'
        when old_rcdate is not null and new_rcdate is null then 'populated -> NULL'
        when new_rcdate > old_rcdate                       then 'forward shift'
        when new_rcdate < old_rcdate                       then 'backward shift'
    end                                                                  as direction,
    count(*)                                                             as n_rows,
    -- rcdate is timestamp-with-time-zone; (ts - ts) yields INTERVAL, which
    -- doesn't cast to numeric for round(). Cast both sides to date first so
    -- subtraction yields an integer day count.
    min(new_rcdate::date - old_rcdate::date)                             as min_day_delta,
    max(new_rcdate::date - old_rcdate::date)                             as max_day_delta,
    -- Same CASE-WHEN-inside-avg pattern as Q2 to avoid the FILTER-after-cast parse issue.
    round(avg(
        case when old_rcdate is not null and new_rcdate is not null
             then (new_rcdate::date - old_rcdate::date)
        end
    )::numeric, 1)                                                       as avg_day_delta
from paired
where old_rcdate is distinct from new_rcdate
group by 1
order by n_rows desc;
