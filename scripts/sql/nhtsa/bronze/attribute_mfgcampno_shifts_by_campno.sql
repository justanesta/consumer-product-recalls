-- Decompose mfgcampno amendments in a single NHTSA run by campno.
--
-- Why this exists: Section M.2 of `documentation/nhtsa/incremental_delta_findings.md`
-- noted that the 2026-05-25 wave's Q3 reported `mfgcampno` (the manufacturer's
-- own campaign-number field) as the fifth-most-modified non-`source_recall_id`
-- field — 14 of 363 amendments touched it. That count is structurally
-- independent of the 3 recalls that shifted rcdate (decomposed by
-- `attribute_rcdate_shifts_by_campno.sql`), so the mechanism is unknown.
-- Could be the same per-recall correction pattern on a different subset, or
-- a genuinely unrelated wave. This script answers that empirically.
--
-- Mechanism: mirrors `attribute_rcdate_shifts_by_campno.sql` exactly — same
-- 11-tuple paired join, but projects mfgcampno old/new instead of rcdate.
-- mfgcampno is a string field, so the year-delta and day-delta diagnostics
-- from the rcdate script don't apply; replaced with a length-delta and a
-- count of NULL transitions to characterize whether shifts are typo-class
-- (small length-delta, same approximate string) or total-replacement-class
-- (large length-delta, possibly a NULL transition).
--
-- Diagnostic interpretation:
--
--   ┌─────────────────────────────┬──────────────────────────┬──────────────────────────┐
--   │ Signal                      │ Per-recall corrections   │ Coincidental noise       │
--   │                             │   (same shape as rcdate) │                          │
--   ├─────────────────────────────┼──────────────────────────┼──────────────────────────┤
--   │ Q2 distinct_campnos_with_  │ Small (≤ ~5)             │ Larger spread            │
--   │   shift                     │                          │                          │
--   │ Q1 n_distinct_pairs per    │ 1 per campno (uniform    │ Variable                 │
--   │   campno                    │   within a recall)       │                          │
--   │ Q3 length deltas            │ Small (typo class) or    │ Mixed                    │
--   │                             │   NULL transitions       │                          │
--   └─────────────────────────────┴──────────────────────────┴──────────────────────────┘
--
-- Reconciliation context: like rcdate, `mfgcampno` is a payload attribute,
-- not part of any candidate identity tuple. Amendments are bronze write-
-- volume cost, not silver fragmentation. This script's purpose is to
-- characterize the mechanism for the M.2 closure note.
--
-- Usage:
--   psql ... -v run_id=<uuid> -f scripts/sql/nhtsa/bronze/attribute_mfgcampno_shifts_by_campno.sql
--   psql ... -f scripts/sql/nhtsa/bronze/attribute_mfgcampno_shifts_by_campno.sql
--       -> defaults to most recent successful NHTSA run

\set ON_ERROR_STOP on
\pset null '<NULL>'

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

\echo '=== Q1: per-campno breakdown of mfgcampno shifts ==='

with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        r.campno,
        r.maketxt,
        prior.mfgcampno as old_mfgcampno,
        r.mfgcampno     as new_mfgcampno
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
    string_agg(distinct maketxt, ', ' order by maketxt)                                                as makes,
    count(*)                                                                                           as n_rows_shifted,
    count(distinct (old_mfgcampno, new_mfgcampno))                                                     as n_distinct_pairs,
    string_agg(
        distinct (coalesce(old_mfgcampno, '<NULL>') || ' -> ' || coalesce(new_mfgcampno, '<NULL>')),
        ' | ' order by (coalesce(old_mfgcampno, '<NULL>') || ' -> ' || coalesce(new_mfgcampno, '<NULL>'))
    )                                                                                                  as shift_pairs
from paired
where old_mfgcampno is distinct from new_mfgcampno
group by campno
order by n_rows_shifted desc, campno;

\echo
\echo '=== Q2: aggregate signature ==='

with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        r.campno,
        prior.mfgcampno as old_mfgcampno,
        r.mfgcampno     as new_mfgcampno
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
    count(*) filter (where old_mfgcampno is distinct from new_mfgcampno)                                     as rows_with_shift,
    count(distinct campno) filter (where old_mfgcampno is distinct from new_mfgcampno)                       as distinct_campnos_with_shift,
    count(distinct (old_mfgcampno, new_mfgcampno)) filter (where old_mfgcampno is distinct from new_mfgcampno) as distinct_shift_pairs,
    count(*) filter (where old_mfgcampno is null     and new_mfgcampno is not null)                          as null_to_populated,
    count(*) filter (where old_mfgcampno is not null and new_mfgcampno is null)                              as populated_to_null
from paired;

\echo
\echo '=== Q3: length-delta distribution ==='
\echo Small abs(length_delta) + same-prefix => typo-class correction
\echo Large abs(length_delta) => total replacement (possibly different campaign name)

with run_rows as (
    select *
    from nhtsa_recalls_bronze
    where raw_landing_path = :'run_landing_path'
),
paired as (
    select distinct on (r.id)
        prior.mfgcampno as old_mfgcampno,
        r.mfgcampno     as new_mfgcampno
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
        when old_mfgcampno is null and new_mfgcampno is not null                                  then 'NULL -> populated'
        when old_mfgcampno is not null and new_mfgcampno is null                                  then 'populated -> NULL'
        when length(new_mfgcampno) - length(old_mfgcampno) between -2 and 2                       then 'minor edit (|len_delta| <= 2)'
        when abs(length(new_mfgcampno) - length(old_mfgcampno)) > 2                               then 'major edit (|len_delta| > 2)'
    end                                                                                          as class,
    count(*)                                                                                     as n_rows,
    min(length(new_mfgcampno) - length(old_mfgcampno))                                           as min_len_delta,
    max(length(new_mfgcampno) - length(old_mfgcampno))                                           as max_len_delta
from paired
where old_mfgcampno is distinct from new_mfgcampno
group by 1
order by n_rows desc;
