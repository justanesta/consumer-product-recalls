-- Phase 5c — assert that the 11-tuple identity is stable *across runs*
-- in NHTSA bronze.
--
-- Context: ADR 0030 (amended) commits to an 11-tuple identity for
-- NHTSA bronze dedup. `verify_eleven_tuple_row_unique.sql` checks
-- *within-corpus* uniqueness (no two bronze rows share an 11-tuple).
-- This script checks the complementary property: *across runs*, no
-- physical recall component should ever appear under two different
-- 11-tuples.
--
-- Why it matters (per documentation/nhtsa/incremental_delta_findings.md
-- Section D): if NHTSA fixes a typo in `mfgname`, evolves a `compname`
-- taxonomy, or otherwise edits any of the 11 identity fields for an
-- existing recall, the corrected row will land as a "net-new" 11-tuple
-- rather than as an amendment. Bronze grows; the prior version sits
-- unreachable from silver's `(11-tuple → max(extraction_timestamp))`
-- "latest version" lookup; downstream double-counting follows.
--
-- Strategy: for each of the 10 non-campno identity fields, GROUP BY
-- the OTHER 9 + campno and HAVING the dropped field has >1 distinct
-- value (treating NULL as a value) AND the rows in the group come
-- from >1 distinct `raw_landing_path` (i.e., were loaded by different
-- runs). The cross-run filter is essential — see Section G of
-- `documentation/nhtsa/incremental_delta_findings.md`. NHTSA's bronze
-- has natural one-to-many multiplicities WITHIN a single run (one
-- recall component → many part numbers, one part → many lots, etc.);
-- without the cross-run filter this assertion surfaces 5,176 of those
-- structural groups and obscures the small number of true edits.
--
-- Replaces an earlier self-join formulation that was O(rows_per_campno²)
-- summed over campnos. Large tire-style campnos with thousands of rows
-- (each row sharing campno + many common fields, differing on just a
-- few) made the pair-cardinality catastrophic. The GROUP BY formulation
-- is single-pass O(n) per field, ten passes total, each independent —
-- well under a second per query at 72k bronze rows.
--
-- NULL semantics: the 9 GROUP BY fields rely on Postgres's NULL-as-
-- group-key behavior (NULLs collide into one group, matching
-- `is not distinct from`). The dropped field's cardinality test is
-- written to count NULL→value transitions:
--   count(distinct X) > 1                         -- ≥2 non-null distinct
--   OR (count(*) > count(X) AND count(X) > 0)     -- mix of null & non-null
-- The cross-run filter is `count(distinct raw_landing_path) > 1`.
--
-- Expected outcome on a clean corpus: drift_group_count = 0 across all
-- 10 fields. Non-zero results mean either:
--   (a) NHTSA edited an identity field — investigate, decide whether
--       to amend ADR 0030's identity choice (e.g., demote the drifty
--       field) or document the case as a known exception, OR
--   (b) the 11-tuple was never quite right — re-open the Phase 5c
--       investigation that landed at the 11-tuple.
--
-- Wire-up plans (see project_scope/implementation_plan.md Phase 7):
--   * Near-term: run manually after each `recalls extract nhtsa` and
--     after every weekly `deep-rescan-nhtsa.yml`.
--   * Later: graduate the assertion + this script's siblings to a
--     dedicated DQ framework (Soda Core or Great Expectations).

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-field cross-run drift-group counts ==='
\echo 'Structural invariant: drift_group_count = 0 on the NATURAL-KEY CORE'
\echo '  (compname, maketxt, modeltxt, yeartxt, rcl_cmpt_id, mfr_comp_name).'
\echo 'Non-zero on the secondary descriptors (mfr_comp_ptno, mfr_comp_desc,'
\echo 'bgman, endman) is EXPECTED in steady-state — accumulated structural'
\echo 'multi-batch (supplier supersession) + the field-population / boundary-edit'
\echo 'real_drift classes documented in incremental_delta_findings.md Sections'
\echo 'H/I/K/L/M. Decompose with `decompose_eleven_tuple_drift.sql` to split'
\echo 'structural_multi_batch (silver-correct) from real_drift (silver-fragmenting).'
\echo
\echo 'TOTAL aggregates both classes across all 10 non-campno fields; treat the'
\echo 'natural-key-core rows as the strict invariant and the secondary-descriptor'
\echo 'rows as a watch list rather than an alarm. (See M.4, 2026-05-25, for the'
\echo 'framing-refinement context.)'

with per_field as (
    select 'maketxt' as drifting_field, count(*) as drift_group_count
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
        having (count(distinct maketxt) > 1
                or (count(*) > count(maketxt) and count(maketxt) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'modeltxt', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
        having (count(distinct modeltxt) > 1
                or (count(*) > count(modeltxt) and count(modeltxt) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'yeartxt', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
        having (count(distinct yeartxt) > 1
                or (count(*) > count(yeartxt) and count(yeartxt) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'compname', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
        having (count(distinct compname) > 1
                or (count(*) > count(compname) and count(compname) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'rcl_cmpt_id', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
        having (count(distinct rcl_cmpt_id) > 1
                or (count(*) > count(rcl_cmpt_id) and count(rcl_cmpt_id) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'mfr_comp_ptno', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_desc, mfr_comp_name, endman, bgman
        having (count(distinct mfr_comp_ptno) > 1
                or (count(*) > count(mfr_comp_ptno) and count(mfr_comp_ptno) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'mfr_comp_desc', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_name, endman, bgman
        having (count(distinct mfr_comp_desc) > 1
                or (count(*) > count(mfr_comp_desc) and count(mfr_comp_desc) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'mfr_comp_name', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, endman, bgman
        having (count(distinct mfr_comp_name) > 1
                or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'endman', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman
        having (count(distinct endman) > 1
                or (count(*) > count(endman) and count(endman) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'bgman', count(*)
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman
        having (count(distinct bgman) > 1
                or (count(*) > count(bgman) and count(bgman) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
),
labeled as (
    select drifting_field, drift_group_count, 0 as sort_section
    from per_field
    union all
    select 'TOTAL', sum(drift_group_count), 1
    from per_field
)
select drifting_field, drift_group_count
from labeled
order by sort_section, drift_group_count desc, drifting_field;

\echo
\echo '=== Q2: sample cross-run drift groups per field (up to 5 per field) ==='
\echo 'For each field with non-zero drift, shows the shared 10 fields, the'
\echo 'distinct values observed for the drifting field, and how many runs'
\echo 'contributed rows. n_landing_paths > 1 is the cross-run signal.'

\echo
\echo '--- maketxt drift samples ---'
select campno, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       string_agg(distinct case when maketxt is null then '<NULL>' else maketxt end, ' | ') as distinct_maketxt,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct maketxt) > 1
        or (count(*) > count(maketxt) and count(maketxt) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- modeltxt drift samples ---'
select campno, maketxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       string_agg(distinct case when modeltxt is null then '<NULL>' else modeltxt end, ' | ') as distinct_modeltxt,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct modeltxt) > 1
        or (count(*) > count(modeltxt) and count(modeltxt) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- yeartxt drift samples ---'
select campno, maketxt, modeltxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       string_agg(distinct case when yeartxt is null then '<NULL>' else yeartxt end, ' | ') as distinct_yeartxt,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct yeartxt) > 1
        or (count(*) > count(yeartxt) and count(yeartxt) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- compname drift samples ---'
select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       string_agg(distinct case when compname is null then '<NULL>' else compname end, ' | ') as distinct_compname,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct compname) > 1
        or (count(*) > count(compname) and count(compname) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- rcl_cmpt_id drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       string_agg(distinct case when rcl_cmpt_id is null then '<NULL>' else rcl_cmpt_id end, ' | ') as distinct_rcl_cmpt_id,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct rcl_cmpt_id) > 1
        or (count(*) > count(rcl_cmpt_id) and count(rcl_cmpt_id) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- mfr_comp_ptno drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       string_agg(distinct case when mfr_comp_ptno is null then '<NULL>' else mfr_comp_ptno end, ' | ') as distinct_mfr_comp_ptno,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_desc, mfr_comp_name, endman, bgman
having (count(distinct mfr_comp_ptno) > 1
        or (count(*) > count(mfr_comp_ptno) and count(mfr_comp_ptno) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- mfr_comp_desc drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_name, endman, bgman,
       string_agg(distinct case when mfr_comp_desc is null then '<NULL>' else mfr_comp_desc end, ' | ') as distinct_mfr_comp_desc,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_name, endman, bgman
having (count(distinct mfr_comp_desc) > 1
        or (count(*) > count(mfr_comp_desc) and count(mfr_comp_desc) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- mfr_comp_name drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, endman, bgman,
       string_agg(distinct case when mfr_comp_name is null then '<NULL>' else mfr_comp_name end, ' | ') as distinct_mfr_comp_name,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, endman, bgman
having (count(distinct mfr_comp_name) > 1
        or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- endman drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, bgman,
       string_agg(distinct case when endman is null then '<NULL>' else endman::text end, ' | ') as distinct_endman,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman
having (count(distinct endman) > 1
        or (count(*) > count(endman) and count(endman) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- bgman drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name, endman,
       string_agg(distinct case when bgman is null then '<NULL>' else bgman::text end, ' | ') as distinct_bgman,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman
having (count(distinct bgman) > 1
        or (count(*) > count(bgman) and count(bgman) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;
