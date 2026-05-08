-- Phase 5c — empirically test whether the 9-tuple is stable across runs,
-- as a candidate for the NHTSA silver canonical-entity key.
--
-- Context: ADR 0030 fixed the bronze 11-tuple identity for row-grain
-- dedup. `assert_eleven_tuple_identity_stable.sql` showed the 11-tuple
-- has a small but non-zero cross-run drift hazard, concentrated in
-- `endman`/`bgman` (1 case in 2 runs as of 2026-05-08 — see
-- `documentation/nhtsa/incremental_delta_findings.md` Section G).
--
-- Hypothesis: dropping `endman` and `bgman` from the identity yields a
-- 9-tuple that IS stable across runs:
--   (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
--    mfr_comp_ptno, mfr_comp_desc, mfr_comp_name)
--
-- If true: the 9-tuple becomes the silver `source_recall_id` (as a
-- deterministic hash) per the layered design discussed in Section G's
-- "Silver implication" — bronze keeps 11-tuple row grain (audit-quality,
-- preserves within-run multi-batch rows), silver derives a stable
-- surrogate from the 9-tuple, and `endman`/`bgman` either get
-- normalized into a child `recall_component_batch` table or rolled up
-- (latest-wins Type 1 SCD).
--
-- If false: the 9-tuple isn't stable either, and silver needs a
-- richer mechanism (a mapping table with manual / fuzzy reconciliation,
-- or further demotion of fields from the identity).
--
-- Strategy: same shape as `assert_eleven_tuple_identity_stable.sql`
-- but checks 8 non-campno fields instead of 10. For each field X in
-- the 9-tuple, GROUP BY (campno + the other 7 9-tuple fields) and
-- HAVING that group has >1 distinct value of X AND comes from >1
-- distinct `raw_landing_path` (cross-run filter — see Section G).
--
-- Note: this script does NOT include endman/bgman in the GROUP BY.
-- That's the whole point — at the 9-tuple grain, within-run multi-
-- batch rows (e.g., the Heil garbage truck case from Section G with
-- two endman values for the same part) collapse into one group, and
-- those endman/bgman values are summed/aggregated away. We're asking
-- whether the parent 9-tuple identity itself is stable, independent
-- of batch-level fields.
--
-- Expected outcome if hypothesis holds: TOTAL = 0. Any non-zero result
-- is a 9-tuple drift event — which would invalidate the proposed
-- silver surrogate-key design and warrant a richer reconciliation path.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-field cross-run drift-group counts (9-tuple grain) ==='
\echo 'Headline assertion: TOTAL = 0 means the 9-tuple identity is stable across runs.'
\echo 'If TOTAL = 0, the 9-tuple is a viable basis for silver canonical-entity surrogate.'

with per_field as (
    select 'maketxt' as drifting_field, count(*) as drift_group_count
    from (
        select 1
        from nhtsa_recalls_bronze
        group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
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
                 mfr_comp_desc, mfr_comp_name
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
                 mfr_comp_ptno, mfr_comp_name
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
                 mfr_comp_ptno, mfr_comp_desc
        having (count(distinct mfr_comp_name) > 1
                or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
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
\echo '=== Q2: sample 9-tuple cross-run drift groups per field (up to 5 per field) ==='
\echo 'For each field with non-zero drift, shows the shared 8 fields, the distinct'
\echo 'values observed for the drifting field, and how many runs contributed rows.'
\echo 'Eyeball any non-zero result — these would invalidate the 9-tuple as a stable'
\echo 'silver surrogate-key basis.'

\echo
\echo '--- maketxt drift samples ---'
select campno, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name,
       string_agg(distinct case when maketxt is null then '<NULL>' else maketxt end, ' | ') as distinct_maketxt,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having (count(distinct maketxt) > 1
        or (count(*) > count(maketxt) and count(maketxt) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- modeltxt drift samples ---'
select campno, maketxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name,
       string_agg(distinct case when modeltxt is null then '<NULL>' else modeltxt end, ' | ') as distinct_modeltxt,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having (count(distinct modeltxt) > 1
        or (count(*) > count(modeltxt) and count(modeltxt) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- yeartxt drift samples ---'
select campno, maketxt, modeltxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name,
       string_agg(distinct case when yeartxt is null then '<NULL>' else yeartxt end, ' | ') as distinct_yeartxt,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having (count(distinct yeartxt) > 1
        or (count(*) > count(yeartxt) and count(yeartxt) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- compname drift samples ---'
select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name,
       string_agg(distinct case when compname is null then '<NULL>' else compname end, ' | ') as distinct_compname,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having (count(distinct compname) > 1
        or (count(*) > count(compname) and count(compname) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- rcl_cmpt_id drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, mfr_comp_ptno,
       mfr_comp_desc, mfr_comp_name,
       string_agg(distinct case when rcl_cmpt_id is null then '<NULL>' else rcl_cmpt_id end, ' | ') as distinct_rcl_cmpt_id,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having (count(distinct rcl_cmpt_id) > 1
        or (count(*) > count(rcl_cmpt_id) and count(rcl_cmpt_id) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- mfr_comp_ptno drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_desc, mfr_comp_name,
       string_agg(distinct case when mfr_comp_ptno is null then '<NULL>' else mfr_comp_ptno end, ' | ') as distinct_mfr_comp_ptno,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_desc, mfr_comp_name
having (count(distinct mfr_comp_ptno) > 1
        or (count(*) > count(mfr_comp_ptno) and count(mfr_comp_ptno) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- mfr_comp_desc drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_name,
       string_agg(distinct case when mfr_comp_desc is null then '<NULL>' else mfr_comp_desc end, ' | ') as distinct_mfr_comp_desc,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_name
having (count(distinct mfr_comp_desc) > 1
        or (count(*) > count(mfr_comp_desc) and count(mfr_comp_desc) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '--- mfr_comp_name drift samples ---'
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id, mfr_comp_ptno,
       mfr_comp_desc,
       string_agg(distinct case when mfr_comp_name is null then '<NULL>' else mfr_comp_name end, ' | ') as distinct_mfr_comp_name,
       count(distinct raw_landing_path) as n_landing_paths,
       count(*) as n_rows
from nhtsa_recalls_bronze
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc
having (count(distinct mfr_comp_name) > 1
        or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;
