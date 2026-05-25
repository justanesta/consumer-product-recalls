-- Phase 5c — assert that the 9-tuple identity is stable *across runs* in
-- NHTSA bronze, as a candidate for a future silver canonical-entity key.
--
-- Status: EXPLORATORY / hypothesis check. Production silver currently uses
-- md5(11-tuple) per `dbt/models/silver/recall_product.sql:99-108`, NOT
-- md5(9-tuple). This assertion answers: "if we MIGRATED silver to the
-- 9-tuple per ADR 0031 Section G's 'silver implication' exploration,
-- would cross-run fragmentation be zero or near-zero?" See
-- `decompose_nine_tuple_drift.sql` for the structural-vs-real-drift
-- decomposition view; this assertion is the smoke test (real_drift only).
--
-- The 9-tuple:
--   (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
--    mfr_comp_ptno, mfr_comp_desc, mfr_comp_name)
-- i.e., the 11-tuple with `endman` and `bgman` (batch-window fields)
-- collapsed out. Per ADR 0030/0031, those would migrate to a child
-- `recall_component_batch` table or be rolled up Type-1 in silver.
--
-- Mechanism (refactored 2026-05-13): per-path-value-set divergence check.
-- For each rotated identity field, compute the set of distinct values the
-- field takes *within each raw_landing_path* (string-aggregated, sorted,
-- NULL-coalesced), then group by the other 8 9-tuple fields and flag
-- groups where the per-path value sets are NOT all identical. Mirrors
-- the refactor applied to the dbt test
-- `dbt/tests/source_assumptions/assert_nhtsa_eleven_tuple_identity_stable.sql`
-- on 2026-05-12 (104 → 9), and decomposed in
-- `decompose_eleven_tuple_drift.sql` / `decompose_nine_tuple_drift.sql`.
--
-- What this suppresses: structural multi-batch (silver-correct false
-- positive). One physical recall component legitimately reporting
-- multiple values for a field (e.g., LEAF battery chemistry variants
-- `295B0 5SA1C` + `295B0 5SF0A`, both real, both in every archive). Every
-- path's value set is the same canonical string; the group is not flagged.
-- The prior strict-boolean filter mistook these for drift; this filter
-- does not.
--
-- What this still catches: real 9-tuple drift — NHTSA editing one of the
-- 8 non-campno 9-tuple fields for an existing recall between archive
-- generations. The canonical example is the `'AC DELCO'` → `'ACDELCO'`
-- maketxt normalization (campno 22E002000, observed 2026-05-08 by
-- `scripts/nhtsa/tsv_analysis/cross_corpus_stability.py`). Any such
-- drift fragments silver if/when silver migrates to md5(9-tuple).
--
-- Pre-refactor reference (for comparison): the older strict-boolean
-- filter (now removed) reported `TOTAL = 97` as of 2026-05-13, all in
-- `mfr_comp_ptno`. Post-refactor reports `TOTAL = 0` — all 97 were
-- structural multi-batch (verified via `decompose_nine_tuple_drift.sql`
-- 2026-05-13).
--
-- Divergence from the 11-tuple dbt test (which uses the same mechanism
-- at a different grain): the dbt test groups by all 10 non-campno
-- 11-tuple fields including `endman` + `bgman`. This script drops both
-- — that's the whole point of the 9-tuple hypothesis. So at the 9-tuple
-- grain, within-run multi-batch rows (e.g., the Heil garbage truck case
-- from Section G with two endman values for the same part) collapse
-- into one group, and the endman/bgman drift that fragments md5(11-tuple)
-- silver simply doesn't appear here.
--
-- Expected outcome if hypothesis holds: TOTAL = 0. Any non-zero result is
-- a 9-tuple drift event — would invalidate the proposed silver surrogate-
-- key migration and warrant a richer reconciliation path (mapping table,
-- fuzzy match, further demotion of fields).
--
-- Wire-up plans:
--   * Near-term: run manually alongside the 11-tuple dbt test as a
--     monthly snapshot to track whether the 9-tuple stays clean.
--   * If the 9-tuple stays clean over multiple corpora and ADR 0031's
--     v1 fragmentation rate exceeds its threshold, promote this to a
--     dbt test `assert_nhtsa_nine_tuple_identity_stable.sql` and migrate
--     silver to md5(9-tuple) + `recall_component_batch` child table.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-field cross-run real_drift counts (9-tuple grain, per-path-value-set) ==='
\echo 'Headline assertion: TOTAL = 0 means the 9-tuple identity is stable across runs.'
\echo 'Refactored 2026-05-13: per-path-value-set check. Pre-refactor (strict boolean'
\echo 'filter) reported TOTAL = 97 — all structural multi-batch, all silver-correct.'

with per_field as (
    select 'maketxt' as drifting_field, count(*) as drift_group_count
    from (
        select 1
        from (
            select campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
                   mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(maketxt::text, '<NULL>'),
                              ', ' order by coalesce(maketxt::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
                     mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'modeltxt', count(*)
    from (
        select 1
        from (
            select campno, maketxt, yeartxt, compname, rcl_cmpt_id,
                   mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(modeltxt::text, '<NULL>'),
                              ', ' order by coalesce(modeltxt::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
                     mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'yeartxt', count(*)
    from (
        select 1
        from (
            select campno, maketxt, modeltxt, compname, rcl_cmpt_id,
                   mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(yeartxt::text, '<NULL>'),
                              ', ' order by coalesce(yeartxt::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
                     mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'compname', count(*)
    from (
        select 1
        from (
            select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
                   mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(compname::text, '<NULL>'),
                              ', ' order by coalesce(compname::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
                     mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'rcl_cmpt_id', count(*)
    from (
        select 1
        from (
            select campno, maketxt, modeltxt, yeartxt, compname,
                   mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(rcl_cmpt_id::text, '<NULL>'),
                              ', ' order by coalesce(rcl_cmpt_id::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, modeltxt, yeartxt, compname,
                     mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, modeltxt, yeartxt, compname,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'mfr_comp_ptno', count(*)
    from (
        select 1
        from (
            select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                   mfr_comp_desc, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(mfr_comp_ptno::text, '<NULL>'),
                              ', ' order by coalesce(mfr_comp_ptno::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                     mfr_comp_desc, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_desc, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'mfr_comp_desc', count(*)
    from (
        select 1
        from (
            select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                   mfr_comp_ptno, mfr_comp_name,
                   raw_landing_path,
                   string_agg(distinct coalesce(mfr_comp_desc::text, '<NULL>'),
                              ', ' order by coalesce(mfr_comp_desc::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                     mfr_comp_ptno, mfr_comp_name,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_name
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
    ) g
    union all
    select 'mfr_comp_name', count(*)
    from (
        select 1
        from (
            select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                   mfr_comp_ptno, mfr_comp_desc,
                   raw_landing_path,
                   string_agg(distinct coalesce(mfr_comp_name::text, '<NULL>'),
                              ', ' order by coalesce(mfr_comp_name::text, '<NULL>')) as path_value_set
            from nhtsa_recalls_bronze
            group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                     mfr_comp_ptno, mfr_comp_desc,
                     raw_landing_path
        ) per_path
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc
        having count(distinct raw_landing_path) > 1
           and count(distinct path_value_set) > 1
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
\echo '=== Q2: sample real_drift groups per field (up to 5 per field) ==='
\echo 'For each field with non-zero drift, shows the shared 8 fields plus the per-path'
\echo 'value sets that diverged. observed_value_sets joins distinct per-path sets with " || "'
\echo '— e.g. "A || A, B" means one path reported {A} and another reported {A, B}.'
\echo 'Empty section = no real_drift for that field (the common, expected case).'

\echo
\echo '--- maketxt real_drift samples ---'
with per_path as (
    select campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(maketxt::text, '<NULL>'), ', ' order by coalesce(maketxt::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
             raw_landing_path
)
select campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- modeltxt real_drift samples ---'
with per_path as (
    select campno, maketxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(modeltxt::text, '<NULL>'), ', ' order by coalesce(modeltxt::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
             raw_landing_path
)
select campno, maketxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- yeartxt real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(yeartxt::text, '<NULL>'), ', ' order by coalesce(yeartxt::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
             raw_landing_path
)
select campno, maketxt, modeltxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- compname real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(compname::text, '<NULL>'), ', ' order by coalesce(compname::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- rcl_cmpt_id real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(rcl_cmpt_id::text, '<NULL>'), ', ' order by coalesce(rcl_cmpt_id::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- mfr_comp_ptno real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_desc, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_ptno::text, '<NULL>'), ', ' order by coalesce(mfr_comp_ptno::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_desc, mfr_comp_name,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_desc, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_desc, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- mfr_comp_desc real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_name,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_desc::text, '<NULL>'), ', ' order by coalesce(mfr_comp_desc::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_name,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_name,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_name
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- mfr_comp_name real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_name::text, '<NULL>'), ', ' order by coalesce(mfr_comp_name::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;
