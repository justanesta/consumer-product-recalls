-- Phase 5c — decompose 9-tuple cross-run drift into structural multi-batch
-- vs. real drift. Companion to `decompose_eleven_tuple_drift.sql` at
-- 9-tuple grain (drops endman + bgman from identity).
--
-- Status: EXPLORATORY / forensic. Production silver uses md5(11-tuple) per
-- `dbt/models/silver/recall_product.sql:99-108`, NOT md5(9-tuple). The
-- operational fragmentation signal is the 11-tuple dbt test:
-- `dbt/tests/source_assumptions/assert_nhtsa_eleven_tuple_identity_stable.sql`.
-- This script answers a different question:
--
--    "If we MIGRATED silver to the 9-tuple identity (per ADR 0031 Section G's
--     'silver implication' exploration), what would the fragmentation rate
--     look like? Specifically, does collapsing endman/bgman expose real
--     mfr_comp_ptno drift that the 11-tuple grain masked?"
--
-- Context: `assert_nine_tuple_identity_stable.sql` uses the older STRICT
-- boolean counting (TOTAL=0 means stable), conflating two phenomena:
--
--   (a) STRUCTURAL MULTI-BATCH — false positive, silver-correct.
--       Same physical component legitimately reports multiple ptno values
--       (e.g., LEAF battery chemistry variants `295B0 5SA1C` + `295B0 5SF0A`,
--       both present in every archive). At 11-tuple grain the multiple
--       ptnos appear as distinct rows; at 9-tuple grain they appear as
--       multiple values within one group. Per-path value sets are
--       identical → not actionable.
--
--   (b) REAL DRIFT — the failure mode the 9-tuple hypothesis would catch.
--       Field takes value A in archive 1 and value B in archive 2 (or
--       per-path value sets otherwise diverge). At 9-tuple grain, this
--       could surface drift that the 11-tuple masked: if path 1 had
--       (bgman=A, ptno=1) and path 2 had (bgman=B, ptno=2), the 11-tuple
--       sees two distinct rows with no drift; the 9-tuple merges them and
--       sees ptno {1} vs {2} as divergent value sets → flagged.
--
-- Mechanism: identical to `decompose_eleven_tuple_drift.sql`. For each
-- flagged group, count distinct `(raw_landing_path, field_value)` pairs.
-- If pairs == `n_paths * n_distinct_values` (Cartesian) → every path saw
-- every value → structural. If pairs < Cartesian → at least one path
-- missed a value → real drift.
--
-- Divergence from `assert_eleven_tuple_identity_stable.sql` (rich):
--   * That script: 10 non-campno identity fields, 10-field GROUP BY,
--     no decomposition (returns raw drift_group_count per field).
--   * This script: 8 non-campno 9-tuple fields, 8-field GROUP BY,
--     decomposed into structural_multi_batch + real_drift columns.
--
-- Divergence from the dbt test (`assert_nhtsa_eleven_tuple_identity_stable.sql`):
--   * dbt test: 11-tuple grain (includes endman, bgman in GROUP BY).
--     Production signal — alert fires on real_drift > 0 per ADR 0031:84.
--     Mechanism: per-path-value-set divergence check (same logic as below,
--     filtered to real_drift only — `count(distinct path_value_set) > 1`).
--   * This script: 9-tuple grain (excludes endman, bgman from GROUP BY).
--     Forensic — answers the hypothetical "if we used the 9-tuple instead,
--     what would silver fragmentation look like?" Reports both structural
--     and real_drift, so you can see the full noise / signal split.
--   * Mechanism is symmetric to the dbt test's mechanism, applied at a
--     different identity grain.
--
-- NULL handling: field values are coalesced to `'<NULL>'` so NULL/non-NULL
-- mixes participate in both cardinality and pair counts. Row tuples
-- `(raw_landing_path, coalesced_value)` are always non-null because
-- `raw_landing_path` is non-nullable.
--
-- Expected outcome (hypothesis check): if the 9-tuple is a viable silver
-- canonical-entity surrogate, real_drift TOTAL should be near zero (modulo
-- the ~known 11-tuple endman/bgman cases which would NOT appear here by
-- definition — they get collapsed away). Non-zero real_drift here means
-- collapsing endman/bgman exposed cross-run divergence on one of the 8
-- 9-tuple fields — a stronger fragmentation signal than the 11-tuple count.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-field decomposition (structural_multi_batch vs real_drift) — 9-tuple grain ==='
\echo 'real_drift is the column that would drive silver-fragmentation if md5(9-tuple) were'
\echo 'adopted as the canonical key. structural_multi_batch is silver-correct noise.'
\echo 'TOTAL of (structural + real_drift) should equal assert_nine_tuple_identity_stable.sql Q1 TOTAL.'

with per_field as (
    select 'maketxt' as drifting_field,
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals) as structural_multi_batch,
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals) as real_drift
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(maketxt::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(maketxt::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having (count(distinct maketxt) > 1
                or (count(*) > count(maketxt) and count(maketxt) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'modeltxt',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(modeltxt::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(modeltxt::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having (count(distinct modeltxt) > 1
                or (count(*) > count(modeltxt) and count(modeltxt) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'yeartxt',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(yeartxt::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(yeartxt::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having (count(distinct yeartxt) > 1
                or (count(*) > count(yeartxt) and count(yeartxt) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'compname',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(compname::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(compname::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having (count(distinct compname) > 1
                or (count(*) > count(compname) and count(compname) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'rcl_cmpt_id',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(rcl_cmpt_id::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(rcl_cmpt_id::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name
        having (count(distinct rcl_cmpt_id) > 1
                or (count(*) > count(rcl_cmpt_id) and count(rcl_cmpt_id) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'mfr_comp_ptno',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(mfr_comp_ptno::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(mfr_comp_ptno::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_desc, mfr_comp_name
        having (count(distinct mfr_comp_ptno) > 1
                or (count(*) > count(mfr_comp_ptno) and count(mfr_comp_ptno) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'mfr_comp_desc',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(mfr_comp_desc::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(mfr_comp_desc::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_name
        having (count(distinct mfr_comp_desc) > 1
                or (count(*) > count(mfr_comp_desc) and count(mfr_comp_desc) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'mfr_comp_name',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(mfr_comp_name::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(mfr_comp_name::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc
        having (count(distinct mfr_comp_name) > 1
                or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
),
labeled as (
    select drifting_field, structural_multi_batch, real_drift,
           structural_multi_batch + real_drift as total, 0 as sort_section
    from per_field
    union all
    select 'TOTAL', sum(structural_multi_batch), sum(real_drift),
           sum(structural_multi_batch + real_drift), 1
    from per_field
)
select drifting_field, structural_multi_batch, real_drift, total
from labeled
order by sort_section, real_drift desc, structural_multi_batch desc, drifting_field;

\echo
\echo '=== Q2: real_drift sample groups per field (up to 5 per field) ==='
\echo 'Empty section means 0 real_drift cases for that field — only structural_multi_batch'
\echo '(silver-correct false positive) was observed there. observed_value_sets shows the'
\echo 'distinct per-path value sets, joined by " || " — e.g. "A || A, B" means one path'
\echo 'reported {A} and another reported {A, B}.'

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
