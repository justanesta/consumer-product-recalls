-- Phase 5c — decompose 11-tuple cross-run drift into structural multi-batch
-- vs. real drift. Companion / refinement to
-- `assert_eleven_tuple_identity_stable.sql`.
--
-- Context: the assertion flags any 10-field group whose dropped 11th field
-- takes >1 distinct value (or a NULL/non-NULL mix) across >1
-- `raw_landing_path`. That single filter conflates two phenomena —
-- documented at `documentation/nhtsa/incremental_delta_findings.md:235-243`:
--
--   (a) STRUCTURAL MULTI-BATCH — false positive, silver-correct.
--       One physical recall component legitimately has multiple values
--       for the field (e.g., Ferrari 26V152000 ptno 000788416 + 000788418
--       are both real for the same side-window component, both reported
--       in both runs). The 11-tuple is stable per (path, value); silver's
--       `(11-tuple → max(extraction_timestamp))` lookup correctly emits
--       one row per real value. Not actionable.
--
--   (b) REAL DRIFT — the failure mode ADR 0030/0031 traded off.
--       Field took value A in archive 1 and value B in archive 2, or
--       swapped NULL ↔ populated. Silver fragments the same physical
--       thing into two rows. Example: Western Star 26V079000 `endman`
--       `2026-02-03 → 2026-04-10`. This is what feeds ADR 0031:84's
--       ">0.01% silver row count fragmented per month" trigger.
--
-- Mechanism: for each flagged group, count distinct
-- `(raw_landing_path, field_value)` pairs. If pairs ==
-- `n_paths * n_distinct_values` (the Cartesian product), every path saw
-- every value → per-path value sets are identical → structural
-- multi-batch. If pairs < Cartesian, at least one path missed at least
-- one value → per-path sets differ → real drift.
--
-- Worked example (mfr_comp_ptno, n_paths=2, values {A, B}):
--   path 1 rows = {A, B}, path 2 rows = {A, B} → 4 pairs, Cartesian 4 → STRUCTURAL
--   path 1 rows = {A},    path 2 rows = {B}    → 2 pairs, Cartesian 4 → REAL DRIFT
--   path 1 rows = {A},    path 2 rows = {A, B} → 3 pairs, Cartesian 4 → REAL DRIFT
--
-- NULL handling: field values are coalesced to `'<NULL>'` so NULL/non-NULL
-- mixes participate in both cardinality and pair counts. Row tuples
-- `(raw_landing_path, coalesced_value)` are always non-null because
-- `raw_landing_path` is non-nullable.
--
-- Wire-up: only the `real_drift` count contributes to ADR 0031:84
-- silver-fragmentation triggers. The `structural_multi_batch` count is
-- informational — silver materializes those correctly. If structural
-- dominates across many runs (expected per the Ferrari/Chrysler patterns
-- in `incremental_delta_findings.md:235`), the next step is to fold the
-- per-path-set check into `assert_eleven_tuple_identity_stable.sql` as a
-- default filter and re-baseline ADR 0031:84 with the cleaner number.
--
-- Implements the assertion refinement foreshadowed at
-- `documentation/nhtsa/incremental_delta_findings.md:241-243`
-- ("the assertion should add a 'value set unchanged across runs' check").

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-field decomposition (structural_multi_batch vs real_drift) ==='
\echo 'real_drift is the column that drives ADR 0031 silver-fragmentation triggers.'
\echo 'structural_multi_batch is silver-correct noise (incremental_delta_findings.md:235).'
\echo 'TOTAL of (structural + real_drift) should equal assert_eleven_tuple_identity_stable.sql Q1 TOTAL.'

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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
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
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
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
                 mfr_comp_desc, mfr_comp_name, endman, bgman
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
                 mfr_comp_ptno, mfr_comp_name, endman, bgman
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
                 mfr_comp_ptno, mfr_comp_desc, endman, bgman
        having (count(distinct mfr_comp_name) > 1
                or (count(*) > count(mfr_comp_name) and count(mfr_comp_name) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'endman',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(endman::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(endman::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman
        having (count(distinct endman) > 1
                or (count(*) > count(endman) and count(endman) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'bgman',
           count(*) filter (where path_value_pairs = n_paths * n_distinct_vals),
           count(*) filter (where path_value_pairs < n_paths * n_distinct_vals)
    from (
        select count(distinct raw_landing_path) as n_paths,
               count(distinct coalesce(bgman::text, '<NULL>')) as n_distinct_vals,
               count(distinct (raw_landing_path, coalesce(bgman::text, '<NULL>'))) as path_value_pairs
        from nhtsa_recalls_bronze
        group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
                 mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman
        having (count(distinct bgman) > 1
                or (count(*) > count(bgman) and count(bgman) > 0))
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
\echo 'Empty section means 0 real_drift cases for that field — only the'
\echo 'structural_multi_batch class (silver-correct false positive) was'
\echo 'observed there. observed_value_sets shows the distinct per-path'
\echo 'value sets, joined by " || " — e.g. "A || A, B" means one path'
\echo 'reported {A} and another reported {A, B}.'

\echo
\echo '--- maketxt real_drift samples ---'
with per_path as (
    select campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(maketxt::text, '<NULL>'), ', ' order by coalesce(maketxt::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- modeltxt real_drift samples ---'
with per_path as (
    select campno, maketxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(modeltxt::text, '<NULL>'), ', ' order by coalesce(modeltxt::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, maketxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- yeartxt real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(yeartxt::text, '<NULL>'), ', ' order by coalesce(yeartxt::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- compname real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(compname::text, '<NULL>'), ', ' order by coalesce(compname::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- rcl_cmpt_id real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(rcl_cmpt_id::text, '<NULL>'), ', ' order by coalesce(rcl_cmpt_id::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- mfr_comp_ptno real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_desc, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_ptno::text, '<NULL>'), ', ' order by coalesce(mfr_comp_ptno::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_desc, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_desc, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_desc, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- mfr_comp_desc real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_name, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_desc::text, '<NULL>'), ', ' order by coalesce(mfr_comp_desc::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_name, endman, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_name, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_name, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- mfr_comp_name real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, endman, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(mfr_comp_name::text, '<NULL>'), ', ' order by coalesce(mfr_comp_name::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, endman, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, endman, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, endman, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- endman real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman,
           raw_landing_path,
           string_agg(distinct coalesce(endman::text, '<NULL>'), ', ' order by coalesce(endman::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, bgman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;

\echo
\echo '--- bgman real_drift samples ---'
with per_path as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman,
           raw_landing_path,
           string_agg(distinct coalesce(bgman::text, '<NULL>'), ', ' order by coalesce(bgman::text, '<NULL>')) as path_value_set
    from nhtsa_recalls_bronze
    group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
             mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman,
             raw_landing_path
)
select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
       mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman,
       count(distinct raw_landing_path) as n_paths,
       count(distinct path_value_set) as n_distinct_path_sets,
       string_agg(distinct path_value_set, ' || ' order by path_value_set) as observed_value_sets
from per_path
group by campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
         mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman
having count(distinct raw_landing_path) > 1
   and count(distinct path_value_set) > 1
limit 5;
