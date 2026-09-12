-- Headline assertions for CPSC's `products[]` stability across bronze snapshots.
-- The psql counterpart of the two dbt singular tests:
--   dbt/tests/source_assumptions/assert_cpsc_products_array_append_only.sql  (Q1/Q2 here)
--   dbt/tests/source_assumptions/assert_cpsc_product_ordinal_stable.sql      (Q4/Q5 here)
-- Incident drill-down lives in the companion
--   diagnose_products_array_append_only_violations.sql
--
-- ============================================================================
-- C2 ("products[] is append-only") IS FALSIFIED -- 2026-09-12
-- ============================================================================
-- CPSC is collapsing enumerated per-model product rows into a single summary row
-- across the historical archive, walking forward roughly daily since 2026-06-10:
--   00176  8 -> 1  eight named Empire ride-on models -> '"Power Drivers" and "Buddy L"'
--   00119  7 -> 1  six named robe brands            -> 'Children''s robes'
--   00105  6 -> 1  six named Answer/Manitou forks   -> 'Answer and Manitou brand bicycle forks'
--   00163  5 -> 1  Coyote/Fox/Manco/Phoenix/Rattler -> 'Go-karts sold under the Manco, ...'
-- Upstream, not an extraction artifact: the 2026-09-06 deep rescan independently
-- re-fetched the collapsed arrays through a different query and landing file.
-- Baseline 2026-09-12: 64 regressions / 64 recalls / 107 product rows absent from
-- silver (silver reads the latest snapshot only). See ADR 0031's 2026-09-12
-- amendment and documentation/cpsc/array_stability_findings.md.
--
-- WHY THE PREDICATES CHANGED. The original assertion grouped by
-- (source_recall_id, product_name, product_model) and flagged any group spanning
-- > 1 ordinal AND > 1 raw_landing_path. Those two conditions are evaluated
-- independently, which is unsound in both directions once the 2026-06-13 ADR 0031
-- amendment demoted name/model to mutable Type-1 attributes:
--   * it fired on 12 legacy recalls where nothing had moved (a legitimate
--     within-snapshot duplicate pair plus a newly-acquired second snapshot),
--     holding transform.yml red 2026-09-07 -> 2026-09-12; and
--   * it caught NONE of the 64 real regressions, because a consolidation renames
--     slot 1 as it truncates, leaving the old and new (name, model) groups with
--     one landing path each -- both filtered out.
-- Q1/Q2 and Q4/Q5 below are the replacement: shrinkage and slot-movement metered
-- separately, because they now have different baselines (64-and-rising vs 0).
--
-- Expected outcome: Q1 tracks the reviewed shrinkage baseline; Q4 must be 0.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: LENGTH_REGRESSION headline -- products[] got shorter ==='
\echo 'Baseline 2026-09-12 = 64. This is a METER on a falsified assumption, not a'
\echo 'pass/fail gate; the dbt wrapper warns above 0 and errors above the reviewed'
\echo 'baseline. A jump means the consolidation rate accelerated -- re-check Q2 and'
\echo 'the diagnostic companion Q8/Q9 before raising the threshold.'

with lengths as (
    select
        source_recall_id,
        extraction_timestamp,
        content_hash,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from cpsc_recalls_bronze
),
with_previous as (
    select
        source_recall_id,
        n_products,
        lag(n_products) over (
            partition by source_recall_id
            order by extraction_timestamp, content_hash
        ) as previous_n_products
    from lengths
)
select count(*) as length_regression_count
from with_previous
where previous_n_products is not null
  and n_products < previous_n_products;

\echo
\echo '=== Q2: sample LENGTH_REGRESSION cases (largest drops first) ==='

with lengths as (
    select
        source_recall_id,
        extraction_timestamp,
        content_hash,
        jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from cpsc_recalls_bronze
),
with_previous as (
    select
        source_recall_id,
        extraction_timestamp,
        n_products,
        lag(n_products) over (
            partition by source_recall_id
            order by extraction_timestamp, content_hash
        ) as previous_n_products
    from lengths
)
select
    source_recall_id,
    extraction_timestamp,
    previous_n_products,
    n_products,
    previous_n_products - n_products as product_rows_lost
from with_previous
where previous_n_products is not null
  and n_products < previous_n_products
order by product_rows_lost desc, source_recall_id
limit 15;

\echo
\echo '=== Q3: corpus-wide products[] length distribution ==='
\echo 'Context for Q1/Q2. The 2026-06-02 full-corpus seed measured 8.3% of recalls'
\echo 'carrying > 1 product (max 57); the consolidation wave is eroding that.'

with arr_len as (
    select source_recall_id,
           jsonb_array_length(coalesce(products, '[]'::jsonb)) as n_products
    from cpsc_recalls_bronze
)
select
    n_products,
    count(*) as n_recall_rows,
    count(distinct source_recall_id) as n_distinct_recalls
from arr_len
group by n_products
order by n_products;

\echo
\echo '=== Q4: ORDINAL_MOVED headline -- a product changed array position ==='
\echo 'MUST be 0. This is the identity invariant: recall_product_id is'
\echo 'md5(CPSC|source_recall_id|product_ordinal), so a product that moves slots'
\echo 'means a later product silently inherits an earlier slot''s id. Restricted to'
\echo '(name, model) pairs that are a singleton in every snapshot they appear in --'
\echo 'that restriction is what removes the false positives of the old formulation.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),
per_path as (
    select
        source_recall_id, product_name, product_model, raw_landing_path,
        count(*)             as n_slots_in_snapshot,
        min(product_ordinal) as the_ordinal
    from exploded
    group by source_recall_id, product_name, product_model, raw_landing_path
),
singletons as (
    select source_recall_id, product_name, product_model
    from per_path
    group by source_recall_id, product_name, product_model
    having max(n_slots_in_snapshot) = 1
)
select count(*) as ordinal_moved_count
from (
    select p.source_recall_id, p.product_name, p.product_model
    from per_path p
    join singletons s
      on  s.source_recall_id = p.source_recall_id
      and s.product_name     = p.product_name
      and s.product_model    = p.product_model
    group by p.source_recall_id, p.product_name, p.product_model
    having count(distinct p.the_ordinal) > 1
) g;

\echo
\echo '=== Q5: sample ORDINAL_MOVED cases (empty when Q4 = 0) ==='

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        coalesce(prod.value ->> 'name',  '<<NULL>>') as product_name,
        coalesce(prod.value ->> 'model', '<<NULL>>') as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),
per_path as (
    select
        source_recall_id, product_name, product_model, raw_landing_path,
        count(*)             as n_slots_in_snapshot,
        min(product_ordinal) as the_ordinal
    from exploded
    group by source_recall_id, product_name, product_model, raw_landing_path
),
singletons as (
    select source_recall_id, product_name, product_model
    from per_path
    group by source_recall_id, product_name, product_model
    having max(n_slots_in_snapshot) = 1
)
select
    p.source_recall_id,
    left(p.product_name, 50)  as product_name,
    left(p.product_model, 20) as product_model,
    count(*)                      as n_snapshots,
    count(distinct p.the_ordinal) as n_distinct_ordinals,
    string_agg(distinct p.the_ordinal::text, ' -> ' order by p.the_ordinal::text) as ordinals_seen
from per_path p
join singletons s
  on  s.source_recall_id = p.source_recall_id
  and s.product_name     = p.product_name
  and s.product_model    = p.product_model
group by p.source_recall_id, p.product_name, p.product_model
having count(distinct p.the_ordinal) > 1
order by p.source_recall_id
limit 15;
