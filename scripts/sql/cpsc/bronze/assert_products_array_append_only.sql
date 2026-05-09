-- Phase 5c follow-up — assert that CPSC's products[] JSONB array is
-- append-only (no insertions mid-array, no reorders) across bronze
-- snapshots.
--
-- Context: silver `recall_product_id` for CPSC is
--   md5(recall_event_id || '|' || name || '|' || model || '|' || product_ordinal)
-- per `dbt/models/silver/recall_product.sql:38-46`. The ordinal is derived
-- via `LATERAL jsonb_array_elements WITH ORDINALITY` from the bronze
-- `products` JSONB column. ADR 0031:96 calls out the implicit assumption:
-- "CPSC's design implicitly assumes `products[]` is append-only." If CPSC
-- ever inserts a new product mid-array OR reorders existing entries, every
-- product after the insertion point gets a new ordinal → new
-- recall_product_id → silver row fragmentation downstream.
--
-- Why it matters: a single mid-array insertion in a recall with N products
-- fragments N-1 silver rows. The blast radius scales with array length.
-- ADR 0031's per-source threshold for CPSC is `>0.1% silver row count
-- fragmented per quarter` before triggering Phase 6 reconciliation.
--
-- Strategy: for each recall, group by (source_recall_id, product_name,
-- product_model). If the same logical product appears at >1 distinct
-- ordinal across runs (cross-run filter via `count(distinct
-- raw_landing_path) > 1`), the array has been reordered or had something
-- inserted mid-position. The cross-run filter is essential — within a
-- single snapshot, identical (name, model) pairs at different ordinals
-- are legitimate (CPSC can list the same product variant twice in one
-- response). Cross-run drift is the falsification signal.
--
-- Caveat as of 2026-05-08: per
-- `documentation/cpsc/first_extraction_findings.md` Section A, every
-- observed CPSC recall has exactly 1 product. The "ordinal shift" failure
-- mode physically cannot occur on a 1-element array. A current
-- `drift_group_count = 0` therefore means "we've seen no opportunity for
-- the assumption to fail" — it does NOT mean the assumption is verified.
-- Continued monitoring as bronze accumulates multi-product recalls is
-- required.
--
-- NULL semantics: GROUP BY name+model relies on Postgres's NULL-as-
-- group-key behavior (NULLs collide into one group). The cardinality
-- test is `count(distinct ordinal) > 1` — a product appearing at
-- multiple distinct ordinals across runs is the violation.
--
-- Expected outcome on a clean corpus: drift_group_count = 0.
-- Non-zero results mean either:
--   (a) CPSC inserted a product mid-array — investigate the
--       affected recalls; trigger Phase 6 reconciliation per
--       ADR 0031, OR
--   (b) the array was reordered — same response, possibly more
--       extensive fragmentation downstream.
--
-- Wire-up: also exercised via dbt singular test
-- `dbt/tests/source_assumptions/assert_cpsc_products_array_append_only.sql`
-- at severity=warn.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: products[] append-only headline assertion ==='
\echo 'drift_group_count = 0 means CPSC has not reordered products[] or'
\echo 'inserted a product mid-array in any recall observed across the bronze'
\echo 'snapshots in this database.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        prod.value ->> 'name'  as product_name,
        prod.value ->> 'model' as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select count(*) as drift_group_count
from (
    select source_recall_id, product_name, product_model
    from exploded
    group by source_recall_id, product_name, product_model
    having count(distinct product_ordinal) > 1
       and count(distinct raw_landing_path) > 1
) g;

\echo
\echo '=== Q2: sample append-only violations (up to 5 cases) ==='
\echo 'Each row shows a (recall, product) that landed at multiple distinct'
\echo 'ordinal positions across runs. n_landing_paths > 1 confirms the drift'
\echo 'is cross-run, not within-run.'

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        prod.value ->> 'name'  as product_name,
        prod.value ->> 'model' as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select
    source_recall_id,
    product_name,
    product_model,
    string_agg(distinct product_ordinal::text, ' | ' order by product_ordinal::text) as distinct_ordinals,
    count(distinct raw_landing_path) as n_landing_paths,
    count(*) as n_rows
from exploded
group by source_recall_id, product_name, product_model
having count(distinct product_ordinal) > 1
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '=== Q3: corpus-wide products[] length distribution ==='
\echo 'Context for interpreting Q1: if max products per recall is 1 today,'
\echo 'the append-only assumption physically cannot fail on observed data.'
\echo 'Watch this distribution as bronze accumulates multi-product recalls.'

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
