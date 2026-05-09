-- Phase 5c follow-up — assert that CPSC product `name` and `model`
-- strings are not character-normalized after publication.
--
-- Context: silver `recall_product_id` for CPSC includes raw `name` and
-- `model` in its md5 hash (per `dbt/models/silver/recall_product.sql:40`).
-- If CPSC ever normalizes (e.g., 'AC DELCO' → 'ACDELCO',
-- '  Toaster Oven  ' → 'Toaster Oven', 'Type-A' → 'TypeA') for an
-- existing (source_recall_id, ordinal) slot, the hash changes → silver
-- row fragmentation. This is the same class of failure NHTSA exhibits
-- on `maketxt` per ADR 0031:84 ("AC DELCO maketxt normalization
-- observed 2026-05-08"); detecting it for CPSC is the parity assertion.
--
-- Why it matters: this is independent of the array-reorder assumption
-- (`assert_products_array_append_only.sql`). Even if CPSC never reorders
-- products[], a character-normalization edit on name/model still
-- fragments the silver surrogate.
--
-- Strategy: for each (source_recall_id, ordinal), compare the raw and
-- normalized (UPPER+TRIM) name and model values across runs. If the
-- same slot's `upper(trim(name))` (or model) takes >1 distinct value
-- across `count(distinct raw_landing_path) > 1` runs, normalization has
-- occurred. The UPPER+TRIM mirrors the firm-name normalization used at
-- `dbt/models/silver/firm.sql:82` — it catches casing/whitespace edits
-- AND any other character-level edit (since `normalized_a != normalized_b`
-- iff `a != b` after uppercase+trim).
--
-- NULL semantics: a transition between NULL and non-NULL on either
-- field is also a violation (count(*) > count(field) AND count(field) > 0
-- mirrors the NHTSA assert pattern).
--
-- Expected outcome on a clean corpus: drift_group_count = 0 for both
-- name and model.
-- Non-zero results mean CPSC has performed a post-publication character
-- edit on a product name/model — Phase 6 reconciliation triggered per
-- ADR 0031.
--
-- Wire-up: also exercised via dbt singular test
-- `dbt/tests/source_assumptions/assert_cpsc_name_model_normalization_stable.sql`
-- at severity=warn.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-field cross-run normalization-drift counts ==='
\echo 'Headline assertion: TOTAL = 0 means no (source_recall_id, ordinal)'
\echo 'slot has had its name or model character-normalized across runs.'

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
),
per_field as (
    select 'name' as drifting_field, count(*) as drift_group_count
    from (
        select 1
        from exploded
        group by source_recall_id, product_ordinal
        having (count(distinct product_name) > 1
                or (count(*) > count(product_name) and count(product_name) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
    union all
    select 'model', count(*)
    from (
        select 1
        from exploded
        group by source_recall_id, product_ordinal
        having (count(distinct product_model) > 1
                or (count(*) > count(product_model) and count(product_model) > 0))
           and count(distinct raw_landing_path) > 1
    ) g
),
labeled as (
    select drifting_field, drift_group_count, 0 as sort_section
    from per_field
    union all
    select 'TOTAL', sum(drift_group_count), 1 from per_field
)
select drifting_field, drift_group_count
from labeled
order by sort_section, drift_group_count desc, drifting_field;

\echo
\echo '=== Q2: sample name-drift cases (up to 5) ==='

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        prod.value ->> 'name' as product_name
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select
    source_recall_id,
    product_ordinal,
    string_agg(distinct case when product_name is null then '<NULL>' else product_name end,
               ' | ' order by case when product_name is null then '<NULL>' else product_name end) as distinct_names,
    count(distinct raw_landing_path) as n_landing_paths
from exploded
group by source_recall_id, product_ordinal
having (count(distinct product_name) > 1
        or (count(*) > count(product_name) and count(product_name) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;

\echo
\echo '=== Q3: sample model-drift cases (up to 5) ==='

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        prod.value ->> 'model' as product_model
    from cpsc_recalls_bronze,
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select
    source_recall_id,
    product_ordinal,
    string_agg(distinct case when product_model is null then '<NULL>' else product_model end,
               ' | ' order by case when product_model is null then '<NULL>' else product_model end) as distinct_models,
    count(distinct raw_landing_path) as n_landing_paths
from exploded
group by source_recall_id, product_ordinal
having (count(distinct product_model) > 1
        or (count(*) > count(product_model) and count(product_model) > 0))
   and count(distinct raw_landing_path) > 1
limit 5;
