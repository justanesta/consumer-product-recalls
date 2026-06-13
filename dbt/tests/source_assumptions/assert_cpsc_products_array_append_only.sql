{{ config(severity='error') }}
-- Singular test: CPSC products[] is append-only across bronze snapshots.
-- Returns rows for each (source_recall_id, name, model) that has been
-- observed at >1 distinct ordinal across runs.
--
-- SEVERITY = ERROR — overrides the source_assumptions group default of `warn`
-- (dbt_project.yml). Escalated 2026-06-13 (ADR 0031 amendment): this is no longer
-- a threshold-based monitor with a known baseline. Since CPSC product identity now
-- keys on (event, ordinal) alone (`recall_product.sql` cpsc_products CTE), a
-- violation here is a silent identity *conflation* — a later product inherits an
-- earlier slot's recall_product_id — i.e. a correctness incident that must fail the
-- build, not just warn. Baseline is 0, validated on the multi-product corpus
-- 2026-06-13. Rich diagnostic version with per-violation samples lives at
-- scripts/sql/cpsc/bronze/assert_products_array_append_only.sql.

with exploded as (
    select
        source_recall_id,
        raw_landing_path,
        prod.ordinality::int as product_ordinal,
        prod.value ->> 'name'  as product_name,
        prod.value ->> 'model' as product_model
    from {{ source('cpsc', 'cpsc_recalls_bronze') }},
         lateral jsonb_array_elements(coalesce(products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
)
select source_recall_id, product_name, product_model
from exploded
group by source_recall_id, product_name, product_model
having count(distinct product_ordinal) > 1
   and count(distinct raw_landing_path) > 1
