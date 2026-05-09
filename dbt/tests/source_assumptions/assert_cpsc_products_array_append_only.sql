-- Singular test: CPSC products[] is append-only across bronze snapshots.
-- Returns rows for each (source_recall_id, name, model) that has been
-- observed at >1 distinct ordinal across runs. Severity=warn via
-- dbt_project.yml. Rich diagnostic version with per-violation samples
-- lives at scripts/sql/cpsc/bronze/assert_products_array_append_only.sql.

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
