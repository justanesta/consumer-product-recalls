-- Singular test: CPSC product name/model strings do not get character-
-- normalized across bronze snapshots. Returns rows for each
-- (source_recall_id, ordinal) where name OR model has taken >1 distinct
-- value across runs (NULL→non-NULL also counts). Severity=warn via
-- dbt_project.yml. Per-field breakdown and samples in
-- scripts/sql/cpsc/bronze/assert_name_model_normalization_stable.sql.

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
select source_recall_id, product_ordinal
from exploded
group by source_recall_id, product_ordinal
having count(distinct raw_landing_path) > 1
   and (
        count(distinct product_name)  > 1
        or count(distinct product_model) > 1
        or (count(*) > count(product_name)  and count(product_name)  > 0)
        or (count(*) > count(product_model) and count(product_model) > 0)
   )
