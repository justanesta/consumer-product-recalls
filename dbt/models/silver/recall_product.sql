{{ config(materialized='table') }}

-- Line-level recall products (ADR 0002). One row per affected product instance.
-- CPSC: explodes the Products[] JSONB array — one row per array element with
--   ordinal-based surrogate key to distinguish identical product names.
-- FDA: each bronze row IS a product (PRODUCTID = source_recall_id), so no array
--   explosion needed — staging feeds directly into the product table.
-- Neither source associates UPCs with specific products (CPSC UPCs are recall-level;
-- FDA does not return them via the bulk POST endpoint), so upc is NULL for both.

with cpsc_exploded as (
    select
        s.source_recall_id,
        md5('CPSC' || '|' || s.source_recall_id) as recall_event_id,
        (prod.value ->> 'name')               as product_name,
        (prod.value ->> 'description')        as product_description,
        (prod.value ->> 'model')              as model,
        (prod.value ->> 'type')               as type,
        (prod.value ->> 'category_id')        as category_id,
        (prod.value ->> 'number_of_units')    as number_of_units,
        prod.value                            as source_specific_attrs,
        prod.ordinality                       as product_ordinal
    from {{ ref('stg_cpsc_recalls') }} s,
         lateral jsonb_array_elements(coalesce(s.products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),

cpsc_products as (
    select
        md5(recall_event_id || '|' || coalesce(product_name, '') || '|'
            || coalesce(model, '') || '|' || product_ordinal::text) as recall_product_id,
        recall_event_id,
        'CPSC'                 as source,
        source_recall_id,
        product_name,
        product_description,
        model,
        type,
        category_id,
        number_of_units,
        cast(null as text)     as upc,
        source_specific_attrs
    from cpsc_exploded
),

fda_products as (
    select
        md5('FDA' || '|' || source_recall_id)         as recall_product_id,
        md5('FDA' || '|' || recall_event_id::text)    as recall_event_id,
        'FDA'                                         as source,
        source_recall_id,
        product_description_txt                       as product_name,
        product_short_reason_txt                      as product_description,
        cast(null as text)                            as model,
        product_type_short                            as type,
        cast(null as text)                            as category_id,
        product_distributed_quantity                  as number_of_units,
        cast(null as text)                            as upc,
        jsonb_build_object(
            'rid',                            rid,
            'center_cd',                      center_cd,
            'recall_num',                     recall_num,
            'center_classification_type_txt', center_classification_type_txt
        )                                             as source_specific_attrs
    from {{ ref('stg_fda_recalls') }}
)

select * from cpsc_products
union all
select * from fda_products
