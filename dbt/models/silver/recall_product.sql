{{ config(materialized='table') }}

-- Line-level recall products (ADR 0002). One row per element in the CPSC
-- Products[] array per recall event. CPSC does not associate specific UPCs
-- with specific products, so `upc` is NULL here; the recall-level UPC list
-- lives on recall_event.source_payload_raw.

with exploded as (
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
)

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
from exploded
