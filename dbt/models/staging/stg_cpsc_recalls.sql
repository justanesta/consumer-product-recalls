{{ config(materialized='view') }}

-- Latest-per-recall projection over the CPSC bronze table.
-- Bronze may contain multiple rows per source_recall_id when content changes
-- (content-hash dedup prevents identical re-ingestion but legitimate edits
-- produce new rows). Silver consumes only the most recent version.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from {{ source('cpsc', 'cpsc_recalls_bronze') }}
)

select
    source_recall_id,
    recall_id,
    recall_date::timestamptz       as announced_at,
    last_publish_date::timestamptz as published_at,
    title,
    description,
    url,
    consumer_contact,
    sold_at_label,
    products,
    manufacturers,
    retailers,
    importers,
    distributors,
    manufacturer_countries,
    product_upcs,
    hazards,
    remedies,
    remedy_options,
    in_conjunctions,
    images,
    injuries,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
