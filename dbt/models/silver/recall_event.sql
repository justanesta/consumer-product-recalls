{{ config(materialized='table') }}

-- Header-level recall events (ADR 0002). One row per CPSC recall.
-- Cross-source unification lands in Phase 6; for now CPSC is the only source.

select
    md5('CPSC' || '|' || source_recall_id) as recall_event_id,
    'CPSC'                                 as source,
    source_recall_id,
    announced_at,
    published_at,
    title,
    description,
    url,
    cast(null as text)                     as classification,  -- CPSC does not publish
    cast(null as text)                     as status,          -- CPSC does not publish
    hazards,
    jsonb_build_object(
        'recall_id',              recall_id,
        'consumer_contact',       consumer_contact,
        'sold_at_label',          sold_at_label,
        'manufacturer_countries', manufacturer_countries,
        'product_upcs',           product_upcs,
        'remedies',               remedies,
        'remedy_options',         remedy_options,
        'in_conjunctions',        in_conjunctions,
        'images',                 images,
        'injuries',               injuries
    )                                      as source_payload_raw,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from {{ ref('stg_cpsc_recalls') }}
