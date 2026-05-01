{{ config(materialized='view') }}

-- Latest-per-recall English projection over the USDA FSIS bronze table.
-- USDA's natural identity is (source_recall_id, langcode) — bilingual EN/ES
-- siblings share a recall number. Dedup partitions by both, then filters to
-- English only ('EN-primary, drop ES'). Spanish siblings remain in bronze
-- for audit but do not propagate to silver.
--
-- last_modified_date is 42% null per Finding D; downstream silver coalesces to
-- recall_date.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id, langcode
            order by extraction_timestamp desc
        ) as rn
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
)

select
    source_recall_id,
    title,
    recall_date::timestamptz        as announced_at,
    last_modified_date::timestamptz as published_at,
    closed_date::timestamptz        as closed_at,
    recall_classification           as classification,
    recall_type,
    risk_level,
    archive_recall,
    active_notice,
    related_to_outbreak,
    establishment,
    recall_reason,
    processing,
    states,
    summary,
    product_items,
    distro_list,
    labels,
    qty_recovered,
    recall_url                      as url,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
  and langcode = 'English'
