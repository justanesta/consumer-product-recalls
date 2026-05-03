{{ config(materialized='view') }}

-- Latest-per-recall English projection over the USDA FSIS bronze table.
-- USDA's natural identity is (source_recall_id, langcode) — bilingual EN/ES
-- siblings share a recall number. Dedup partitions by both, then filters to
-- English only ('EN-primary, drop ES'). Spanish siblings remain in bronze
-- for audit but do not propagate to silver.
--
-- last_modified_date is 42% null per Finding D; downstream silver coalesces to
-- recall_date.
--
-- nullif(col, '') wrappers per ADR 0027: bronze preserves the source's ''
-- representation verbatim (Finding C — many fields use '' as a missing-value
-- sentinel). Silver normalizes empty strings to null so downstream consumers
-- don't have to remember the dance.
--
-- HTML-entity decode on `establishment` per
-- documentation/usda/establishment_join_coverage.md: the recall API returns
-- names with `&#039;` (apostrophe) and `&amp;` (ampersand), while the
-- Establishment Listing API returns plain text. Decoding on the recall side
-- before the silver join lifts the per-distinct-name match rate from 82.85%
-- to ~97%. Two replaces, no macro — minimal entity surface.

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
    nullif(risk_level, '')          as risk_level,
    archive_recall,
    active_notice,
    related_to_outbreak,
    nullif(
        replace(replace(establishment, '&#039;', E'\''), '&amp;', '&'),
        ''
    )                               as establishment,
    nullif(recall_reason, '')       as recall_reason,
    nullif(processing, '')          as processing,
    nullif(states, '')              as states,
    nullif(summary, '')             as summary,
    nullif(product_items, '')       as product_items,
    nullif(distro_list, '')         as distro_list,
    nullif(labels, '')              as labels,
    nullif(qty_recovered, '')       as qty_recovered,
    nullif(recall_url, '')          as url,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
  and langcode = 'English'
