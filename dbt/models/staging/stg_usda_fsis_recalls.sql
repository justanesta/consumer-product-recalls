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
--
-- trim(source_recall_id) per Finding R in recall_api_observations.md: 5 of
-- 1215 distinct source_recall_id values in bronze have leading or trailing
-- whitespace (e.g., ` 021-2020 `, `007-2020-EXP `). Per ADR 0027, bronze
-- preserves source-verbatim values; this staging layer normalizes the
-- whitespace so downstream silver/gold and any future Phase 6 firm-resolution
-- join uses clean keys. trim() applied in both the partition-by (so the
-- latest-version window groups all snapshots of a recall under one key) and
-- the select projection (so downstream consumers see the clean form).

with ranked as (
    select
        *,
        row_number() over (
            partition by trim(source_recall_id), langcode
            order by extraction_timestamp desc
        ) as rn
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
)

select
    trim(source_recall_id)          as source_recall_id,
    title,
    recall_date::timestamptz        as announced_at,
    last_modified_date::timestamptz as published_at,
    closed_date::timestamptz        as closed_at,
    -- 2026-06 USDA API change (Finding S): closed_date (full date) is no longer returned;
    -- closed_year (year only) is. Project it so recall_event can carry a year-grain close
    -- (terminated_year) when the precise date is gone, rather than fabricating a date.
    nullif(closed_year, '')         as closed_year,
    recall_classification           as classification,
    recall_type,
    nullif(risk_level, '')          as risk_level,
    archive_recall,
    active_notice,
    related_to_outbreak,
    -- 2026-06 USDA API change (migration 0028 / Finding S): these fields are now jsonb
    -- arrays in bronze. jsonb_array_to_csv() collapses them back to the comma-joined text
    -- the current downstream silver contract expects (nullif then normalizes empty→null,
    -- same as before). Exploiting the native arrays is deferred follow-up. summary,
    -- qty_recovered, risk_level, url stayed scalar and are unchanged.
    nullif(
        replace(
            replace({{ jsonb_array_to_csv('establishment') }}, '&#039;', E'\''),
            '&amp;', '&'
        ),
        ''
    )                               as establishment,
    nullif({{ jsonb_array_to_csv('recall_reason') }}, '')  as recall_reason,
    nullif({{ jsonb_array_to_csv('processing') }}, '')     as processing,
    nullif({{ jsonb_array_to_csv('states') }}, '')         as states,
    nullif(summary, '')             as summary,
    nullif({{ jsonb_array_to_csv('product_items') }}, '')  as product_items,
    nullif({{ jsonb_array_to_csv('distro_list') }}, '')    as distro_list,
    nullif({{ jsonb_array_to_csv('labels') }}, '')         as labels,
    nullif(qty_recovered, '')       as qty_recovered,
    -- W4 Phase A: → recall_event.firm_contact_block_text
    nullif({{ jsonb_array_to_csv('company_media_contact') }}, '') as company_media_contact,
    nullif(recall_url, '')          as url,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
  and langcode = 'English'
