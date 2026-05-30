{{ config(materialized='view') }}

-- Latest-per-MIC projection over the USCG manufacturer-directory bronze table.
-- Bronze may contain multiple rows per source_recall_id (= MIC) when content
-- changes — content-hash dedup with hash_exclude_fields={detail_url,
-- uscg_directory_id} prevents identical re-ingestion, but genuine field edits
-- (Address change, Company rename, State move) produce new rows. Silver
-- consumes only the most recent version per source_recall_id.
--
-- Normalizations applied here per ADR 0027 (bronze stores verbatim; silver
-- normalizes):
--   - Finding F.3 — three distinct missing-data sentinels in the source:
--     literal 'UNK' (city/address — e.g. MIC YCP / YU CHING), literal '-'
--     (company/address/city when redacted/withdrawn — e.g. MIC YCT), and
--     empty string '' (state for some Canadian rows). All map to NULL via
--     CASE WHEN checks; bronze keeps verbatim per ADR 0027.
--   - ADR 0027 — empty-string → NULL via nullif(col, '') wrappers, same
--     shape as USCG-recalls staging.
--   - No casing normalization — company_name preserves source casing
--     (Finding F.5 — mixed Title/UPPER across the corpus); firm.sql does
--     upper(trim()) for the cross-source join key.
--   - Address embedded newlines (Finding F.2 — HONDA row example) are
--     preserved verbatim; downstream consumers that need single-line
--     display call replace(address, chr(10), ' ').
--   - Listing addresses are truncated at ~30 chars at source (Finding F.1
--     VARCHAR constraint) — documented limitation; not recoverable here.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from {{ source('uscg_manufacturers', 'uscg_manufacturers_bronze') }}
)

select
    source_recall_id,
    source_recall_id                                        as mic,

    -- Finding F.3 — UNK / dash / empty-string sentinels → NULL.
    case
        when company_name is null then null
        when company_name in ('-', 'UNK', '') then null
        else company_name
    end                                                     as company_name,

    case
        when address is null then null
        when address in ('-', 'UNK', '') then null
        else address
    end                                                     as address,

    case
        when city is null then null
        when city in ('-', 'UNK', '') then null
        else city
    end                                                     as city,

    -- Finding G — state cell is empty string for non-US (Canadian) rows
    -- with no province code present. Empty string is the only sentinel
    -- observed for State (no 'UNK' / '-' seen in Step 1 probes).
    nullif(state, '')                                       as state,

    uscg_directory_id,
    detail_url,

    -- Lineage / audit.
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
