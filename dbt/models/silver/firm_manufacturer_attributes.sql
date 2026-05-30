{{ config(materialized='table') }}

-- USCG-registered boat-manufacturer attributes — directory metadata that
-- doesn't fit on firm.sql (which is keyed on normalized name and shared
-- across CPSC/FDA/USDA/NHTSA/USCG). Phase 5d Step 7 Step 5, sibling to
-- firm_establishment_attributes.sql for USDA.
--
-- One row per MIC (the regulatory canonical id, which is also the column
-- populated as company_id on USCG firms in firm.sql). Records with null
-- source_recall_id are excluded — they can't be joined back to a firm so
-- they have no place in this dim.
--
-- Source: stg_uscg_manufacturers (the new Phase 5d Step 7 staging view).
-- The recall side has no analogous structured directory fields beyond mic
-- + company_name; firm.sql remains keyed on normalized name for cross-source
-- dedup, and this dim sits alongside it. Mirror of the USDA approach
-- (firm dim + per-source attribute sibling).

select
    source_recall_id            as mic,
    company_name,
    address,
    city,
    state,
    uscg_directory_id,
    detail_url
from {{ ref('stg_uscg_manufacturers') }}
where source_recall_id is not null
