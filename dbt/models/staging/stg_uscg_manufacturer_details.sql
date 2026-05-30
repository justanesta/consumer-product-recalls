{{ config(materialized='view') }}

-- Latest-per-MIC projection over the USCG manufacturer DETAIL bronze table
-- (Phase 5d Step 7 detail / Path B). Sibling to stg_uscg_manufacturers.sql;
-- this carries the detail-page fields the listing source lacks: the succession
-- lineage (past_company_1/2/3, parent_company/parent_mic), the FULL untruncated
-- address (vs the listing's ~30-char truncation, Finding F.1), contact fields,
-- status, and the M/D/YYYY dates (in_business / out_of_business / date_modified).
--
-- Normalizations (ADR 0027 — bronze stores verbatim, silver normalizes):
--   - Finding F.3 sentinels 'UNK' / '-' / '' → NULL across text fields.
--   - company_official additionally uses the '-, -' sentinel ("no official
--     recorded") → NULL.
--   - empty-string state (Canadian rows, Finding G) → NULL via nullif.
--   - Dates are already coerced to TIMESTAMPTZ at bronze (M/D/YYYY
--     BeforeValidator); passed through unchanged.
--
-- VALUE caveats for downstream (silver SCD-2 / firm dim — Phase 6 / ADR 0035):
--   - in_business is CONTAMINATED by record-touch dates on active firms
--     (MERCURY / VOLVO PENTA / CATERPILLAR show in_business ≈ date_modified ≈
--     2025/2026; defunct 4WN shows a real 1972). Never treat as a "founded"
--     date in isolation.
--   - out_of_business (top-level) = the CURRENT holder is defunct (the SCD
--     valid_to). A Past Company '(OOB)' = a PRIOR holder ceased → the MIC was
--     recycled. These are DIFFERENT signals — do NOT conflate.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from {{ source('uscg_manufacturer_details', 'uscg_manufacturer_details_bronze') }}
)

select
    source_recall_id,
    source_recall_id                                            as mic,

    -- Finding F.3 — 'UNK' / '-' / '' sentinels → NULL.
    case when company_name in ('-', 'UNK', '') then null else company_name end as company_name,
    case when dba in ('-', 'UNK', '') then null else dba end                   as dba,
    case when parent_company in ('-', 'UNK', '') then null else parent_company end
                                                                as parent_company,
    case when parent_mic in ('-', 'UNK', '') then null else parent_mic end     as parent_mic,
    case when past_company_1 in ('-', 'UNK', '') then null else past_company_1 end
                                                                as past_company_1,
    case when past_company_2 in ('-', 'UNK', '') then null else past_company_2 end
                                                                as past_company_2,
    case when past_company_3 in ('-', 'UNK', '') then null else past_company_3 end
                                                                as past_company_3,
    case when address in ('-', 'UNK', '') then null else address end           as address,
    case when city in ('-', 'UNK', '') then null else city end                 as city,
    -- Finding G — empty-string state for Canadian rows with no province code.
    nullif(state, '')                                           as state,
    case when zip in ('-', 'UNK', '') then null else zip end                   as zip,
    case when country in ('-', 'UNK', '') then null else country end           as country,
    case when phone in ('-', 'UNK', '') then null else phone end               as phone,
    case when fax in ('-', 'UNK', '') then null else fax end                   as fax,
    case when status in ('-', 'UNK', '') then null else status end             as status,
    -- '-, -' sentinel ("no official recorded") in addition to the usual three.
    case
        when company_official in ('-', 'UNK', '', '-, -') then null
        else company_official
    end                                                         as company_official,
    case when type in ('-', 'UNK', '') then null else type end                 as type,
    case
        when additional_address in ('-', 'UNK', '') then null
        else additional_address
    end                                                         as additional_address,

    -- Dates (TIMESTAMPTZ from bronze) — passthrough.
    in_business,
    out_of_business,
    date_modified,

    -- Lineage / audit.
    uscg_directory_id,
    detail_url,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
