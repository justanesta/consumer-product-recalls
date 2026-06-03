{{ config(materialized='view') }}

-- Latest-per-product projection over the FDA iRES bronze table.
-- Bronze may contain multiple rows per source_recall_id (PRODUCTID) when content
-- changes — content-hash dedup prevents identical re-ingestion, but genuine edits
-- (e.g., phase transitions, classification updates) produce new rows. Silver
-- consumes only the most recent version per product. ROW_NUMBER is equivalent to
-- MAX(extraction_timestamp) but avoids a self-join.
--
-- nullif(col, '') wrappers per ADR 0027: bronze preserves the source's mixed
-- null/'' representation verbatim (Finding J — FDA uses both for the same fields
-- across records). Silver normalizes empty strings to null so downstream
-- consumers don't have to remember the dance.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id
            order by extraction_timestamp desc
        ) as rn
    from {{ source('fda', 'fda_recalls_bronze') }}
)

select
    source_recall_id,
    recall_event_id,
    rid,
    center_cd,
    product_type_short,
    event_lmd,
    firm_legal_nam,
    firm_fei_num,
    nullif(recall_num, '')                     as recall_num,
    nullif(phase_txt, '')                      as phase_txt,
    nullif(center_classification_type_txt, '') as center_classification_type_txt,
    recall_initiation_dt,
    center_classification_dt,
    termination_dt,
    enforcement_report_dt,
    determination_dt,
    nullif(initial_firm_notification_txt, '')  as initial_firm_notification_txt,
    nullif(distribution_area_summary_txt, '')  as distribution_area_summary_txt,
    nullif(voluntary_type_txt, '')             as voluntary_type_txt,
    nullif(product_description_txt, '')        as product_description_txt,
    nullif(product_short_reason_txt, '')       as product_short_reason_txt,
    nullif(product_distributed_quantity, '')   as product_distributed_quantity,
    -- Capture-expansion (b) PR W1: firm address/continuity fields landed by
    -- migration 0019, consumed by firm_fda_attributes (keyed on firm_fei_num).
    -- nullif per ADR 0027 (Finding J — FDA uses both null and '' for these).
    -- firm_legal_nam / firm_fei_num are already projected above.
    nullif(firm_city_nam, '')                  as firm_city_nam,
    nullif(firm_country_nam, '')               as firm_country_nam,
    nullif(firm_line1_adr, '')                 as firm_line1_adr,
    nullif(firm_line2_adr, '')                 as firm_line2_adr,
    nullif(firm_postal_cd, '')                 as firm_postal_cd,
    nullif(firm_state_cd, '')                  as firm_state_cd,
    nullif(firm_state_prvnc_nam, '')           as firm_state_prvnc_nam,
    nullif(firm_surviving_nam, '')             as firm_surviving_nam,
    firm_surviving_fei,
    -- W1b: first public-posting date → recall_event.first_posted_at. No nullif —
    -- it's a proper date/null in bronze (not an '' sentinel).
    posted_internet_dt,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
