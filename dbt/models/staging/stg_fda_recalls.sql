{{ config(materialized='view') }}

-- Latest-per-product projection over the FDA iRES bronze table.
-- Bronze may contain multiple rows per source_recall_id (PRODUCTID) when content
-- changes — content-hash dedup prevents identical re-ingestion, but genuine edits
-- (e.g., phase transitions, classification updates) produce new rows. Silver
-- consumes only the most recent version per product. ROW_NUMBER is equivalent to
-- MAX(extraction_timestamp) but avoids a self-join.

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
    recall_num,
    phase_txt,
    center_classification_type_txt,
    recall_initiation_dt,
    center_classification_dt,
    termination_dt,
    enforcement_report_dt,
    determination_dt,
    initial_firm_notification_txt,
    distribution_area_summary_txt,
    voluntary_type_txt,
    product_description_txt,
    product_short_reason_txt,
    product_distributed_quantity,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
