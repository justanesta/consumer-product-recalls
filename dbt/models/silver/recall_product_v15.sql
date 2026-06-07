{{ config(materialized='view') }}

-- Parallel v1.5 current-state view of NHTSA recall products (Phase 6c.6 Layer 2, ADR 0033). Selects
-- the snapshot's CURRENT rows (dbt_valid_to is null) and projects them into recall_product's EXACT
-- column shape, NHTSA only, so scripts/sql/nhtsa/silver/compare_v1_v15_cardinality.sql can diff it
-- against `recall_product WHERE source = 'NHTSA'`. At cutover (6c.7) this SELECT replaces
-- recall_product's nhtsa_products CTE verbatim and this model is dropped. recall_product_id here is
-- the 7-tuple md5 (ADR 0033 2026-06-06 amendment; intentionally differs from v1's 11-tuple id by
-- demoting desc/name/bgman/endman to snapshot attributes — that difference IS the migration).
-- Column order is byte-for-byte identical to recall_product's nhtsa_products CTE.

select
    recall_product_id,
    md5('NHTSA' || '|' || campno)                          as recall_event_id,
    'NHTSA'                                                 as source,
    campno                                                 as source_recall_id,
    compname                                               as product_name,
    mfr_comp_desc                                          as product_description,
    modeltxt                                               as model,
    rcltype                                                as type,
    cast(null as text)                                     as category_id,
    potaff                                                 as number_of_units,
    case when potaff ~ '^[0-9]+$' then potaff::integer end as unit_count,
    model_year                                             as model_year,
    cast(null as text)                                     as hin,
    cast(null as text)                                     as label_artifact_name,
    cast(null as text)                                     as distribution_list_artifact_name,
    cast(null as text)                                     as upc,
    jsonb_build_object(
        'maketxt',       maketxt,
        'yeartxt',       yeartxt,
        'mfgname',       mfgname,
        'mfgtxt',        mfgtxt,
        'rcl_cmpt_id',   rcl_cmpt_id,
        'mfr_comp_ptno', mfr_comp_ptno,
        'mfr_comp_name', mfr_comp_name,
        'bgman',         bgman,
        'endman',        endman,
        'fmvss',         fmvss
    )                                                      as source_specific_attrs
from {{ ref('nhtsa_recall_product_snapshot') }}
where dbt_valid_to is null
