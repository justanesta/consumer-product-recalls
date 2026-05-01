{{ config(materialized='table') }}

-- Many-to-many association between recall events and firms with role (ADR 0002).
-- CPSC: firms extracted from four JSONB arrays per event (manufacturer, retailer,
--   importer, distributor roles).
-- FDA: single scalar firm per product row (firm_legal_nam), always 'manufacturer'
--   role. DISTINCT ON recall_event_id prevents duplicating the same firm across
--   multiple products in the same event.
-- USDA: free-text establishment (FSIS-regulated facility), role='establishment'.

with cpsc_firms as (
    select source_recall_id, 'manufacturer' as role,
           jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
    union all
    select source_recall_id, 'retailer' as role,
           jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
    union all
    select source_recall_id, 'importer' as role,
           jsonb_array_elements(coalesce(importers, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
    union all
    select source_recall_id, 'distributor' as role,
           jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
),

cpsc_event_firms as (
    select distinct
        md5('CPSC' || '|' || source_recall_id)  as recall_event_id,
        md5(upper(trim(firm_json ->> 'name')))  as firm_id,
        role
    from cpsc_firms
    where (firm_json ->> 'name') is not null
      and trim(firm_json ->> 'name') <> ''
),

fda_event_firms as (
    select distinct
        md5('FDA' || '|' || recall_event_id::text) as recall_event_id,
        md5(upper(trim(firm_legal_nam)))            as firm_id,
        'manufacturer'                              as role
    from {{ ref('stg_fda_recalls') }}
    where firm_legal_nam is not null
      and trim(firm_legal_nam) <> ''
),

usda_event_firms as (
    select distinct
        md5('USDA' || '|' || source_recall_id)  as recall_event_id,
        md5(upper(trim(establishment)))         as firm_id,
        'establishment'                         as role
    from {{ ref('stg_usda_fsis_recalls') }}
    where establishment is not null
      and trim(establishment) <> ''
)

select * from cpsc_event_firms
union all
select * from fda_event_firms
union all
select * from usda_event_firms
