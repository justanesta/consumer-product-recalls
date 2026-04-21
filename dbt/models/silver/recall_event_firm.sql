{{ config(materialized='table') }}

-- Many-to-many association between recall events and firms with role (ADR 0002).

with all_firms as (
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
)

select distinct
    md5('CPSC' || '|' || source_recall_id)       as recall_event_id,
    md5(upper(trim(firm_json ->> 'name')))       as firm_id,
    role
from all_firms
where (firm_json ->> 'name') is not null
  and trim(firm_json ->> 'name') <> ''
