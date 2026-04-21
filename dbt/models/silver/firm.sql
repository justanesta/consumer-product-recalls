{{ config(materialized='table') }}

-- Firm dimension (ADR 0002). Deduped by normalized (upper-trimmed) name.
-- Phase 6 will add cross-source fuzzy resolution; for now CPSC-only.

with all_firms as (
    select 'manufacturer' as role,
           jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
    union all
    select 'retailer' as role,
           jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
    union all
    select 'importer' as role,
           jsonb_array_elements(coalesce(importers, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
    union all
    select 'distributor' as role,
           jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) as firm_json
    from {{ ref('stg_cpsc_recalls') }}
),

normalized as (
    select
        role,
        firm_json ->> 'name'                              as raw_name,
        upper(trim(firm_json ->> 'name'))                 as normalized_name,
        firm_json ->> 'company_id'                        as company_id
    from all_firms
    where (firm_json ->> 'name') is not null
      and trim(firm_json ->> 'name') <> ''
)

select
    md5(normalized_name)                      as firm_id,
    normalized_name,
    (array_agg(raw_name order by raw_name))[1] as canonical_name,
    jsonb_agg(distinct raw_name)              as observed_names,
    jsonb_agg(distinct company_id)
        filter (where company_id is not null) as observed_company_ids
from normalized
group by normalized_name
