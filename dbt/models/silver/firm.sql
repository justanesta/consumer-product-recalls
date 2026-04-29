{{ config(materialized='table') }}

-- Firm dimension (ADR 0002). Deduped by normalized (upper-trimmed) name.
-- CPSC contributes firms from four JSONB arrays (manufacturers, retailers, importers,
-- distributors) with structured {name, company_id} objects.
-- FDA contributes a single scalar firm per product row (firm_legal_nam + firm_fei_num),
-- always in the 'manufacturer' role. DISTINCT prevents duplicating the same firm
-- across multiple products in the same recall event.
-- Matching by normalized_name enables implicit cross-source firm deduplication:
-- a firm that appears in both CPSC and FDA data with the same normalized name will
-- collapse to a single row with both company IDs in observed_company_ids.

with cpsc_firms as (
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

cpsc_normalized as (
    select
        role,
        firm_json ->> 'name'              as raw_name,
        upper(trim(firm_json ->> 'name')) as normalized_name,
        firm_json ->> 'company_id'        as company_id
    from cpsc_firms
    where (firm_json ->> 'name') is not null
      and trim(firm_json ->> 'name') <> ''
),

fda_normalized as (
    select distinct
        'manufacturer'                as role,
        firm_legal_nam                as raw_name,
        upper(trim(firm_legal_nam))   as normalized_name,
        firm_fei_num::text            as company_id
    from {{ ref('stg_fda_recalls') }}
    where firm_legal_nam is not null
      and trim(firm_legal_nam) <> ''
),

all_normalized as (
    select * from cpsc_normalized
    union all
    select * from fda_normalized
)

select
    md5(normalized_name)                      as firm_id,
    normalized_name,
    (array_agg(raw_name order by raw_name))[1] as canonical_name,
    jsonb_agg(distinct raw_name)              as observed_names,
    jsonb_agg(distinct company_id)
        filter (where company_id is not null) as observed_company_ids
from all_normalized
group by normalized_name
