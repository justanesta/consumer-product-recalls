{{ config(materialized='table') }}

-- Firm dimension (ADR 0002). Deduped by normalized (upper-trimmed) name.
-- CPSC contributes firms from four JSONB arrays (manufacturers, retailers, importers,
-- distributors) with structured {name, company_id} objects.
-- FDA contributes a single scalar firm per product row (firm_legal_nam + firm_fei_num),
-- always in the 'establishment' role — `firm_legal_nam` is semantically the
-- recalling FDA-registered establishment, analogous to USDA's establishment
-- field (relabeled per implementation_plan.md §445 architectural follow-up #5;
-- prior versions of this model used role='manufacturer'). DISTINCT prevents
-- duplicating the same firm across multiple products in the same recall event.
-- USDA contributes a free-text 'establishment' (recalling FSIS-regulated facility)
-- with role='establishment'. company_id is populated via a LEFT JOIN against
-- stg_usda_fsis_establishments matching on normalized establishment_name —
-- Phase 5b.2 Step 5; covers ~97% of distinct recall names per
-- documentation/usda/establishment_join_coverage.md (HTML-entity decode applied
-- on the recall side in stg_usda_fsis_recalls.sql lifts the rate from 82.85%).
-- Names with no FSIS match keep company_id=null and are unaffected by the join.
-- NHTSA contributes a single scalar firm per recall row (mfgname), always
-- 'manufacturer' role. company_id=null — NHTSA has no analog to FDA's firmfeinum.
-- The 'AC DELCO' vs 'ACDELCO' drift class (ADR 0031) currently produces two
-- firm rows; reconciliation is Phase 6 RapidFuzz work per ADR 0002.
-- USCG contributes coalesce(mic, company_name) as the firm anchor, always
-- 'manufacturer' role. company_id = mic when populated (USCG's structured
-- Manufacturer Industry Code), falling back to null when only company_name
-- exists. Finding S null-anchor rows (both mic AND company_name NULL) are
-- filtered out — they never reach the firm dimension.
-- Matching by normalized_name enables implicit cross-source firm deduplication:
-- a firm that appears in multiple sources with the same normalized name will
-- collapse to a single row with all company IDs in observed_company_ids.

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
    -- FDA's `firm_legal_nam` is semantically the recalling establishment
    -- (analogous to USDA's `establishment` field), not a manufacturer.
    -- Relabeled per implementation_plan.md §445 architectural follow-up #5.
    select distinct
        'establishment'               as role,
        firm_legal_nam                as raw_name,
        upper(trim(firm_legal_nam))   as normalized_name,
        firm_fei_num::text            as company_id
    from {{ ref('stg_fda_recalls') }}
    where firm_legal_nam is not null
      and trim(firm_legal_nam) <> ''
),

usda_normalized as (
    select distinct
        'establishment'                as role,
        r.establishment                as raw_name,
        upper(trim(r.establishment))   as normalized_name,
        e.establishment_number         as company_id
    from {{ ref('stg_usda_fsis_recalls') }} r
    left join {{ ref('stg_usda_fsis_establishments') }} e
        on upper(trim(r.establishment)) = upper(trim(e.establishment_name))
    where r.establishment is not null
      and trim(r.establishment) <> ''
),

nhtsa_normalized as (
    select distinct
        'manufacturer'              as role,
        mfgname                     as raw_name,
        upper(trim(mfgname))        as normalized_name,
        cast(null as text)          as company_id
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgname is not null
      and trim(mfgname) <> ''
),

uscg_normalized as (
    -- Firm anchor = coalesce(mic, company_name) per Finding S. company_id
    -- is mic when populated (USCG's structured Manufacturer Industry Code,
    -- a 3-character alpha identifier), null otherwise.
    select distinct
        'manufacturer'                                  as role,
        coalesce(mic, company_name)                     as raw_name,
        upper(trim(coalesce(mic, company_name)))        as normalized_name,
        mic                                             as company_id
    from {{ ref('stg_uscg_recalls') }}
    where coalesce(mic, company_name) is not null
      and trim(coalesce(mic, company_name)) <> ''
),

all_normalized as (
    select * from cpsc_normalized
    union all
    select * from fda_normalized
    union all
    select * from usda_normalized
    union all
    select * from nhtsa_normalized
    union all
    select * from uscg_normalized
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
