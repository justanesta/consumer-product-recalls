{{ config(materialized='table') }}

-- Many-to-many association between recall events and firms with role (ADR 0002).
-- CPSC: firms extracted from four JSONB arrays per event (manufacturer, retailer,
--   importer, distributor roles).
-- FDA: single scalar firm per product row (firm_legal_nam), always
--   'establishment' role — `firm_legal_nam` is the recalling FDA-registered
--   establishment, analogous to USDA's establishment (relabeled per
--   implementation_plan.md §445 follow-up #5; prior: 'manufacturer'). DISTINCT
--   ON recall_event_id prevents duplicating the same firm across multiple
--   products in the same event.
-- USDA: free-text establishment (FSIS-regulated facility), role='establishment'.
-- NHTSA: single scalar firm per recall row (mfgname), always 'manufacturer' role.
--   Multiple bronze rows per campno (one per recall component); DISTINCT
--   collapses to one bridge row per (campno, mfgname) — typically one firm
--   per recall, occasionally co-recalls with multiple manufacturers.
-- USCG: firm anchor is coalesce(mic, company_name) per Finding S — the
--   Manufacturer Industry Code (MIC) is the structured firm identifier, with
--   company_name as a fallback. Always 'manufacturer' role. Finding S
--   null-firm-anchor rows (~23 records, both mic AND company_name NULL) are
--   filtered out via the WHERE clause; they appear in recall_event but are
--   absent from this bridge. Phase 6 may revisit with a synthetic-anchor
--   strategy if cross-source firm rollups require placeholder rows.

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
    -- FDA's `firm_legal_nam` is the recalling establishment, not a
    -- manufacturer. Relabeled per implementation_plan.md §445 follow-up #5.
    select distinct
        md5('FDA' || '|' || recall_event_id::text) as recall_event_id,
        md5(upper(trim(firm_legal_nam)))            as firm_id,
        'establishment'                             as role
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
),

nhtsa_event_firms as (
    select distinct
        md5('NHTSA' || '|' || campno)           as recall_event_id,
        md5(upper(trim(mfgname)))               as firm_id,
        'manufacturer'                          as role
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgname is not null
      and trim(mfgname) <> ''
),

uscg_event_firms as (
    -- Two filters: (1) coalesce(mic, company_name) IS NOT NULL drops the
    -- ~23 Finding S null-firm-anchor rows; (2) announced_at IS NOT NULL
    -- mirrors the recall_event filter so this bridge does not produce
    -- orphan recall_event_id rows. The latter is required for the
    -- relationships test on recall_event_firm.recall_event_id.
    select distinct
        md5('USCG' || '|' || source_recall_id)               as recall_event_id,
        md5(upper(trim(coalesce(mic, company_name))))        as firm_id,
        'manufacturer'                                       as role
    from {{ ref('stg_uscg_recalls') }}
    where coalesce(mic, company_name) is not null
      and trim(coalesce(mic, company_name)) <> ''
      and announced_at is not null
)

select * from cpsc_event_firms
union all
select * from fda_event_firms
union all
select * from usda_event_firms
union all
select * from nhtsa_event_firms
union all
select * from uscg_event_firms
