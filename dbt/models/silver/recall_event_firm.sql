{{ config(materialized='table') }}

-- Many-to-many association between recall events and firms with role (ADR 0002).
-- CPSC: firms extracted from three JSONB arrays per event (manufacturer,
--   importer, distributor roles). Retailers are excluded (Option B,
--   consolidation §3) — they live in recall_event.sales_channel_narrative.
-- FDA: single scalar firm per product row (firm_legal_nam), always
--   'establishment' role — `firm_legal_nam` is the recalling FDA-registered
--   establishment, analogous to USDA's establishment (relabeled per
--   implementation_plan.md §445 follow-up #5; prior: 'manufacturer'). DISTINCT
--   ON recall_event_id prevents duplicating the same firm across multiple
--   products in the same event.
-- USDA: free-text establishment (FSIS-regulated facility), role='establishment'.
-- NHTSA: filer/manufacturer split (consolidation §3) — two bridge rows per
--   recall: mfgname role 'filer' + mfgtxt role 'manufacturer'. Multiple bronze
--   rows per campno (one per recall component); DISTINCT collapses to one
--   bridge row per (campno, firm, role).
-- USCG: firm anchor is coalesce(directory.company_name, recalls.company_name,
--   recalls.mic) per Phase 5d Step 7 directory enrichment. Always
--   'manufacturer' role. Finding S null-firm-anchor rows (~23 records, both
--   mic AND company_name NULL AND no directory match) are filtered out via
--   the WHERE clause; they appear in recall_event but are absent from this
--   bridge. The firm_id computation MUST stay in lockstep with firm.sql's
--   uscg_normalized CTE — both use the same case-insensitive LEFT JOIN to
--   stg_uscg_manufacturers on upper(trim(mic)) and the same 3-way coalesce
--   priority. Any divergence causes recall_event_firm.firm_id orphans
--   against firm.firm_id (1549 orphans observed 2026-05-30 before this
--   alignment was applied).

with cpsc_firms as (
    select source_recall_id, 'manufacturer' as role,
           jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) as firm_json
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
    -- Filer/manufacturer split (consolidation §3): two bridge rows per recall —
    -- mfgname as 'filer' (the entity that filed the recall) and mfgtxt as
    -- 'manufacturer' (the product manufacturer); 95.9% disjoint when differing.
    -- DISTINCT collapses the many-rows-per-campno (one per component) to one
    -- row per (campno, firm, role). Both firm_ids resolve in firm.sql's
    -- nhtsa_normalized (which now emits both mfgname and mfgtxt) — lockstep.
    select distinct
        md5('NHTSA' || '|' || campno)           as recall_event_id,
        md5(upper(trim(mfgname)))               as firm_id,
        'filer'                                 as role
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgname is not null
      and trim(mfgname) <> ''
    union all
    select distinct
        md5('NHTSA' || '|' || campno)           as recall_event_id,
        md5(upper(trim(mfgtxt)))                as firm_id,
        'manufacturer'                          as role
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgtxt is not null
      and trim(mfgtxt) <> ''
),

uscg_event_firms as (
    -- Three filters: (1) coalesce(directory.company_name, recalls.company_name,
    -- recalls.mic) IS NOT NULL drops the ~23 Finding S null-firm-anchor rows
    -- that don't recover via the directory either; (2) announced_at IS NOT
    -- NULL mirrors the recall_event filter so this bridge does not produce
    -- orphan recall_event_id rows (required for the relationships test on
    -- recall_event_firm.recall_event_id). The firm_id computation mirrors
    -- firm.sql's uscg_normalized CTE EXACTLY — directory enrichment via
    -- case-insensitive LEFT JOIN on mic, then 3-way coalesce. Keep these
    -- two CTEs in lockstep.
    select distinct
        md5('USCG' || '|' || r.source_recall_id)                                   as recall_event_id,
        md5(upper(trim(coalesce(m.company_name, r.company_name, r.mic))))          as firm_id,
        'manufacturer'                                                             as role
    from {{ ref('stg_uscg_recalls') }} r
    left join {{ ref('stg_uscg_manufacturers') }} m
        on upper(trim(r.mic)) = upper(trim(m.mic))
    where coalesce(m.company_name, r.company_name, r.mic) is not null
      and trim(coalesce(m.company_name, r.company_name, r.mic)) <> ''
      and r.announced_at is not null
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
