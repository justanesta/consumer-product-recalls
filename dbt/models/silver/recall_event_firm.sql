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

-- match_confidence (Phase 6b substrate, PR 6b.0): the firm-resolution path/quality
-- for each bridge row. Defaults to 'exact_name'; per-source PRs overwrite it —
-- 6b.1 (CPSC suffix-strip / DBA-extract), 6b.2 (USDA disambiguation signals),
-- 6b.4 (RapidFuzz tiers), 6b.5 (USCG MIC time-sensitivity). The shared
-- accepted_values vocabulary (severity=warn) is single-homed in _silver.yml. Keep
-- the column in EVERY branch so the union stays width-aligned — lockstep with
-- firm.sql applies to the whole column SET, not just firm_id.

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
        role,
        'exact_name'                            as match_confidence,
        cast(null as text)                      as establishment_number
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
        'establishment'                             as role,
        'exact_name'                                as match_confidence,
        cast(null as text)                          as establishment_number
    from {{ ref('stg_fda_recalls') }}
    where firm_legal_nam is not null
      and trim(firm_legal_nam) <> ''
),

usda_event_firms as (
    -- Phase 6b PR 6b.2: establishment_number + the disambiguation match_confidence come from
    -- recall_event_establishment_resolution (per recall_event_id). firm_id is UNCHANGED (md5 of
    -- the name) — the resolution is bridge-level only, so firm.sql needs no change and the firm_id
    -- lockstep is untouched. match_confidence falls back to 'exact_name' for the ~4 name-no-match
    -- recalls that have no resolution row.
    select distinct
        md5('USDA' || '|' || r.source_recall_id)        as recall_event_id,
        md5(upper(trim(r.establishment)))               as firm_id,
        'establishment'                                 as role,
        coalesce(res.match_confidence, 'exact_name')    as match_confidence,
        res.establishment_number                        as establishment_number
    from {{ ref('stg_usda_fsis_recalls') }} r
    left join {{ ref('recall_event_establishment_resolution') }} res
        on res.recall_event_id = md5('USDA' || '|' || r.source_recall_id)
    where r.establishment is not null
      and trim(r.establishment) <> ''
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
        'filer'                                 as role,
        'exact_name'                            as match_confidence,
        cast(null as text)                      as establishment_number
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgname is not null
      and trim(mfgname) <> ''
    union all
    select distinct
        md5('NHTSA' || '|' || campno)           as recall_event_id,
        md5(upper(trim(mfgtxt)))                as firm_id,
        'manufacturer'                          as role,
        'exact_name'                            as match_confidence,
        cast(null as text)                      as establishment_number
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
        'manufacturer'                                                             as role,
        'exact_name'                                                               as match_confidence,
        cast(null as text)                                                         as establishment_number
    from {{ ref('stg_uscg_recalls') }} r
    left join {{ ref('stg_uscg_manufacturers') }} m
        on upper(trim(r.mic)) = upper(trim(m.mic))
    where coalesce(m.company_name, r.company_name, r.mic) is not null
      and trim(coalesce(m.company_name, r.company_name, r.mic)) <> ''
      and r.announced_at is not null
),

-- Phase 6b PR 6b.1 (Increment B): map each branch's raw firm_id (md5(upper(trim(name))))
-- to its canonical via enrichment.firm_crosswalk and overlay the resolution
-- match_confidence. ONE outer join over the union (NOT per-branch). DISTINCT ON keeps the
-- (recall_event_id, firm_id, role) grain when two raw firms in one event collapse to a
-- single canonical. KEEP IN LOCKSTEP with firm.sql's `resolved` CTE — both join
-- firm_crosswalk on the raw firm_id and coalesce to canonical_firm_id, or firm_id orphans
-- appear against firm.firm_id (the relationships test).
unioned as (
    select * from cpsc_event_firms
    union all
    select * from fda_event_firms
    union all
    select * from usda_event_firms
    union all
    select * from nhtsa_event_firms
    union all
    select * from uscg_event_firms
),
mapped as (
    select
        u.recall_event_id,
        coalesce(x.canonical_firm_id, u.firm_id)         as firm_id,
        u.role,
        -- Precedence: a source-specific resolution (USDA's 'usda_*' from 6b.2) beats the CPSC
        -- crosswalk path; the crosswalk fills in only where the branch left the default 'exact_name'
        -- (matters for the rare CPSC<->USDA shared normalized name).
        coalesce(nullif(u.match_confidence, 'exact_name'), x.match_confidence, u.match_confidence) as match_confidence,
        u.establishment_number
    from unioned u
    left join {{ source('enrichment', 'firm_crosswalk') }} x
        on x.firm_id = u.firm_id
)

select distinct on (recall_event_id, firm_id, role)
    recall_event_id,
    firm_id,
    role,
    match_confidence,
    establishment_number
from mapped
order by recall_event_id, firm_id, role, match_confidence, establishment_number nulls last
