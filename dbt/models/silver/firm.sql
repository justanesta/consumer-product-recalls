{{ config(materialized='table') }}

-- Firm dimension (ADR 0002). Deduped by normalized (upper-trimmed) name.
-- CPSC contributes firms from three JSONB arrays (manufacturers, importers,
-- distributors) with structured {name, company_id} objects. Retailers are
-- excluded (Option B, consolidation §3) — retailer names live in
-- recall_event.sales_channel_narrative, not the firm dimension.
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
-- NHTSA contributes two firms per recall row via the filer/manufacturer split
-- (consolidation §3): mfgname (role 'filer' in the bridge) and mfgtxt (role
-- 'manufacturer'). company_id=null — NHTSA has no analog to FDA's firmfeinum.
-- The 'AC DELCO' vs 'ACDELCO' drift class (ADR 0031) currently produces two
-- firm rows; reconciliation is Phase 6 RapidFuzz work per ADR 0002.
-- USCG contributes a directory-enriched firm anchor (Phase 5d Step 7),
-- always 'manufacturer' role. raw_name preference: directory.company_name
-- > recalls.company_name > recalls.mic. company_id = recalls.mic (the
-- structured Manufacturer Identification Code). LEFT JOIN to
-- stg_uscg_manufacturers via mic supplies the canonical USCG-registered
-- name when available, rescuing the ~10 mic-only-no-name recall rows
-- (Phase 6a USCG audit §3 Bug 3). Finding S null-anchor rows (both mic AND
-- company_name AND directory NULL) are filtered out — they never reach
-- the firm dimension.
-- Matching by normalized_name enables implicit cross-source firm deduplication:
-- a firm that appears in multiple sources with the same normalized name will
-- collapse to a single row with all company IDs in observed_company_ids.

with cpsc_firms as (
    select 'manufacturer' as role,
           jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) as firm_json
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
    -- Filer/manufacturer split (consolidation §3): mfgname is the entity that
    -- FILED the recall with NHTSA, mfgtxt the actual product manufacturer —
    -- 95.9% disjoint when they differ, so both belong in the firm dimension.
    -- company_id=null (no analog to FDA's firm_fei_num). role is vestigial in
    -- this model (the dim groups by normalized_name); it drives the bridge.
    select distinct
        'filer'                     as role,
        mfgname                     as raw_name,
        upper(trim(mfgname))        as normalized_name,
        cast(null as text)          as company_id
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgname is not null
      and trim(mfgname) <> ''
    union all
    select distinct
        'manufacturer'              as role,
        mfgtxt                      as raw_name,
        upper(trim(mfgtxt))         as normalized_name,
        cast(null as text)          as company_id
    from {{ ref('stg_nhtsa_recalls') }}
    where mfgtxt is not null
      and trim(mfgtxt) <> ''
),

uscg_normalized as (
    -- USCG firm anchor coalesces (in priority order):
    --   1. directory.company_name — USCG-registered canonical name from
    --      the manufacturer directory (Phase 5d Step 7 enrichment).
    --   2. recalls.company_name — per-recall name as scraped at recall
    --      time (may be stale relative to the live directory).
    --   3. recalls.mic — last resort when neither directory nor recalls
    --      has a name (Finding S null-anchor rows that still have a MIC).
    -- company_id stays mic — the LEFT JOIN does not change the structured
    -- ID, only adds a richer raw_name source.
    --
    -- The LEFT JOIN serves the Phase 6a USCG audit §3 Bug 3 rescue: the
    -- ~10 mic-only-no-name recall rows resolve to their canonical directory
    -- name (e.g., recalls mic='YDV' + NULL company_name → directory
    -- company_name 'YAMAHA DEALER VENTURES'). It also performs general
    -- firm-name canonicalization across the corpus (recalls company_name
    -- was scraped at recall time and may be stale; directory is the live
    -- USCG registry of record).
    --
    -- Coverage caveat: ~93.2% of recalls have populated mic (per Finding S);
    -- recalls without a mic cannot join the directory and fall through to
    -- recalls.company_name (or null anchor) as before.
    --
    -- Case-insensitive JOIN per Step 3 corpus-validation finding (2026-05-30):
    -- USCG recalls bronze contains 7 distinct lowercase MICs (cec, blb, kis,
    -- lbb, ser, vky, zep) that have uppercase matches in the directory.
    -- The case-sensitive equality `r.mic = m.mic` missed these and they
    -- showed up as Q3b orphans in measure_rescue_and_coverage.sql at 98.48%
    -- match rate. Aligning the JOIN with the USDA precedent
    -- (firm.sql line 82-83: `upper(trim(r.establishment)) =
    -- upper(trim(e.establishment_name))`) recovers them — coverage rises
    -- to ~99.4% and the rescue count from 5 to ~12.
    select distinct
        'manufacturer'                                          as role,
        coalesce(m.company_name, r.company_name, r.mic)         as raw_name,
        upper(trim(coalesce(m.company_name, r.company_name, r.mic)))
                                                                as normalized_name,
        r.mic                                                   as company_id
    from {{ ref('stg_uscg_recalls') }} r
    left join {{ ref('stg_uscg_manufacturers') }} m
        on upper(trim(r.mic)) = upper(trim(m.mic))
    where coalesce(m.company_name, r.company_name, r.mic) is not null
      and trim(coalesce(m.company_name, r.company_name, r.mic)) <> ''
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
),

-- Phase 6b PR 6b.1 (Increment B): resolve each raw firm to its canonical (cleaned —
-- later clustered) id via enrichment.firm_crosswalk, keyed by md5(normalized_name).
-- Firms with no crosswalk row are their own canonical (the coalesce — non-CPSC today).
-- KEEP IN LOCKSTEP with recall_event_firm.sql's `mapped` CTE: both map the raw firm_id
-- -> canonical_firm_id via the SAME crosswalk join, or recall_event_firm.firm_id
-- orphans against firm.firm_id (the relationships test).
resolved as (
    select
        an.raw_name,
        an.normalized_name,
        an.company_id,
        coalesce(x.canonical_firm_id, md5(an.normalized_name)) as canonical_firm_id,
        coalesce(x.canonical_name, an.raw_name)                as resolved_name,
        x.extracted_dba
    from all_normalized an
    left join {{ source('enrichment', 'firm_crosswalk') }} x
        on x.firm_id = md5(an.normalized_name)
)

select
    canonical_firm_id                                                 as firm_id,
    upper(trim((array_agg(resolved_name order by resolved_name))[1])) as normalized_name,
    (array_agg(resolved_name order by resolved_name))[1]             as canonical_name,
    jsonb_agg(distinct raw_name)                                     as observed_names,
    jsonb_agg(distinct company_id)
        filter (where company_id is not null)                       as observed_company_ids,
    -- alternate_names (Phase 6b): DBA brands / surface-form aliases from the crosswalk
    -- (CPSC extracted_dba today; later sources add theirs). NULL when none.
    jsonb_agg(distinct extracted_dba)
        filter (where extracted_dba is not null)                    as alternate_names
from resolved
group by canonical_firm_id
