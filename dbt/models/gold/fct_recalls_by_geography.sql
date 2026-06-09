{{ config(
    materialized='table',
    indexes=[
      {'columns': ['geography_basis', 'source', 'state_code']},
      {'columns': ['state_code']},
    ]
) }}

-- fct_recalls_by_geography — recalls per US state, two complementary lenses (Phase 6e, ADR 0038):
--   * 'distribution'      — where the recalled product went (recall_distribution_area, FDA + USDA).
--   * 'firm_registration' — where the producing firm is REGISTERED (the per-source SCD-2 sidecars
--                           establishment/manufacturer/FDA-firm state, USDA + USCG + FDA). Renamed
--                           from 'firm_location' (C17, 2026-06-09) for honesty: this is the firm's
--                           registered/HQ address, NOT where the product was made or sold.
-- These answer different questions ("recalls affecting people in TX" vs "recalls from TX-registered
-- firms"); the geography_basis column keeps them in one model. GROUPING SETS emits both per-source
-- rows and an 'ALL' all-source rollup (source filter renders a per-source page; 'ALL' the map).
-- firm_id is the 6b CROSS-SOURCE canonical, so a CPSC/NHTSA recall whose firm shares a normalized
-- name with an FDA/USDA/USCG-registered firm INHERITS that firm's state (the firm-resolution payoff);
-- pure-CPSC/NHTSA firms with no shared structural id contribute no state.
-- State codes are constrained to the us_state_abbr seed (drops Canadian provinces / foreign codes).
--
-- MULTI-COUNTING (documented design, gold_design_notes caveat #2): a recall is counted in EVERY state
-- where any of its firms is registered. A firm with facilities registered in N states (up to 7 observed)
-- contributes the recall to all N — an "industry footprint" reading. So per-state counts SUM TO MORE
-- than the distinct-recall total (recall × firm-registered-state incidences, NOT distinct recalls per
-- state). A single-primary-state collapse (C18) was evaluated 2026-06-09 and REVERTED: 65% of
-- multi-state firms have ~1 registration per state, so it picked an essentially-arbitrary state for
-- 6.6% of recalls (evidence: scripts/sql/gold/inspect_firm_state_ties.sql). The footprint reading is
-- kept and documented instead.

with distribution_lens as (
    select
        'distribution'::text as geography_basis,
        re.source,
        code                 as state_code,
        re.recall_event_id
    from {{ ref('recall_distribution_area') }} rda
    join {{ ref('recall_event') }} re using (recall_event_id)
    cross join lateral unnest(rda.distribution_state_codes) as code
),

-- firm -> each observed structural id -> the matching sidecar's state (disjoint id namespaces).
firm_states as (
    select
        f.firm_id,
        upper(trim(s.state_code)) as state_code
    from {{ ref('firm') }} f
    cross join lateral jsonb_array_elements_text(
        coalesce(f.observed_company_ids, '[]'::jsonb)
    ) as cid(company_id)
    cross join lateral (
        select ea.state as state_code
        from {{ ref('firm_usda_attributes') }} ea where ea.establishment_id = cid.company_id
        union all
        select ma.state
        from {{ ref('firm_uscg_attributes') }} ma where ma.mic = cid.company_id
        union all
        select fa.firm_state_cd
        from {{ ref('firm_fda_attributes') }} fa where fa.firm_fei_num::text = cid.company_id
    ) s
    where s.state_code is not null and trim(s.state_code) <> ''
),

firm_states_us as (
    select distinct fs.firm_id, fs.state_code
    from firm_states fs
    join {{ ref('us_state_abbr') }} usa on usa.abbr = fs.state_code
),

firm_registration_lens as (
    select distinct
        'firm_registration'::text as geography_basis,
        re.source,
        fsu.state_code,
        re.recall_event_id
    from {{ ref('recall_event_firm') }} ref
    join firm_states_us fsu on fsu.firm_id = ref.firm_id
    join {{ ref('recall_event') }} re on re.recall_event_id = ref.recall_event_id
),

combined as (
    select * from distribution_lens
    union all
    select * from firm_registration_lens
)

select
    geography_basis,
    coalesce(source, 'ALL')          as source,
    state_code,
    count(distinct recall_event_id)  as recall_count
from combined
group by grouping sets (
    (geography_basis, source, state_code),
    (geography_basis, state_code)
)
order by geography_basis, source, recall_count desc
