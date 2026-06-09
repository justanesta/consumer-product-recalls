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
-- C18 (2026-06-09): each firm is collapsed to a SINGLE primary registered state, so a recall is no
-- longer multi-counted across one firm's several registered states (the documented multi-counting in
-- gold_design_notes caveat #2). Rule (user decision Q1): the MOST-FREQUENT registered state across
-- the firm's structural ids; tie-break is DETERMINISTIC by state_code ASC — a uniform cross-source
-- "most-recent" date is NOT available (establishment has grant_date, manufacturer date_modified, FDA
-- none), so the requested most-recent tiebreak degrades to a stable deterministic one (ties are rare).
-- A recall is still counted once per DISTINCT firm (multi-firm recalls legitimately span states).

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
-- NOT distinct: one row per (firm, structural-id match) so we can count state frequency below.
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
        from {{ ref('firm_establishment_attributes') }} ea where ea.establishment_id = cid.company_id
        union all
        select ma.state
        from {{ ref('firm_manufacturer_attributes') }} ma where ma.mic = cid.company_id
        union all
        select fa.firm_state_cd
        from {{ ref('firm_fda_attributes') }} fa where fa.firm_fei_num::text = cid.company_id
    ) s
    where s.state_code is not null and trim(s.state_code) <> ''
),

firm_states_us as (
    select fs.firm_id, fs.state_code
    from firm_states fs
    join {{ ref('us_state_abbr') }} usa on usa.abbr = fs.state_code
),

-- C18: collapse each firm to ONE primary registered state (most-frequent; tie-break state_code asc).
firm_primary_state as (
    select firm_id, state_code
    from (
        select
            firm_id,
            state_code,
            row_number() over (
                partition by firm_id
                order by count(*) desc, state_code asc
            ) as rn
        from firm_states_us
        group by firm_id, state_code
    ) ranked
    where rn = 1
),

firm_registration_lens as (
    select distinct
        'firm_registration'::text as geography_basis,
        re.source,
        fps.state_code,
        re.recall_event_id
    from {{ ref('recall_event_firm') }} ref
    join firm_primary_state fps on fps.firm_id = ref.firm_id
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
