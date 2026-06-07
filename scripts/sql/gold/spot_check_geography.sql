-- Spot-check fct_recalls_by_geography (Phase 6e geography mart). Run after building the model.
--   psql "$DATABASE_URL" -f scripts/sql/gold/spot_check_geography.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== top 12 DISTRIBUTION-lens states (ALL sources) — where products went ==='
select state_code, recall_count from fct_recalls_by_geography
where geography_basis = 'distribution' and source = 'ALL'
order by recall_count desc limit 12;

\echo '=== top 12 FIRM-LOCATION-lens states (ALL sources) — where firms are registered ==='
select state_code, recall_count from fct_recalls_by_geography
where geography_basis = 'firm_location' and source = 'ALL'
order by recall_count desc limit 12;

\echo '=== firm-location coverage by source (FDA/USDA/USCG direct; CPSC/NHTSA only via cross-source-shared firms) ==='
select source, count(*) as n_state_rows, sum(recall_count) as total_recall_states
from fct_recalls_by_geography
where geography_basis = 'firm_location' and source <> 'ALL'
group by source order by source;

\echo '=== cross-source inheritance proof: CPSC/NHTSA recalls whose firm carries an FDA/USDA/USCG address ==='
select re.source, f.canonical_name, f.observed_company_ids,
       upper(coalesce(ea.state, ma.state, fa.firm_state_cd)) as inherited_state
from recall_event_firm ref
join recall_event re on re.recall_event_id = ref.recall_event_id and re.source in ('CPSC', 'NHTSA')
join firm f on f.firm_id = ref.firm_id
cross join lateral jsonb_array_elements_text(coalesce(f.observed_company_ids, '[]'::jsonb)) as cid(company_id)
left join firm_establishment_attributes ea on ea.establishment_id = cid.company_id
left join firm_manufacturer_attributes ma  on ma.mic = cid.company_id
left join firm_fda_attributes fa           on fa.firm_fei_num::text = cid.company_id
where coalesce(ea.state, ma.state, fa.firm_state_cd) is not null
limit 25;

\echo '=== row shape: rows per (basis, source) incl ALL rollup ==='
select geography_basis, source, count(*) as n_states
from fct_recalls_by_geography group by 1, 2 order by 1, 2;
