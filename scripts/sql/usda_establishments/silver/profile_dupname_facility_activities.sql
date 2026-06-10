-- Gate U4 (6b.2) — establishment-side: dup-name split (corpus-scale, city-aware) + activities + cold-storage.
--
-- PURPOSE: re-derive the duplicate-name category split on the FULL 7,979-establishment dim, GROUPED
-- BY (name, city, state) — the dev-slice 276/103/77 (multi_grant/multi_state/mixed) predates the
-- corpus finding that 67.1% of establishment_ids are '+'-composites. If composites already
-- collapse a facility's M/P/V grants into ONE row, the `same_facility (name,city,state)` category
-- should be SMALL and the same-facility-collapse reframing is largely moot — disambiguation is then
-- about genuinely different facilities sharing a name (multi_state + mixed). Also profiles the
-- activities domain (Signal 3) and the cold-storage operators (wrong-firm-attribution surface).
--
-- Reads firm_usda_attributes (the silver dim, one row per establishment_id — the same
-- candidate set the resolution model disambiguates over). NOTE: the dim aliases
-- `establishment_number AS establishment_id`, so establishment_id HOLDS the FSIS grant number
-- (M.../P.../G.../I.../V..., incl. '+'-composites) — it is not a surrogate key.
--
-- Feeds: project_scope/phase-6-execution-plan.md PR 6b.2 gates G2 (split) + G5 (cold-storage) + Signal 3.
-- Run with: psql ... -f scripts/sql/usda_establishments/silver/profile_dupname_facility_activities.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: dim sanity — establishments, composite share, name uniqueness ==='
select
  count(*)                                                       as establishments,
  count(distinct establishment_name)                            as distinct_names,
  count(*) filter (where establishment_id like '%+%')       as composite_numbers,
  round(100.0 * count(*) filter (where establishment_id like '%+%') / nullif(count(*),0), 1) as pct_composite,
  round(100.0 * count(distinct establishment_name) / nullif(count(*),0), 1) as pct_name_unique
from firm_usda_attributes;

\echo ''
\echo '=== Q2: duplicate-name category split (city-aware) — settles the same-facility question ==='
-- same_facility_or_address = one (name,city,state) holding multiple numbers (the only category the
-- establishment_group_id reframing addresses). If small, composites already solved it.
with groups as (
  select
    establishment_name,
    count(*)                                             as instances,
    count(distinct state)                                as distinct_states,
    count(distinct upper(trim(coalesce(city, ''))))      as distinct_cities,
    count(distinct establishment_id)                 as distinct_numbers,
    bool_or(status_regulated_est = 'Inactive')           as any_inactive,
    bool_or(establishment_id like '%+%')             as any_composite
  from firm_usda_attributes
  group by establishment_name
  having count(*) > 1
),
classified as (
  select *,
    case
      when distinct_states = 1 and distinct_cities = 1 then 'same_facility_or_address'
      when distinct_states = 1                         then 'multi_city_same_state'
      when distinct_states = instances                 then 'multi_state'
      else                                                  'mixed'
    end as category
  from groups
)
select
  category,
  count(*)                                              as group_count,
  sum(instances)                                        as records_in_category,
  round(avg(instances)::numeric, 2)                     as avg_group_size,
  max(instances)                                        as max_group_size,
  sum(case when any_inactive then 1 else 0 end)         as groups_with_inactive,
  sum(case when any_composite then 1 else 0 end)        as groups_with_a_composite
from classified
group by category
order by group_count desc;

\echo ''
\echo '=== Q3: activities domain (Signal 3 vocabulary — recall.processing must map to these) ==='
select trim(elem) as activity, count(*) as n_establishments
from firm_usda_attributes, lateral jsonb_array_elements_text(activities) as elem
where activities is not null
group by trim(elem)
order by n_establishments desc;

\echo ''
\echo '=== Q4: cold-storage operators — wrong-firm-attribution surface (the producer is elsewhere) ==='
-- Establishments whose name or activities scream storage/logistics, not production. When a recall''s
-- establishment IS one of these, the firm-of-interest is the PRODUCER (from product_items), not the
-- storage facility — a Phase 6/7 producer-extraction concern, FLAGGED (not disambiguated) in 6b.2.
select establishment_name, count(*) as n_facilities,
       string_agg(distinct establishment_id, ', ' order by establishment_id) as numbers,
       (array_agg(distinct activities::text))[1] as sample_activities
from firm_usda_attributes
where establishment_name ~* 'lineage|americold|cold storage|cold-storage|logistics|warehous'
   or (activities::text ~* 'storage|warehous|freez|distribution center'
       and activities::text !~* 'slaughter|processing|production')
group by establishment_name
order by n_facilities desc, establishment_name
limit 40;

\echo ''
\echo '=== Q5: DUMP all duplicate-name groups (detail) to data/exploratory/usda_establishments/ ==='
-- Holistic review of the genuine fan-out surface: each shared name with its facilities (number,
-- city, state, status). Inspect to confirm the category heuristic + spot cold-storage chains.
\pset format csv
\pset footer off
\o data/exploratory/usda_establishments/u4_dupname_groups.csv
with groups as (
  select establishment_name
  from firm_usda_attributes
  group by establishment_name
  having count(*) > 1
)
select
  e.establishment_name,
  count(*) over (partition by e.establishment_name)                          as group_size,
  e.establishment_id,
  e.city,
  e.state,
  case when e.status_regulated_est = 'Inactive' then 'Inactive' else 'Active' end as status
from firm_usda_attributes e
join groups g on g.establishment_name = e.establishment_name
order by count(*) over (partition by e.establishment_name) desc, e.establishment_name, e.establishment_id;
\o
\pset format aligned
\pset footer on
\echo 'Wrote data/exploratory/usda_establishments/u4_dupname_groups.csv'
