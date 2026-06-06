\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== All-source crosswalk cleaning blast radius (Phase 6b PR 6b.4, Increment 1) ==='
-- Run AFTER `recalls resolve-firms` writes the all-source firm_crosswalk, BEFORE the
-- `dbt build` that wires it into the silver firm models. The 6b.4 unified cleaner now runs
-- the CPSC geo/DBA strip on EVERY source's names, but the geo strip was only validated on
-- CPSC (data/exploratory/cpsc/g1_comma_less_cohort.csv). This gate surfaces whether it
-- over-fires on FDA / USDA / NHTSA / USCG names — a geo_suffix_strip on a non-CPSC source
-- is the precision risk to eyeball (precision-first: an over-strip corrupts identity and
-- cannot be undone).
--
-- Method: recompute the per-source distinct-name universe (MIRRORS crosswalk_writer's
-- _DISTINCT_FIRM_NAMES + firm.sql all_normalized) and LEFT JOIN the written crosswalk on
-- firm_id = md5(firm_norm). The crosswalk does not store the raw name, so the join-back is
-- how we see before -> after.
--
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/verify_crosswalk_cleaning_blast_radius.sql

drop table if exists _src_names;
create temp table _src_names as
  select 'cpsc' as src, upper(trim(e.value ->> 'name')) as firm_norm
  from stg_cpsc_recalls c, jsonb_array_elements(coalesce(c.manufacturers, '[]'::jsonb)) e
  where nullif(trim(e.value ->> 'name'), '') is not null
  union all
  select 'cpsc', upper(trim(e.value ->> 'name'))
  from stg_cpsc_recalls c, jsonb_array_elements(coalesce(c.importers, '[]'::jsonb)) e
  where nullif(trim(e.value ->> 'name'), '') is not null
  union all
  select 'cpsc', upper(trim(e.value ->> 'name'))
  from stg_cpsc_recalls c, jsonb_array_elements(coalesce(c.distributors, '[]'::jsonb)) e
  where nullif(trim(e.value ->> 'name'), '') is not null
  union all
  select 'fda', upper(trim(firm_legal_nam))
  from stg_fda_recalls where nullif(trim(firm_legal_nam), '') is not null
  union all
  select 'usda', upper(trim(establishment))
  from stg_usda_fsis_recalls where nullif(trim(establishment), '') is not null
  union all
  select 'nhtsa', upper(trim(mfgname))
  from stg_nhtsa_recalls where nullif(trim(mfgname), '') is not null
  union all
  select 'nhtsa', upper(trim(mfgtxt))
  from stg_nhtsa_recalls where nullif(trim(mfgtxt), '') is not null
  union all
  select 'uscg', upper(trim(coalesce(m.company_name, r.company_name, r.mic)))
  from stg_uscg_recalls r
  left join stg_uscg_manufacturers m on upper(trim(r.mic)) = upper(trim(m.mic))
  where nullif(trim(coalesce(m.company_name, r.company_name, r.mic)), '') is not null;

drop table if exists _name_src;
create temp table _name_src as
  select firm_norm, string_agg(distinct src, '+' order by src) as sources
  from _src_names
  group by firm_norm;

\echo ''
\echo '=== Q1: crosswalk coverage — every recomputed name should have a crosswalk row ==='
-- unmatched > 0 means firm_norm diverged from the writer (resolver stale, or a name-query
-- drift between this script and crosswalk_writer). Expect unmatched = 0.
select
  count(*)                                          as distinct_names,
  count(x.firm_id)                                  as matched_in_crosswalk,
  count(*) - count(x.firm_id)                       as unmatched
from _name_src n
left join firm_crosswalk x on x.firm_id = md5(n.firm_norm);

\echo ''
\echo '=== Q2: match_confidence x source-set (watch geo_suffix_strip on non-cpsc sources) ==='
select x.match_confidence, n.sources, count(*) as n
from _name_src n
join firm_crosswalk x on x.firm_id = md5(n.firm_norm)
where x.match_confidence <> 'exact_name'
group by x.match_confidence, n.sources
order by x.match_confidence, n desc;

\echo ''
\echo '=== Q3: geo_suffix_strip on a NON-cpsc-only source — the precision watch-list ==='
-- A name that geo-stripped but is NOT carried by CPSC at all: the geo recipe was tuned on
-- CPSC, so these are the rows to confirm are real trailing-geo suffixes, not integral names.
select count(*) as non_cpsc_geo_strips
from _name_src n
join firm_crosswalk x on x.firm_id = md5(n.firm_norm)
where x.match_confidence = 'geo_suffix_strip_exact'
  and n.sources !~ 'cpsc';

\echo ''
\echo '=== Full raw -> clean change set -> data/exploratory/cross_source/crosswalk_cleaning_changes.csv ==='
\echo '   (Read this: every name the cleaner changed, with its source-set + confidence + before/after.)'
\pset format csv
\o data/exploratory/cross_source/crosswalk_cleaning_changes.csv
select
  n.firm_norm                as raw_name,
  x.clean_name,
  x.alternate_names,
  x.match_confidence,
  n.sources
from _name_src n
join firm_crosswalk x on x.firm_id = md5(n.firm_norm)
where x.match_confidence <> 'exact_name'
order by
  case x.match_confidence
    when 'geo_suffix_strip_exact' then 1
    when 'paren_strip_exact' then 2
    when 'dba_extract_exact' then 3
    else 4
  end,
  n.sources,
  n.firm_norm;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/crosswalk_cleaning_changes.csv'
