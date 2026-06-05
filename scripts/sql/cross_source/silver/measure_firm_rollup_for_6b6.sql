-- MEASURE — firm-resolution rollup behavior on the FULL corpus, to design the PR 6b.6
-- cross-source acceptance tests (thresholds + exemplar asserts + severity). Read-only.
--
-- Sizes: (Q2) the over-merge ceiling, (Q3) the cross-source unification value + monitor floor,
-- (Q4) what Honda/Tyson actually do under name/brand grain (the parent-rollup gap), and (Q5)
-- confirms the known over-merge disasters are currently un-merged before we encode anti-merge tests.
-- Decision home: project_scope/phase-6b-execution-plan.md PR 6b.6 + the 6b.6 design thread.
--
-- Run from repo root (Q3b/Q4b dump to data/exploratory/cross_source/, gitignored):
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/measure_firm_rollup_for_6b6.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: resolution STATE — confirm the shipped FEI-off name/brand grain is in the DB ==='
-- Expect resolver_version like allsrc-tier{12|1}-roll90-v3 and ~24.2k firm rows. If it is the old
-- FEI-on (allsrc-tier{012|01}-...-v2) output, STOP — re-resolve before trusting anything below.
select 'firm rows'                  as metric, count(*)::text                                  as value from firm
union all select 'recall_event_firm rows', count(*)::text                                      from recall_event_firm
union all select 'firm_crosswalk rows',     count(*)::text                                      from firm_crosswalk
union all select 'resolver_version(s)',      coalesce(string_agg(distinct resolver_version, ' | '), '<none>') from firm_crosswalk;

\echo ''
\echo '=== Q2a: cluster-size histogram (observed_names per firm) -> sets the over-merge ceiling ==='
select jsonb_array_length(observed_names) as n_observed_names, count(*) as n_firms
from firm
group by 1
order by 1 desc
limit 40;

\echo ''
\echo '=== Q2b: the 25 LARGEST clusters — eyeball for over-merge (any unrelated names welded?) ==='
select firm_id, canonical_name, jsonb_array_length(observed_names) as n, observed_names
from firm
order by jsonb_array_length(observed_names) desc
limit 25;

\echo ''
\echo '=== Q3a: cross-source SPAN — how many firms appear under N distinct sources ==='
with firm_sources as (
  select ref.firm_id, count(distinct re.source) as n_sources
  from recall_event_firm ref
  join recall_event re using (recall_event_id)
  group by ref.firm_id
)
select n_sources, count(*) as n_firms
from firm_sources
group by 1
order by 1 desc;

\echo ''
\echo '=== Q3b: dump the cross-source firms (>=2 sources) — the value narrative + monitor floor ==='
\copy (with firm_sources as (select ref.firm_id, count(distinct re.source) as n_sources, string_agg(distinct re.source, ',' order by re.source) as sources from recall_event_firm ref join recall_event re using (recall_event_id) group by ref.firm_id) select f.firm_id, f.canonical_name, fs.n_sources, fs.sources, f.observed_names from firm_sources fs join firm f using (firm_id) where fs.n_sources >= 2 order by fs.n_sources desc, f.canonical_name) to 'data/exploratory/cross_source/firms_multi_source.csv' with (format csv, header true)

\echo ''
\echo '=== Q4a: exemplar entities — how many DISTINCT firms does each name token span? ==='
-- n_firms > 1 means the entity fragments under name-grain (e.g. divisions/brands kept separate).
with probes as (
  select unnest(array['HONDA','TYSON','GENERAL MOTORS','FORD','YAMAHA','POLARIS','SAMSUNG','NESTLE','KAWASAKI','PERDUE']) as token
)
select p.token, count(distinct f.firm_id) as n_firms
from probes p
join firm f
  on exists (select 1 from jsonb_array_elements_text(f.observed_names) n where upper(n) like '%' || p.token || '%')
group by p.token
order by p.token;

\echo ''
\echo '=== Q4b: dump the HONDA + TYSON firm rows (names + sources) — quantify the parent-rollup gap ==='
\copy (select f.firm_id, f.canonical_name, jsonb_array_length(f.observed_names) as n_names, (select string_agg(distinct re.source, ',' order by re.source) from recall_event_firm ref join recall_event re using (recall_event_id) where ref.firm_id = f.firm_id) as sources, f.observed_names from firm f where exists (select 1 from jsonb_array_elements_text(f.observed_names) n where upper(n) like '%HONDA%' or upper(n) like '%TYSON%') order by f.canonical_name) to 'data/exploratory/cross_source/honda_tyson_firms.csv' with (format csv, header true)

\echo ''
\echo '=== Q5: anti-merge sanity — known-bad pairs MUST land in different firm_ids (merged_bad = false) ==='
-- If the shipped name-grain resolver is clean, every merged_bad is FALSE. A TRUE row = a confirmed
-- over-merge present in the DB (would mean the crosswalk is the old FEI-on output, or Tier 2 loosened).
with pairs as (
  select * from (values
    ('WHOLE FOODS', 'STRYKER'),
    ('TEVA',        'BAYER'),
    ('BIOMAT',      'GRIFOLS'),
    ('CSL PLASMA',  'OCTAPHARMA'),
    ('TAKEDA',      'BIOLIFE')
  ) as p(a, b)
)
select
  p.a, p.b,
  (select count(distinct firm_id) from firm f where exists (select 1 from jsonb_array_elements_text(f.observed_names) n where upper(n) like '%' || p.a || '%')) as a_firms,
  (select count(distinct firm_id) from firm f where exists (select 1 from jsonb_array_elements_text(f.observed_names) n where upper(n) like '%' || p.b || '%')) as b_firms,
  exists (
    select 1 from firm f
    where exists (select 1 from jsonb_array_elements_text(f.observed_names) n where upper(n) like '%' || p.a || '%')
      and exists (select 1 from jsonb_array_elements_text(f.observed_names) n where upper(n) like '%' || p.b || '%')
  ) as merged_bad
from pairs p
order by p.a;

\echo ''
\echo 'done. Dumps: data/exploratory/cross_source/firms_multi_source.csv + honda_tyson_firms.csv'
