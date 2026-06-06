\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== Cleaning blast radius by source (Phase 6b PR 6b.4, Increment 1) — PRE-resolver ==='
-- Run BEFORE `recalls resolve-firms`. The 6b.4 unified cleaner (clean_firm_name_unified)
-- adds a balanced-parenthetical strip on top of the CPSC geo/DBA strip, applied to EVERY
-- source's names. This sizes that paren strip per source and CLASSIFIES the paren content,
-- so the one genuine precision call — stripping a (CITY)/(COUNTRY) of-incorporation paren,
-- which collapses legally-distinct subsidiaries of one parent — is visible before commit.
--
-- WHY classify paren content instead of previewing the final clean: the geo strip is
-- Python-only (vocab + blocklist + last-"of" anchoring — not faithfully expressible in SQL),
-- and a naive SQL paren-strip OVER-states mess (leaves doubled commas the Python DBA/geo
-- strip + trailing tidy resolve). Paren CONTENT, however, is faithfully inspectable here.
-- For byte-exact final clean names, use the resolver's dry-run dump (the Python cleaner).
--
-- Categories (precedence top-down; a name lands in the first it matches):
--   narrative  — (FORMERLY X) / (A DIVISION OF Y) / (NKA Z) / (NO LONGER IN BUSINESS): pure
--                noise, safe to drop.
--   location   — paren names a place (US/USA/country/Chinese city-province): the OVER-MERGE
--                precision watch-list — collapses (SUZHOU) vs (SHENZHEN) subsidiaries.
--   acronym    — short token (<= 7 chars), e.g. (ASTI)/(BRP): safe alias noise.
--   other      — eyeball.
--
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/probe_cleaning_blast_radius_by_source.sql

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

-- One row per (source, distinct name). A name shared across sources counts once per source
-- (matches how each source's branch feeds the crosswalk).
drop table if exists _names;
create temp table _names as
  select distinct src, firm_norm from _src_names;

-- Classify each paren-bearing name. Location regex = the over-merge watch-list: US-regional,
-- the countries, and the Chinese city/province tokens seen in the corpus.
drop table if exists _classified;
create temp table _classified as
  select
    src,
    firm_norm,
    -- paren strip in ISOLATION (SQL) — for the over-strip guard only, NOT the final clean.
    trim(regexp_replace(firm_norm, '\s*\([^)]*\)', '', 'g')) as paren_stripped_sql,
    case
      when firm_norm ~* '\([^)]*\m(formerly|f\.?/?k\.?/?a|n\.?/?k\.?/?a|known as|division|subsidiary|now known|previously|no longer in business|owner of|under license|licensee|a textron)\M[^)]*\)'
        then 'narrative'
      when firm_norm ~* '\([^)]*\m(u\.?s\.?a?\.?|usa|us|united states|north america|n\. america|america|china|hong kong|hk|taiwan|canada|mexico|vietnam|thailand|malaysia|india|japan|korea|germany|italy|switzerland|spain|u\.?k\.?|england|puerto rico|suzhou|shenzhen|dongguan|zhejiang|zheijiang|zhongshan|tianjin|guangdong|zhuhai|shanghai|ningbo|jiaxing|huizhou|pune|maharashtra)\M[^)]*\)'
        then 'location'
      when firm_norm ~ '\(\s*[A-Z0-9&. -]{1,7}\s*\)'
        then 'acronym'
      else 'other'
    end as paren_category
  from _names
  where firm_norm ~ '\([^)]*\)';

\echo ''
\echo '=== Q1: distinct names + paren-bearing names per source ==='
select
  n.src,
  count(*)                                              as distinct_names,
  count(*) filter (where n.firm_norm ~ '\([^)]*\)')     as paren_names,
  round(100.0 * count(*) filter (where n.firm_norm ~ '\([^)]*\)')
        / nullif(count(*), 0), 1)                       as pct_paren
from _names n
group by n.src
order by n.src;

\echo ''
\echo '=== Q2: paren category breakdown per source (location = over-merge watch-list) ==='
select src, paren_category, count(*) as n
from _classified
group by src, paren_category
order by src, n desc;

\echo ''
\echo '=== Q3: OVER-strip guard — paren strip leaving < 2 chars, per source (must be 0) ==='
select src, count(*) as over_strip_count
from _classified
where length(paren_stripped_sql) < 2
group by src
having count(*) > 0
order by src;

\echo ''
\echo '=== Q4: geo-suffix candidates per source (trailing ", of ..." the geo strip CONSIDERS) ==='
-- A superset of the actual geo strips (the Python blocklist + geo-vocab narrow it). Shows
-- which sources the CPSC-tuned geo strip even touches — expect it concentrated in CPSC.
select
  n.src,
  count(*) filter (where n.firm_norm ~* ',?\s+of\s+[^,]+,?[^,]*$') as trailing_of_candidates
from _names n
group by n.src
order by n.src;

\echo ''
\echo '=== Full per-source paren change set -> data/exploratory/cross_source/cleaning_blast_radius_by_source.csv ==='
\echo '   (Read this; LOCATION + OTHER rows first — those are the eyeball priorities.)'
\pset format csv
\o data/exploratory/cross_source/cleaning_blast_radius_by_source.csv
select
  src,
  paren_category,
  firm_norm                                                    as raw_name,
  (regexp_match(firm_norm, '\(([^)]*)\)'))[1]                  as first_paren_content,
  paren_stripped_sql                                           as paren_stripped_sql_preview
from _classified
order by
  case paren_category
    when 'location' then 1
    when 'other' then 2
    when 'narrative' then 3
    else 4
  end,
  src,
  firm_norm;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/cleaning_blast_radius_by_source.csv'
