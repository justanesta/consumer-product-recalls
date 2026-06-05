\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== Geo-gate cross-source conflict / demotion sizing (Phase 6b PR 6b.4, ADR 0037 amend) ==='
-- The geo-strip is being GATED by source: ON for {cpsc, nhtsa} (name-only sources), OFF for
-- {fda, usda, uscg} (FEI / establishment_number / MIC carry within-source identity). The
-- resolver derives ONE geo-mode per distinct name from ALL its sources, precedence
-- "structured-id source present -> geo OFF", so a name shared across a geo-ON and a geo-OFF
-- source can never produce two divergent crosswalk rows (no PK conflict by construction).
--
-- This query SIZES the side effect that remains: a name carried by BOTH a geo-ON and a
-- geo-OFF source, WITH a geo "of" tail, gets DEMOTED to geo-OFF — so its within-CPSC/NHTSA
-- geo-strip is given up (RapidFuzz recovers the merge later, the "Pfizer-class" demotion).
-- If the demotion count is ~0, the precedence rule almost never fires and there is nothing
-- to worry about; whatever it is, it is the exact set the fuzzy-layer regression test should
-- assert re-merges.
--
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/validate_geo_gate_conflicts.sql

drop table if exists _src_names;
create temp table _src_names as
  select 'cpsc' as source, upper(trim(e.value ->> 'name')) as firm_norm
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

drop table if exists _bynorm;
create temp table _bynorm as
  select
    firm_norm,
    string_agg(distinct source, ',' order by source)                          as sources,
    count(distinct source)                                                     as n_sources,
    bool_or(source in ('cpsc', 'nhtsa'))                                       as has_geo_on,
    bool_or(source in ('fda', 'usda', 'uscg'))                                 as has_geo_off,
    -- loose upper bound on "would the geo-strip touch this name" (the real Python strip is
    -- narrower: vocab + blocklist + last-of anchoring). A standalone word-bounded "of".
    (firm_norm ~* '(^|[ ,])of[ ]')                                             as has_of_clause,
    (firm_norm ~ '\([^)]*\)')                                                  as has_paren
  from _src_names
  group by firm_norm;

\echo ''
\echo '=== Q1: cross-source overlap + the geo-gate demotion set ==='
-- multi_source        = names appearing in >=2 sources (the ~78 G0 overlap, recomputed).
-- straddle_boundary   = appears in BOTH a geo-ON and a geo-OFF source.
-- demotion_candidates = straddlers WITH an "of" clause = names that LOSE a geo-strip under the
--                       "geo-off wins" precedence (upper bound; eyeball Q2 for the true subset).
select
  count(*)                                                                     as distinct_names,
  count(*) filter (where n_sources >= 2)                                       as multi_source,
  count(*) filter (where has_geo_on and has_geo_off)                           as straddle_boundary,
  count(*) filter (where has_geo_on and has_geo_off and has_of_clause)         as demotion_candidates
from _bynorm;

\echo ''
\echo '=== Q2: the demotion candidates (straddle + of-clause) — eyeball: do these truly geo-strip? ==='
-- These are CPSC/NHTSA names also carried by an ID-source that have an of-clause. Under the
-- gate they stay whole (geo-off) instead of stripping; RapidFuzz must re-merge them. The
-- fuzzy-layer regression test should assert each of these re-merges with its bare form.
select firm_norm, sources
from _bynorm
where has_geo_on and has_geo_off and has_of_clause
order by firm_norm;

\echo ''
\echo '=== Q3: ALL multi-source names (the cross-source overlap set) -> dump for the record ==='
\echo '   (Most should be clean brands that clean to themselves under any rule.)'
\pset format csv
\o data/exploratory/cross_source/geo_gate_overlap.csv
select firm_norm, sources, n_sources, has_geo_on, has_geo_off, has_of_clause, has_paren
from _bynorm
where n_sources >= 2
order by (has_geo_on and has_geo_off and has_of_clause) desc, n_sources desc, firm_norm;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/geo_gate_overlap.csv'
