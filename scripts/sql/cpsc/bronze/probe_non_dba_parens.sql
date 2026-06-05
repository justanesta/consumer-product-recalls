\set ON_ERROR_STOP on
set client_min_messages = warning;
\echo '=== CPSC non-DBA parenthetical blast radius (Phase 6b PR 6b.4, Increment 1) ==='
-- The 6b.4 unified cleaner (clean_firm_name_unified) runs _strip_parentheticals AFTER the
-- CPSC clean_firm_name, so CPSC names now also lose any BALANCED non-DBA "(...)" group — a
-- strict improvement over 6b.1, which only removed the "(doing business as X)" DBA paren.
-- A non-DBA paren on a CPSC firm name is usually "(formerly X)" / "(a division of Y)" / a
-- model annotation — safe to drop for identity — but precision-first: size + eyeball the
-- set before committing the crosswalk to the silver firm models.
--
-- DBA parens are EXCLUDED here: clean_firm_name already strips them and extract_firm_dba
-- captures the brand into firm.alternate_names, so they are not new blast radius.
--
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/cpsc/bronze/probe_non_dba_parens.sql

drop table if exists _cpsc_names;
create temp table _cpsc_names as
  with latest as (
    select manufacturers, importers, distributors,
           row_number() over (
               partition by source_recall_id order by extraction_timestamp desc
           ) as rn
    from cpsc_recalls_bronze
  ),
  names as (
    select e.value ->> 'name' as raw
    from latest l, jsonb_array_elements(coalesce(l.manufacturers, '[]'::jsonb)) e
    where l.rn = 1
    union all
    select e.value ->> 'name'
    from latest l, jsonb_array_elements(coalesce(l.importers, '[]'::jsonb)) e
    where l.rn = 1
    union all
    select e.value ->> 'name'
    from latest l, jsonb_array_elements(coalesce(l.distributors, '[]'::jsonb)) e
    where l.rn = 1
  )
  select distinct upper(trim(raw)) as name
  from names
  where nullif(trim(raw), '') is not null;

\echo ''
\echo '=== Q1: distinct CPSC names total ==='
select count(*) as distinct_cpsc_names from _cpsc_names;

\echo ''
\echo '=== Q2: any balanced paren vs NON-DBA balanced paren (the new blast radius) ==='
select
  count(*) filter (where name ~ '\([^)]*\)')                                   as any_paren,
  count(*) filter (where name ~ '\([^)]*\)'
                     and name !~* '\(\s*(doing\s+business\s+as|d[./]?b[./]?a)') as non_dba_paren,
  count(*)                                                                      as total_names
from _cpsc_names;

\echo ''
\echo '=== Q3: OVER-strip guard — paren strip leaving < 2 chars (must be ~0) ==='
select count(*) as over_strip_count
from _cpsc_names
where name ~ '\([^)]*\)'
  and length(trim(regexp_replace(name, '\s*\([^)]*\)', '', 'g'))) < 2;

\echo ''
\echo '=== Full non-DBA-paren CPSC names -> data/exploratory/cpsc/non_dba_parens.csv ==='
\echo '   (Read this to eyeball every CPSC name the new paren strip changes — no truncation.)'
\pset format csv
\o data/exploratory/cpsc/non_dba_parens.csv
select
  name,
  trim(regexp_replace(name, '\s*\([^)]*\)', '', 'g')) as paren_stripped
from _cpsc_names
where name ~ '\([^)]*\)'
  and name !~* '\(\s*(doing\s+business\s+as|d[./]?b[./]?a)'
order by name;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cpsc/non_dba_parens.csv'
