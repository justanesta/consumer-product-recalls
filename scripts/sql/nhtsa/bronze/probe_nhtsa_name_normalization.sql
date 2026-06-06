\set ON_ERROR_STOP on
set client_min_messages = warning;
\echo '=== NHTSA firm-name normalization opportunity (Phase 6b PR 6b.3, post-D1) ==='
-- Locks the deterministic cleaning NHTSA firm names (mfgname filer + mfgtxt manufacturer)
-- receive in enrichment.firm_crosswalk. The 6b.3 plan body predates the 6b.1 D1 revision
-- (there is NO shared dbt macro; cleaning is Python folded into the crosswalk), so this
-- re-grounds the recipe in the corpus instead of the stale "route through clean_firm_name
-- macro" text.
--
-- HYPOTHESIS to confirm/deny with the numbers below:
--   * Parenthetical-strip ("CHRYSLER (FCA US, LLC) (STELLANTIS)" -> "CHRYSLER") is the
--     SAFE deterministic win -> changes firm_id, flows through the crosswalk in 6b.3.
--   * Corporate-form tokens (INC/LLC/CORP/CO) + regional suffixes (OF AMERICA / USA)
--     DEFER to 6b.4's RapidFuzz scoring stopwords (the G0 decision), NOT a 6b.3 firm_id
--     change — stripping them on NHTSA only would desync cross-source exact-name joins,
--     and absorbing that is exactly what the fuzzy layer is for.
--
-- The console queries below are full-corpus AGGREGATES (not truncated). The per-name
-- detail dumps to data/exploratory/nhtsa/name_normalization_features.csv ([[feedback_
-- dump_full_results_for_analysis]]) so the recipe is locked holistically — crucially,
-- the true parenthetical-strip merge yield needs GROUP BY stripped_name (a sample misses
-- the multi-variant clusters, e.g. the three "Chrysler (FCA ...)" spellings -> CHRYSLER).
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/bronze/probe_nhtsa_name_normalization.sql

drop table if exists _nhtsa_names;
create temp table _nhtsa_names as
  select distinct upper(trim(mfgname)) as name
  from nhtsa_recalls_bronze
  where mfgname is not null and trim(mfgname) <> ''
  union
  select distinct upper(trim(mfgtxt))
  from nhtsa_recalls_bronze
  where mfgtxt is not null and trim(mfgtxt) <> '';

\echo '=== Q1: distinct NHTSA firm names (mfgname UNION mfgtxt) ==='
select count(*) as distinct_names from _nhtsa_names;

\echo ''
\echo '=== Q2: names carrying a (parenthetical) — count + rate ==='
select
  count(*) filter (where name ~ '\(')                                   as paren_names,
  count(*)                                                              as total_names,
  round(100.0 * count(*) filter (where name ~ '\(') / nullif(count(*), 0), 1) as pct_paren
from _nhtsa_names;

\echo ''
\echo '=== Q3: parenthetical-strip yield — stripped, merges-to-existing, AND true clusters ==='
-- paren_stripped       = names the strip changes at all
-- merges_to_existing   = stripped form EXACTLY equals another listed name (Q3 of old probe)
-- in_multi_variant_cluster = stripped form is shared by >1 ORIGINAL name (the undercounted
--   win: the 3 "Chrysler (FCA ...)" spellings all collapse to CHRYSLER even with no bare
--   CHRYSLER listed). This is the real merge structure -> GROUP BY stripped_name.
with stripped as (
  select name, trim(regexp_replace(name, '\s*\([^)]*\)', '', 'g')) as stripped_name
  from _nhtsa_names
),
clusters as (
  select stripped_name, count(*) as n_in_cluster
  from stripped
  group by stripped_name
)
select
  count(*) filter (where s.stripped_name <> s.name)                                   as paren_stripped,
  count(*) filter (where s.stripped_name <> s.name
                     and exists (select 1 from _nhtsa_names m
                                 where m.name = s.stripped_name and m.name <> s.name)) as merges_to_existing,
  count(*) filter (where s.stripped_name <> s.name and c.n_in_cluster > 1)            as in_multi_variant_cluster,
  count(distinct s.stripped_name) filter (where c.n_in_cluster > 1)                   as distinct_merge_clusters
from stripped s
join clusters c using (stripped_name);

\echo ''
\echo '=== Q4: parenthetical-strip OVER-strip COUNT (stripped empty or <3 chars) — must be 0 ==='
select count(*) as over_strip_count
from _nhtsa_names
where trim(regexp_replace(name, '\s*\([^)]*\)', '', 'g')) <> name
  and length(trim(regexp_replace(name, '\s*\([^)]*\)', '', 'g'))) < 3;

\echo ''
\echo '=== Q5: corporate-form token frequency (sizes 6b.4 scoring stopwords, NOT a 6b.3 clean) ==='
select
  count(*) filter (where name ~ '\y(INC|INCORPORATED|LLC|CORP|CORPORATION|COMPANY|CO|LTD|LIMITED|GMBH|AG|MOTOR|MOTORS)\y') as names_with_corp_form,
  count(*) as total_names
from _nhtsa_names;

\echo ''
\echo '=== Q5b: corporate-form token breakdown ==='
select tok, count(*) as names_containing
from _nhtsa_names,
  lateral (values ('INC'),('INCORPORATED'),('LLC'),('CORP'),('CORPORATION'),('COMPANY'),
                  ('CO'),('LTD'),('LIMITED'),('GMBH'),('AG'),('MOTOR'),('MOTORS')) v(tok)
where name ~ ('\y' || tok || '\y')
group by tok
order by names_containing desc;

\echo ''
\echo '=== Q6: regional-suffix frequency (also DEFER to 6b.4; mirrors CPSC "of America" non-strip) ==='
select
  count(*) filter (where name ~ '\y(USA|AMERICA)\y') as names_with_usa_america,
  count(*)                                           as total_names
from _nhtsa_names;

\echo ''
\echo '=== Full-corpus feature table -> data/exploratory/nhtsa/name_normalization_features.csv ==='
\echo '   (Read this for the holistic recipe lock — every distinct name + features, no truncation.)'
\pset format csv
\o data/exploratory/nhtsa/name_normalization_features.csv
with stripped as (
  select name, trim(regexp_replace(name, '\s*\([^)]*\)', '', 'g')) as stripped_name
  from _nhtsa_names
)
select
  s.name,
  (s.name ~ '\(')                       as has_paren,
  s.stripped_name,
  (s.stripped_name <> s.name)           as paren_stripped,
  exists (select 1 from _nhtsa_names m
          where m.name = s.stripped_name and m.name <> s.name) as strips_to_existing,
  length(s.stripped_name)               as stripped_len,
  (s.name ~ '\y(INC|INCORPORATED|LLC|CORP|CORPORATION|COMPANY|CO|LTD|LIMITED|GMBH|AG|MOTOR|MOTORS)\y') as has_corp_form,
  (s.name ~ '\y(USA|AMERICA)\y')        as has_region
from stripped s
order by s.name;
\o
\pset format aligned
\echo '   done — Read data/exploratory/nhtsa/name_normalization_features.csv'
