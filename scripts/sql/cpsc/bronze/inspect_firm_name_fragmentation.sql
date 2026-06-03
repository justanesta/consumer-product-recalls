-- Phase 6 (feature/silver-field-remap, W1) — CPSC firm-name fragmentation baseline.
--
-- CAVEAT: this is firm-dimension SHAPE / fragmentation EVIDENCE for the silver remap's
-- firm-architecture decisions (§6 Option B + §3 Bug 2 in
-- documentation/cpsc/field_audit_2026_w22.md) and the cross-source SCD/consolidation
-- docs. It is NOT a re-derivation of which fields to capture (Phase 6a, done), and it is
-- NOT the silver implementation — the suffix-strip below is a read-only SIMULATION that
-- quantifies how much the §3 Bug 2 fragmentation WOULD collapse under name normalization.
-- The actual normalization is deferred to Phase 6b (RapidFuzz workstream); this file only
-- measures the magnitude so Option B and the ADR threshold have empirical backing.
--
-- When to run: against the full-corpus cpsc_recalls_bronze (corpus scale, post Phase 6a.5).
-- Sibling to the two existing CPSC bronze scripts, which this does NOT duplicate:
--   • explore_bronze_shape.sql  — scalar null rates + per-array empty rates + cadence
--   • inspect_array_field_population.sql — nested-key population, enum domains, and the
--     per-role Name length/distinctness signal (Q10/Q11).
-- This file adds the one thing neither covers: the firm-DIM contribution per role (the
-- Option-B retailer-removal magnitude) and the Bug-2 suffix-strip collapse simulation.
--
-- JSONB KEY CASING (load-bearing — see field_audit_2026_w22.md §5 "Bronze JSONB key
-- casing"): bronze stores **snake_case** keys (`name`), NOT the PascalCase the API returns.
-- Every drill below uses `value->>'name'`. PascalCase ('Name') returns NULL silently and
-- fabricates 100%-empty false positives. Do not "fix" these to PascalCase.
--
-- STRIP SIMULATION (Q3/Q5/Q6) — a deliberately CONSERVATIVE lower bound. It removes only
-- the comma-anchored ", of <geo>" / ", dba|d/b/a|doing business as <brand>" suffix tail
-- (the dominant Bug-2 forms per the audit). It does NOT strip space-prefixed " dba " with
-- no comma, parenthetical translations, or legal-entity tokens (Inc./Ltd/LLC) — those are
-- the more invasive Option-C / 6b transforms. Q4 measures that residual headroom separately,
-- so real 6b normalization collapses at least as much as Q3 reports, never less.
--
-- Output feeds: documentation/cpsc/field_audit_2026_w22.md §9 (corpus-scale re-validation)
-- + documentation/audit/bronze_corpus_profile.md §2/§3/§5 (CPSC grain + firm fragmentation).
-- Each Qn block is cited by number in those docs.
--
-- Run with: psql ... -f scripts/sql/cpsc/bronze/inspect_firm_name_fragmentation.sql

\echo '=== Q1: per-role firm-dim footprint (elements vs distinct normalized names) ==='
-- How many firm rows each of the 4 CPSC firm-role arrays contributes today. distinct
-- normalized_name = the current firm.sql key (upper(trim(name))). A high pct_distinct on
-- retailer (§3 Bug 1, ~99%) confirms single-use narrative pollution; 87-94% on M/I/D
-- (§3 Bug 2) confirms suffix fragmentation. Baseline for Option B (Q2) and Bug 2 (Q3).
with firm_elements as (
  select 'manufacturer' as role, upper(trim(m.value->>'name')) as normalized_name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'retailer', upper(trim(m.value->>'name'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'importer', upper(trim(m.value->>'name'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'distributor', upper(trim(m.value->>'name'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select
  role,
  count(*) as elements,
  count(distinct normalized_name) as distinct_firms,
  round(100.0 * count(distinct normalized_name) / nullif(count(*), 0), 1) as pct_distinct
from firm_elements
group by role
order by elements desc;

\echo ''
\echo '=== Q2: Option-B firm-dim reduction (remove retailer role from the firm dim) ==='
-- §6 Option B headline: removing Retailers[] from firm.sql/recall_event_firm.sql drops the
-- firm dim by (firms_all_four_roles - firms_mid_only). retailer_only_distinct_names is the
-- upper bound (a retailer narrative that coincidentally equals an M/I/D name is NOT removed);
-- net_firms_removed is the true reduction. Quantifies the (a)-PR Option-B change.
with firm_elements as (
  select 'manufacturer' as role, upper(trim(m.value->>'name')) as normalized_name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'retailer', upper(trim(m.value->>'name'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'importer', upper(trim(m.value->>'name'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'distributor', upper(trim(m.value->>'name'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select
  count(distinct normalized_name) as firms_all_four_roles,
  count(distinct normalized_name) filter (where role <> 'retailer') as firms_mid_only,
  count(distinct normalized_name) filter (where role = 'retailer') as retailer_only_distinct_names,
  count(distinct normalized_name)
    - count(distinct normalized_name) filter (where role <> 'retailer') as net_firms_removed,
  round(100.0 * (count(distinct normalized_name)
    - count(distinct normalized_name) filter (where role <> 'retailer'))
    / nullif(count(distinct normalized_name), 0), 1) as pct_firm_dim_reduction
from firm_elements;

\echo ''
\echo '=== Q3: Bug-2 suffix-strip collapse SIMULATION (manufacturer/importer/distributor) ==='
-- Read-only lower-bound: distinct firms now vs after the comma-anchored ", of|dba" strip.
-- firms_collapsed = the Bug-2 fragmentation the strip would resolve. Retailer excluded (it
-- leaves the dim under Option B). Feeds the ADR threshold + the 6b normalization scope.
with mid_elements as (
  select 'manufacturer' as role,
         upper(trim(m.value->>'name')) as normalized_name,
         upper(trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i'))) as stripped_name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'importer',
         upper(trim(m.value->>'name')),
         upper(trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i')))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select 'distributor',
         upper(trim(m.value->>'name')),
         upper(trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i')))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select
  coalesce(role, '(all M/I/D)') as role,
  count(*) as elements,
  count(distinct normalized_name) as distinct_current,
  count(distinct stripped_name) as distinct_after_strip,
  count(distinct normalized_name) - count(distinct stripped_name) as firms_collapsed,
  round(100.0 * (count(distinct normalized_name) - count(distinct stripped_name))
    / nullif(count(distinct normalized_name), 0), 1) as pct_collapse
from mid_elements
group by grouping sets ((role), ())
order by elements desc;

\echo ''
\echo '=== Q4: strippable-suffix prevalence (M/I/D) — regex coverage + 6b headroom ==='
-- What fraction of M/I/D names carry each suffix pattern. comma_of_geo + comma_dba are what
-- Q3 strips; space_dba_no_comma + parenthetical are the residual the conservative strip
-- leaves (the extra collapse real 6b normalization would add on top of Q3).
with mid_names as (
  select m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select
  count(*) as elements,
  count(*) filter (where name ~* ',\s*of\s+') as comma_of_geo,
  count(*) filter (where name ~* ',\s*(dba|d/b/a|doing business as)\s+') as comma_dba,
  count(*) filter (where name ~* '\y(dba|d/b/a|doing business as)\y') as dba_any_form,
  count(*) filter (where name ~* '\(.+\)') as parenthetical,
  count(*) filter (where name ~* ',\s*(of|dba|d/b/a|doing business as)\s+') as comma_strippable_total,
  round(100.0 * count(*) filter (where name ~* ',\s*(of|dba|d/b/a|doing business as)\s+')
    / nullif(count(*), 0), 1) as pct_comma_strippable
from mid_names;

\echo ''
\echo '=== Q5: recurring-firm rate (names in >=2 distinct recalls), current vs stripped (M/I/D) ==='
-- Fragmentation suppresses cross-recall firm identity: today firms barely repeat (§3 Bug 2,
-- 87-94% distinct). recurring_firms_* counts names appearing in >=2 distinct recall_ids.
-- A rise from current -> stripped is the cross-recall identity the normalization buys back.
with mid_elements as (
  select recall_id,
         upper(trim(m.value->>'name')) as normalized_name,
         upper(trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i'))) as stripped_name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select recall_id, upper(trim(m.value->>'name')),
         upper(trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i')))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select recall_id, upper(trim(m.value->>'name')),
         upper(trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i')))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
),
cur as (
  select normalized_name, count(distinct recall_id) as recalls
  from mid_elements group by normalized_name
),
strp as (
  select stripped_name, count(distinct recall_id) as recalls
  from mid_elements group by stripped_name
)
select
  (select count(*) from cur) as distinct_firms_current,
  (select count(*) from cur where recalls >= 2) as recurring_firms_current,
  (select count(*) from strp) as distinct_firms_stripped,
  (select count(*) from strp where recalls >= 2) as recurring_firms_stripped;

\echo ''
\echo '=== Q6: sample suffix-strip collapses (qualitative ADR / consolidation evidence) ==='
-- Concrete before -> after pairs where the conservative strip changed the name. Makes the
-- Bug-2 magnitude self-evident in the output (e.g. "ZOLIQUEX, of China" -> "ZOLIQUEX").
with mid_elements as (
  select m.value->>'name' as raw_name,
         trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i')) as stripped_name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name',
         trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
  union all
  select m.value->>'name',
         trim(regexp_replace(m.value->>'name',
           ',\s*(of|dba|d/b/a|doing business as)\s+.*$', '', 'i'))
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where nullif(trim(m.value->>'name'), '') is not null
)
select distinct left(raw_name, 90) as raw_name, left(stripped_name, 60) as stripped_name
from mid_elements
where raw_name <> stripped_name
order by 1
limit 20;
