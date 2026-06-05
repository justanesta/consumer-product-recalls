-- VERIFY — structure of the Tier-2 (rapidfuzz_rollup) merges, to scope the false-merge fix and the
-- cross-source-downweight decision (6b.4 precision follow-up). Read-only.
--
-- Answers: (Q1) are rollup merges mostly small "2-pair" welds or larger? (Q2) are they cross-source
-- (disjoint regulatory domain) or within-source? (Q3/Q4) dump the candidates so we can SEE how many
-- the place/generic denylist would catch vs how many need the cross-source prior.
-- rapidfuzz_rollup = the only tier that can false-merge distinct entities; Tier-1 (name_variant /
-- typo) and exact are handled by the other fixes. Decision home: ADR 0037 + the 6b.6 thread.
--
-- Run from repo root (Q3/Q4 dump to data/exploratory/cross_source/, gitignored):
--   psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/verify_rollup_false_merge_structure.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: rollup firms — observed-names size distribution (the "mostly 2-pairs?" question) ==='
-- NB observed_names counts raw variants, so it OVER-states distinct entities welded (geo/punct
-- variants inflate it); size=2 is an unambiguous 2-entity weld, larger needs the dump to judge.
with rollup_canon as (
  select distinct canonical_firm_id from firm_crosswalk where match_confidence = 'rapidfuzz_rollup'
)
select jsonb_array_length(f.observed_names) as observed_names, count(*) as n_rollup_firms
from rollup_canon rc
join firm f on f.firm_id = rc.canonical_firm_id
group by 1
order by 1;

\echo ''
\echo '=== Q2: rollup firms — cross-source vs within-source, split by size (the downweight value test) ==='
-- If rollup false-merges are mostly cross_source=true AND size_2, the cross-source downweight is
-- high-value + simple. If many are within-source, the denylist/distinctiveness fix is the real lever.
with rollup_canon as (
  select distinct canonical_firm_id from firm_crosswalk where match_confidence = 'rapidfuzz_rollup'
),
fsrc as (
  select ref.firm_id, count(distinct re.source) as n_src
  from recall_event_firm ref
  join recall_event re using (recall_event_id)
  group by 1
)
select
  (coalesce(fs.n_src, 1) >= 2)                                              as cross_source,
  count(*)                                                                  as n_rollup_firms,
  count(*) filter (where jsonb_array_length(f.observed_names) = 2)          as size_2,
  count(*) filter (where jsonb_array_length(f.observed_names) >= 3)         as size_3plus
from rollup_canon rc
join firm f on f.firm_id = rc.canonical_firm_id
left join fsrc fs on fs.firm_id = f.firm_id
group by 1
order by 1;

\echo ''
\echo '=== Q3: dump the CROSS-SOURCE rollup firms (the highest-risk false-merge candidates) ==='
\copy (with rollup_canon as (select distinct canonical_firm_id from firm_crosswalk where match_confidence = 'rapidfuzz_rollup'), fsrc as (select ref.firm_id, count(distinct re.source) as n_src, string_agg(distinct re.source, ',' order by re.source) as sources from recall_event_firm ref join recall_event re using (recall_event_id) group by 1) select f.firm_id, f.canonical_name, jsonb_array_length(f.observed_names) as n_names, fs.n_src, fs.sources, f.observed_names from rollup_canon rc join firm f on f.firm_id = rc.canonical_firm_id join fsrc fs on fs.firm_id = f.firm_id where fs.n_src >= 2 order by jsonb_array_length(f.observed_names), f.canonical_name) to 'data/exploratory/cross_source/rollup_cross_source.csv' with (format csv, header true)

\echo ''
\echo '=== Q4: dump ALL small rollup firms (n_names <= 3), cross AND within source ==='
-- So we can see the WITHIN-source false-merges too (the cross-source rule cannot catch those).
\copy (with rollup_canon as (select distinct canonical_firm_id from firm_crosswalk where match_confidence = 'rapidfuzz_rollup'), fsrc as (select ref.firm_id, count(distinct re.source) as n_src, string_agg(distinct re.source, ',' order by re.source) as sources from recall_event_firm ref join recall_event re using (recall_event_id) group by 1) select f.firm_id, f.canonical_name, jsonb_array_length(f.observed_names) as n_names, coalesce(fs.n_src, 0) as n_src, fs.sources, f.observed_names from rollup_canon rc join firm f on f.firm_id = rc.canonical_firm_id left join fsrc fs on fs.firm_id = f.firm_id where jsonb_array_length(f.observed_names) <= 3 order by f.canonical_name) to 'data/exploratory/cross_source/rollup_small_all.csv' with (format csv, header true)

\echo ''
\echo 'done. Dumps: data/exploratory/cross_source/rollup_cross_source.csv + rollup_small_all.csv'
