\set ON_ERROR_STOP on
set client_min_messages = warning;
\pset null '<NULL>'
\echo '=== Tiered firm-resolution cluster review (Phase 6b PR 6b.4) ==='
-- Run AFTER `recalls resolve-firms`. The tiered resolver (Tier 0 current-FEI / Tier 1 name
-- repair / Tier 2 entity rollup) collapsed clean names onto a shared canonical_firm_id. The
-- EYEBALL TIER is `rapidfuzz_rollup` (Tier 2): its residual false-merge mode is a geographic
-- 2-token coincidence (unrelated "San Antonio ..." firms). `fei_exact` (Tier 0) and
-- name_variant_exact / name_typo_high (Tier 1) are authoritative/safe — focus review on the
-- largest rapidfuzz_rollup clusters. Fix a place hub in src/enrichment/place_words.py, an FEI
-- hub via diagnose_fei_fanout.sql + FEI_FANOUT_CAP (see operations.md review loop), then re-run.
--
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/verify_fuzzy_clusters.sql

\echo ''
\echo '=== Q1: match_confidence distribution (the resolution path per row) ==='
select match_confidence, count(*) as rows,
       round(avg(match_score), 1) as avg_fuzzy_score
from firm_crosswalk
group by match_confidence
order by rows desc;

\echo ''
\echo '=== Q2: cluster-size summary (canonical_firm_ids grouping >1 raw name) ==='
with sizes as (
  select canonical_firm_id, count(*) as n
  from firm_crosswalk group by canonical_firm_id
)
select
  count(*) filter (where n = 1)  as singletons,
  count(*) filter (where n > 1)  as multi_clusters,
  count(*) filter (where n >= 5) as clusters_ge5,
  max(n)                         as largest_cluster
from sizes;

\echo ''
\echo '=== Q3: 30 largest clusters — EYEBALL for a group of unrelated firms (a hub) ==='
select
  count(*) as n,
  min(canonical_name)                                          as canonical,
  string_agg(distinct match_confidence, '/')                   as how,
  left(string_agg(distinct clean_name, ' | ' order by clean_name), 130) as members
from firm_crosswalk
group by canonical_firm_id
having count(*) > 1
order by n desc, canonical
limit 30;

\echo ''
\echo '=== Full multi-member clusters -> data/exploratory/cross_source/fuzzy_clusters.csv ==='
\echo '   (Every cluster + members, largest first — review the rapidfuzz_rollup clusters offline.)'
\pset format csv
\o data/exploratory/cross_source/fuzzy_clusters.csv
with grouped as (
  select
    canonical_firm_id,
    count(*) as n,
    min(canonical_name) as canonical,
    string_agg(distinct match_confidence, '/') as how,
    string_agg(distinct clean_name, ' | ' order by clean_name) as members
  from firm_crosswalk
  group by canonical_firm_id
  having count(*) > 1
)
select n, canonical, how, members from grouped order by n desc, canonical;
\o
\pset format aligned
\echo '   done — Read data/exploratory/cross_source/fuzzy_clusters.csv'
