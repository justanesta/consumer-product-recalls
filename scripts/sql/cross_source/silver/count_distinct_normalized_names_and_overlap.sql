-- Corpus gate G0 — size the cross-source firm-resolution input (Phase 6b).
--
-- PURPOSE: before building the RapidFuzz crosswalk (PR 6b.4) and wiring the
-- additive canonical_firm_id regroup, measure (a) how many distinct normalized
-- firm names the resolver must cluster, (b) how much cross-source name overlap
-- already collapses for free, and (c) whether a brute-force O(n^2) pairwise pass
-- is feasible or a first-token blocking key is required. Also sizes the
-- canonical_firm_id re-key blast radius (= total distinct firm rows).
--
-- Mirrors firm.sql's all_normalized CTE EXACTLY (same per-source name fields,
-- same upper(trim()) normalization, same not-null/non-empty filters, same USCG
-- directory LEFT JOIN) so the counts match what the resolver will actually see.
-- Reads the staging views + silver firm unqualified (search_path includes the
-- dbt schema, per scripts/sql/uscg_manufacturers/silver/measure_rescue_and_coverage.sql).
--
-- GATES:
--   * brute-force vs blocking in `recalls resolve-firms` (PR 6b.4) — compare the
--     no-blocking pair count (Q3) against the within-first-token-block pair sum.
--   * the rapidfuzz threshold sizing input (paired with the 6b.4 residual gate).
--   * the canonical_firm_id re-key magnitude (Q2 total distinct).
--
-- Feeds: project_scope/phase-6b-execution-plan.md PR 6b.0 / 6b.4.
-- Run with: psql "$NEON_DATABASE_URL" -f scripts/sql/cross_source/silver/count_distinct_normalized_names_and_overlap.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

-- Per-source normalized firm names (one row per (source, normalized_name)
-- occurrence; dedup happens in the queries below). Reconstructs firm.sql.
-- NOTE: session-scoped temp table, NOT `on commit drop` — psql autocommits each
-- statement, so `on commit drop` would drop the table before the next query reads
-- it. It is auto-dropped when the psql -f session ends; the guard makes a re-run
-- in the same interactive session safe.
drop table if exists _src_names;
create temporary table _src_names as
with cpsc_arrays as (
    select 'CPSC' as source, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) as firm_json from stg_cpsc_recalls
    union all
    select 'CPSC' as source, jsonb_array_elements(coalesce(importers, '[]'::jsonb))     as firm_json from stg_cpsc_recalls
    union all
    select 'CPSC' as source, jsonb_array_elements(coalesce(distributors, '[]'::jsonb))  as firm_json from stg_cpsc_recalls
),
cpsc as (
    select source, upper(trim(firm_json ->> 'name')) as normalized_name
    from cpsc_arrays
    where (firm_json ->> 'name') is not null and trim(firm_json ->> 'name') <> ''
),
fda as (
    select 'FDA' as source, upper(trim(firm_legal_nam)) as normalized_name
    from stg_fda_recalls
    where firm_legal_nam is not null and trim(firm_legal_nam) <> ''
),
usda as (
    select 'USDA' as source, upper(trim(establishment)) as normalized_name
    from stg_usda_fsis_recalls
    where establishment is not null and trim(establishment) <> ''
),
nhtsa as (
    select 'NHTSA' as source, upper(trim(mfgname)) as normalized_name
    from stg_nhtsa_recalls
    where mfgname is not null and trim(mfgname) <> ''
    union all
    select 'NHTSA' as source, upper(trim(mfgtxt)) as normalized_name
    from stg_nhtsa_recalls
    where mfgtxt is not null and trim(mfgtxt) <> ''
),
uscg as (
    select 'USCG' as source,
           upper(trim(coalesce(m.company_name, r.company_name, r.mic))) as normalized_name
    from stg_uscg_recalls r
    left join stg_uscg_manufacturers m on upper(trim(r.mic)) = upper(trim(m.mic))
    where coalesce(m.company_name, r.company_name, r.mic) is not null
      and trim(coalesce(m.company_name, r.company_name, r.mic)) <> ''
)
select source, normalized_name from cpsc
union all select source, normalized_name from fda
union all select source, normalized_name from usda
union all select source, normalized_name from nhtsa
union all select source, normalized_name from uscg;

\echo '=== Q1: distinct normalized firm names per source ==='
-- Per-source contribution to the resolution universe (NHTSA carries both filer +
-- manufacturer names, so it is the heaviest).
select source, count(distinct normalized_name) as distinct_names
from _src_names
group by source
order by distinct_names desc;

\echo '=== Q2: total distinct names + cross-source overlap + firm-dim cross-check ==='
-- total_distinct should equal `select count(*) from firm` (firm is one row per
-- normalized_name). multi_source_names = names already shared across >=2 sources
-- (collapsed for free by the existing md5(normalized_name) key — NOT new fuzzy work).
select
    (select count(distinct normalized_name) from _src_names)                    as total_distinct_names,
    (select count(*) from firm)                                                 as firm_dim_rows,
    (select count(*) from (
        select normalized_name
        from _src_names
        group by normalized_name
        having count(distinct source) >= 2
     ) x)                                                                       as multi_source_names;

\echo '=== Q2b: sample of cross-source-shared names (already collapse on exact match) ==='
select normalized_name, count(distinct source) as n_sources, string_agg(distinct source, ',' order by source) as sources
from _src_names
group by normalized_name
having count(distinct source) >= 2
order by n_sources desc, normalized_name
limit 40;

\echo '=== Q3: blocking feasibility — first-token block sizes + pairwise comparison counts ==='
-- Brute-force compares every pair: N*(N-1)/2. Blocking on the first token reduces
-- it to the SUM of within-block pair counts. Compare blocked_pairs vs bruteforce_pairs
-- to decide whether `recalls resolve-firms` needs the blocking key on by default.
with distinct_names as (
    select distinct normalized_name from _src_names
),
blocks as (
    select split_part(normalized_name, ' ', 1) as block_key, count(*) as n_names
    from distinct_names
    group by split_part(normalized_name, ' ', 1)
)
select
    (select count(*) from distinct_names)                                       as total_distinct_names,
    (select (count(*)::bigint * (count(*) - 1)) / 2 from distinct_names)         as bruteforce_pairs,
    (select coalesce(sum((n_names::bigint * (n_names - 1)) / 2), 0) from blocks) as blocked_pairs,
    (select count(*) from blocks)                                               as n_blocks,
    (select max(n_names) from blocks)                                           as largest_block_names;

\echo '=== Q3b: the 25 largest first-token blocks (watch for over-coarse blocking) ==='
-- A huge block (e.g. a generic leading token) means blocking does little there;
-- a long tail of tiny blocks means blocking is very effective. Informs whether a
-- smarter block key (e.g. first two tokens) is worth it in 6b.4.
with distinct_names as (
    select distinct normalized_name from _src_names
)
select split_part(normalized_name, ' ', 1) as block_key,
       count(*)                            as n_names,
       (count(*)::bigint * (count(*) - 1)) / 2 as within_block_pairs
from distinct_names
group by split_part(normalized_name, ' ', 1)
order by n_names desc
limit 25;
