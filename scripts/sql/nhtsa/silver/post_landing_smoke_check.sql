-- Phase 5c Step 5 — silver-layer smoke check after NHTSA landing.
--
-- Run after `dbt build --select +silver --target main` to verify the silver
-- tables look plausible. Complements `dbt test` (which checks structural
-- properties like uniqueness and referential integrity) by sanity-checking
-- the actual row counts and the documented v1 drift case.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/post_landing_smoke_check.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: per-source recall_event counts ==='
\echo 'NHTSA n_events ≈ count(distinct campno) in nhtsa_recalls_bronze.'
\echo 'For the current --since=2023-12-01 dev slice that is ~7-8k events.'

select source, count(*) as n_events
from recall_event
group by source
order by source;

\echo
\echo '=== Q2: per-source recall_product counts ==='
\echo 'NHTSA n_products ≈ count of distinct 11-tuples in nhtsa_recalls_bronze'
\echo '(the 11-tuple is the silver recall_product grain per ADR 0031 option 3b).'
\echo 'Should match verify_eleven_tuple_row_unique.sql output for NHTSA.'

select source, count(*) as n_products
from recall_product
group by source
order by source;

\echo
\echo '=== Q3: NHTSA recall_product fragmentation baseline ==='
\echo 'Documented v1 drift case from documentation/nhtsa/incremental_delta_findings.md'
\echo 'Section G — campno 22E002000 (Toyota lower ball joint) had maketxt drift'
\echo 'between the 2026-05-07 and 2026-05-08 captures (AC DELCO → ACDELCO).'
\echo
\echo 'Expected: 2 silver recall_product rows under the same recall_event,'
\echo 'with both maketxt variants visible in the source_specific_attrs JSONB.'
\echo 'If you only see 1 row, the drift never reached your bronze (which is'
\echo 'fine — it just means today''s slice happens to include only one version).'

select
    p.recall_event_id,
    e.source_recall_id              as campno,
    p.recall_product_id,
    p.source_specific_attrs ->> 'maketxt'   as maketxt,
    p.source_specific_attrs ->> 'modeltxt'  as modeltxt,
    p.source_specific_attrs ->> 'yeartxt'   as yeartxt
from recall_product p
join recall_event   e  on p.recall_event_id = e.recall_event_id
where e.source = 'NHTSA' and e.source_recall_id = '22E002000'
order by p.recall_product_id;

\echo
\echo '=== Q4: cross-source firm-dedup spot check ==='
\echo 'Firms whose normalized_name appears across multiple sources collapse to'
\echo 'one firm row per ADR 0002. NHTSA contributes mfgname; CPSC/FDA/USDA'
\echo 'contribute their own firm names. Honda is the canonical example —'
\echo 'expected to appear via NHTSA (recalls) + possibly CPSC if any.'

select
    f.firm_id,
    f.canonical_name,
    f.normalized_name,
    jsonb_array_length(f.observed_names) as n_observed_name_variants,
    f.observed_names
from firm f
where f.normalized_name like '%HONDA%'
order by f.normalized_name
limit 10;

\echo
\echo '=== Q5: NHTSA-specific firm count ==='
\echo 'Firms that appear in any NHTSA recall_event. Use this as a coarse signal'
\echo 'for "did the bridge populate correctly" — should be ~hundreds for the'
\echo 'current dev slice.'

select count(distinct firm_id) as nhtsa_firm_count
from recall_event_firm ref
join recall_event       e on ref.recall_event_id = e.recall_event_id
where e.source = 'NHTSA';

\echo
\echo '=== Q6: orphan check — NHTSA recall_products without a recall_event ==='
\echo 'Should be 0. Already covered by `dbt test assert_no_orphan_products`'
\echo 'but worth a manual confirmation.'

select count(*) as orphan_nhtsa_products
from recall_product p
left join recall_event e on p.recall_event_id = e.recall_event_id
where p.source = 'NHTSA' and e.recall_event_id is null;
