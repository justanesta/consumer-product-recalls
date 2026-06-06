\set ON_ERROR_STOP on
\echo '=== Verify AC DELCO product-merge (Phase 6b PR 6b.3) — run AFTER building recall_product ==='
-- Confirms the maketxt-normalization fix landed: one recall_product row per NHTSA
-- (campno, normalized make, ...identity), displayed maketxt kept RAW (critic C14).
-- Run: psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/verify_ac_delco_product_merge.sql

\echo '=== Q1: recall_product rows for campno 22E002000 (the documented AC DELCO case) ==='
-- POST-FIX expectation: <= 1 row per identity, maketxt_displayed = latest survivor
-- spelling. May be 0 rows if 22E002000 (a 2022 recall) is outside the current
-- --since=2023-12-01 slice — then the fix is preventive until the 6a.5 full seed.
select
  recall_product_id,
  source_specific_attrs ->> 'maketxt' as maketxt_displayed,
  product_name,
  model
from recall_product
where source = 'NHTSA' and source_recall_id = '22E002000'
order by recall_product_id;

\echo ''
\echo '=== Q2: displayed maketxt stays RAW (internal spaces preserved) ==='
-- Proves the normalization touched only the surrogate key, not the displayed value:
-- multi-word makes ("LAND ROVER") still show their spaces in source_specific_attrs.
select recall_product_id, source_specific_attrs ->> 'maketxt' as maketxt_displayed
from recall_product
where source = 'NHTSA'
  and (source_specific_attrs ->> 'maketxt') ~ '\s'
limit 10;

\echo ''
\echo '=== Q3: recall_product_id is unique across NHTSA (the fix must not duplicate) ==='
-- The dbt unique test already enforces this globally; this is a human-readable echo
-- that the maketxt collapse produced 0 duplicate surrogate keys for NHTSA.
select count(*) as nhtsa_rows, count(distinct recall_product_id) as distinct_ids
from recall_product
where source = 'NHTSA';
