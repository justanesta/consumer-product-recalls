\set ON_ERROR_STOP on
\pset null '<NULL>'
-- Why might recall_product_history not show in the public table list? Three candidates:
--   (a) it's a VIEW (materialized='view'), so a Tables-only browser pane hides it;
--   (b) it's alphabetically right after recall_product (often just below the fold);
--   (c) it was never built because nhtsa_recall_product_snapshot doesn't exist
--       (no `dbt snapshot`/no NHTSA data on this DB) — a view over a missing relation can't be created.
-- This settles which.
--
-- Run: psql -f scripts/sql/nhtsa/silver/diagnose_product_history_presence.sql

\echo '=== Q1: the two relations — present? TABLE or VIEW? which schema? ==='
select table_schema, table_name, table_type
from information_schema.tables
where table_name in ('recall_product_history', 'nhtsa_recall_product_snapshot')
order by table_name, table_schema;

\echo ''
\echo '=== Q2: relation existence via to_regclass (NULL = not built, no error) ==='
select
    to_regclass('public.recall_product_history')                  as public_recall_product_history,
    to_regclass('silver_snapshots.nhtsa_recall_product_snapshot') as nhtsa_snapshot;

\echo ''
\echo '=== Q3: is NHTSA actually populated on THIS database? bronze + silver counts ==='
select
    (select count(*) from nhtsa_recalls_bronze)                  as nhtsa_bronze_rows,
    (select count(*) from recall_product where source = 'NHTSA') as nhtsa_recall_product_rows;
