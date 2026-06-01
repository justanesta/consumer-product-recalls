-- Neon branch storage footprint — per-table + total, largest first.
--
-- Use during the Phase 6a.5 seed to confirm tier headroom (NHTSA is the
-- storage-dominant step; FDA's codeinformation TOAST is the next big one).
--
-- NOTE: this is the *logical* Postgres size. Neon's BILLED storage (shown in the
-- console) differs — it includes WAL/history retention and copy-on-write sharing
-- with the parent branch, so the console number is the source of truth for the
-- 10 GB Launch-tier quota. This query shows where the bytes live and is a good
-- proxy for the data footprint. For the billed figure: Neon console → your
-- project → Branches → main → "Data size" (or the project Usage/Monitoring tab).
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/storage_usage.sql

\pset null '<NULL>'

\echo '=== total logical database size (proxy; see header re: Neon billed storage) ==='
select pg_size_pretty(pg_database_size(current_database())) as database_size;

\echo '=== per-table size, largest first ==='
select
    relname                                                          as table,
    to_char(n_live_tup, 'FM999,999,999')                            as approx_rows,
    pg_size_pretty(pg_total_relation_size(relid))                   as total_size,
    pg_size_pretty(pg_relation_size(relid))                         as heap_size,
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as index_plus_toast
from pg_stat_user_tables
order by pg_total_relation_size(relid) desc;
