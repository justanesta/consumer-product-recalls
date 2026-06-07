-- Reset the NHTSA recall-product snapshot so it re-initializes against the 7-tuple anchor
-- (ADR 0033 2026-06-06 amendment; Phase 6c.6). The prior build keyed it on the 6-tuple
-- recall_product_id; changing the recipe would otherwise leave the old 6-tuple rows orphaned as
-- phantom-"current" rows alongside the new 7-tuple ones (dbt snapshot does not hard-delete missing
-- keys by default). The snapshot holds only today's initial bank (no real history yet), so dropping
-- it is safe — dbt re-creates + re-initializes it on the next `dbt build`. dbt snapshots are
-- intentionally exempt from `--full-refresh` (history protection), so a manual drop is the standard
-- dev reset.
--
-- Two dbt VIEWS depend on the snapshot (recall_product_v15, recall_product_history) — Postgres
-- views hard-depend on their source table. We drop them explicitly FIRST (in dependency order) so we
-- never need DROP ... CASCADE. Both are stateless views that the next `dbt build` recreates; nothing
-- else references the snapshot (v1 recall_product, recall_event, gold do NOT). Run from repo root
-- BEFORE rebuilding:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/silver/reset_nhtsa_recall_product_snapshot.sql

\set ON_ERROR_STOP on

drop view if exists public.recall_product_v15;
drop view if exists public.recall_product_history;
drop table if exists silver_snapshots.nhtsa_recall_product_snapshot;
