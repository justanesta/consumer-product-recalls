-- Verify the Phase 6d re-ingest (R2 replay) lineage + the two designed-out silver bugs.
--
-- Run AFTER a `recalls re-ingest <source> ... --change-type schema_rebaseline` run. Re-ingest runs
-- are identified unambiguously by `extraction_runs.replayed_from_run_id IS NOT NULL` (migration
-- 0029) — NOT by change_type, because a normal `recalls extract|deep-rescan
-- --change-type=schema_rebaseline` also writes a `schema_rebaseline` run (and for USDA correctly
-- writes a presence manifest, since it re-enumerates the full corpus). Only re-ingest runs must
-- have no manifest and must not move the watermark.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/_pipeline/verify_reingest_lineage.sql
--
-- Read-only; re-runnable.

\set ON_ERROR_STOP on

\echo
\echo '=== 1) Every re-baseline run (newest first), split by lineage. is_reingest=t are the runs'
\echo '       YOUR `recalls re-ingest` created (replayed_from_run_id set); is_reingest=f are normal'
\echo '       extract/deep-rescan re-baselines. Re-ingest runs should show manifest_rows=0; extract'
\echo '       re-baselines correctly show manifest_rows>0 for USDA (full re-enumeration). ==='
SELECT er.source,
       er.started_at,
       (er.replayed_from_run_id IS NOT NULL) AS is_reingest,
       er.replayed_from_run_id,
       er.records_inserted,
       count(eri.id) AS manifest_rows
  FROM extraction_runs er
  LEFT JOIN extraction_run_identities eri ON eri.run_id = er.run_id
 WHERE er.change_type IN ('schema_rebaseline', 'hash_helper_rebaseline')
 GROUP BY er.run_id, er.source, er.started_at, er.replayed_from_run_id, er.records_inserted
 ORDER BY er.started_at DESC;

\echo
\echo '=== 2) CRITICAL — re-ingest runs (replayed_from_run_id IS NOT NULL) that wrote a presence'
\echo '       manifest. MUST be 0: a replay must never assert presence (it would become'
\echo '       usda_latest_run and corrupt recall_lifecycle). Normal extract re-baselines are'
\echo '       excluded from this check by the replayed_from_run_id filter. ==='
SELECT count(*) AS reingest_runs_with_manifest,
       CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL — investigate immediately' END AS verdict
  FROM extraction_runs er
  JOIN extraction_run_identities eri ON eri.run_id = er.run_id
 WHERE er.replayed_from_run_id IS NOT NULL;

\echo
\echo '=== 3) Freshness watermark snapshot. A re-ingest bypasses the source load_bronze, so'
\echo '       last_successful_extract_at must be UNCHANGED by the replay — compare this against'
\echo '       the value you noted BEFORE running re-ingest (especially for usda). ==='
SELECT source, last_successful_extract_at, updated_at
  FROM source_watermarks
 ORDER BY source;
