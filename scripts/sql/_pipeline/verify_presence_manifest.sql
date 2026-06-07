-- Verify the ADR 0026 per-run presence manifest (extraction_run_identities, Phase 6c.0).
--
-- Run after a successful USDA extract / deep-rescan. Confirms the manifest is written only
-- for track_presence sources (USDA today), captures the recall-grain + langcode split,
-- and proves FK integrity to extraction_runs.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/_pipeline/verify_presence_manifest.sql
--
-- Read-only; re-runnable.

\set ON_ERROR_STOP on

\echo
\echo '=== 1) Manifest is USDA-only (the default_track_presence gate) ==='
SELECT source, count(*) AS manifest_rows, count(DISTINCT run_id) AS runs
  FROM extraction_run_identities
 GROUP BY source
 ORDER BY source;

\echo
\echo '=== 2) Latest enumerating USDA run: rows, distinct recall numbers, langcode split ==='
WITH latest AS (  -- latest ENUMERATING run (wrote manifest rows), not latest success: a
                  -- 304-Not-Modified run succeeds but enumerates nothing, so latest-success
                  -- would read as empty. Same rule recall_lifecycle (6c.2) uses.
  SELECT eri.run_id
    FROM extraction_run_identities eri
    JOIN extraction_runs er ON er.run_id = eri.run_id
   WHERE er.source = 'usda'
   GROUP BY eri.run_id, er.started_at
   ORDER BY er.started_at DESC
   LIMIT 1
)
SELECT
  (SELECT run_id FROM latest)                                      AS run_id,
  count(*)                                                         AS manifest_rows,
  count(DISTINCT source_recall_id)                                 AS distinct_recall_ids,
  count(*) FILTER (WHERE langcode = 'English')                     AS english,
  count(*) FILTER (WHERE langcode = 'Spanish')                     AS spanish,
  count(*) FILTER (WHERE langcode IS NULL)                         AS null_langcode
  FROM extraction_run_identities
 WHERE run_id = (SELECT run_id FROM latest);

\echo
\echo '=== 3) FK integrity: every manifest run_id resolves to an extraction_runs row ==='
SELECT count(*) AS orphan_manifest_rows
  FROM extraction_run_identities eri
  LEFT JOIN extraction_runs er ON er.run_id = eri.run_id
 WHERE er.run_id IS NULL;

\echo
\echo '=== 4) Sample rows (latest enumerating USDA run) ==='
WITH latest AS (  -- latest ENUMERATING run (wrote manifest rows), not latest success: a
                  -- 304-Not-Modified run succeeds but enumerates nothing, so latest-success
                  -- would read as empty. Same rule recall_lifecycle (6c.2) uses.
  SELECT eri.run_id
    FROM extraction_run_identities eri
    JOIN extraction_runs er ON er.run_id = eri.run_id
   WHERE er.source = 'usda'
   GROUP BY eri.run_id, er.started_at
   ORDER BY er.started_at DESC
   LIMIT 1
)
SELECT source_recall_id, langcode
  FROM extraction_run_identities
 WHERE run_id = (SELECT run_id FROM latest)
 ORDER BY source_recall_id, langcode
 LIMIT 6;
