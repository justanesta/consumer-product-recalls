-- USDA FSIS first-extraction verification queries (Phase 5b).
--
-- Run after `uv run recalls extract usda` against Neon dev to populate the
-- numbers in documentation/usda/first_extraction_findings.md. Cross-references
-- Finding B (cardinality probe) and Finding J (active vs archived breakdown).
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/verify_usda_first_extraction.sql
-- Or (if you don't have $NEON_DATABASE_URL exported):
--   psql "<connection-string>" -f scripts/sql/verify_usda_first_extraction.sql
--
-- Re-runnable; queries are read-only.

\echo
\echo '=== 1) Bronze cardinality (target ~2001 from Finding B) ==='
SELECT COUNT(*) AS bronze_total FROM usda_fsis_recalls_bronze;

\echo
\echo '=== 2) Langcode breakdown (Finding B targets: English ~1212, Spanish ~789) ==='
SELECT langcode, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY langcode
 ORDER BY langcode;

\echo
\echo '=== 3) Archive breakdown (Finding J targets: archived ~1829, active ~172) ==='
SELECT archive_recall, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY archive_recall
 ORDER BY archive_recall;

\echo
\echo '=== 4) Active-Spanish breakdown (Finding J: only ~5 active Spanish records) ==='
SELECT langcode, archive_recall, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY langcode, archive_recall
 ORDER BY langcode, archive_recall;

\echo
\echo '=== 5) active_notice nullability check (post-Finding-C-addendum fix) ==='
SELECT
  COUNT(*) FILTER (WHERE active_notice IS TRUE)  AS active_notice_true,
  COUNT(*) FILTER (WHERE active_notice IS FALSE) AS active_notice_false,
  COUNT(*) FILTER (WHERE active_notice IS NULL)  AS active_notice_null
FROM usda_fsis_recalls_bronze;

\echo
\echo '=== 6) Rejected table (expect 0 if no bilingual orphans this batch) ==='
SELECT COUNT(*) AS rejected_total FROM usda_fsis_recalls_rejected;
SELECT failure_stage, COUNT(*) AS n
  FROM usda_fsis_recalls_rejected
 GROUP BY failure_stage
 ORDER BY n DESC;

\echo
\echo '=== 7) Watermark state (etag should be NULL since etag_enabled=false) ==='
\x on
SELECT source, last_cursor, last_etag, last_successful_extract_at, updated_at
  FROM source_watermarks
 WHERE source = 'usda';
\x off

\echo
\echo '=== 8) Extraction run history (most recent 5 USDA runs) ==='
SELECT
  status,
  records_extracted,
  records_inserted,
  records_rejected,
  started_at,
  finished_at
FROM extraction_runs
 WHERE source = 'usda'
 ORDER BY started_at DESC
 LIMIT 5;

\echo
\echo '=== 9) Sample bronze row (sanity check shape) ==='
\x on
SELECT
  source_recall_id,
  langcode,
  recall_date,
  recall_classification,
  archive_recall,
  active_notice,
  has_spanish,
  last_modified_date,
  establishment,
  recall_url
FROM usda_fsis_recalls_bronze
 ORDER BY recall_date DESC
 LIMIT 1;
\x off
