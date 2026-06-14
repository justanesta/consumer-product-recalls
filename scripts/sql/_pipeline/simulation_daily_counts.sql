-- H-a production-simulation artifact: per-source loaded counts (the "actual" side of the
-- expected-vs-actual record kept under documentation/<source>/). Read-only — runs as either role.
--
--     psql -f scripts/sql/_pipeline/simulation_daily_counts.sql
--
-- Section A is the per-run log for the last 3 days (records_inserted = loaded; records_rejected
-- routes to the *_rejected audit tables). Section B is the cumulative bronze row count per source,
-- so you can diff day-over-day growth against the per-source baselines in the WS-H plan
-- (CPSC ~9k+backfill, FDA ~API total, USDA ~2k, NHTSA ~322k, USCG TBD).

\echo '=== Section A: extraction_runs — per source/day for the last 3 days ==='
SELECT
    source,
    started_at::date          AS run_date,
    status,
    change_type,
    records_extracted,
    records_inserted          AS records_loaded,
    records_rejected,
    was_short_circuited,
    finished_at - started_at  AS duration
FROM extraction_runs
WHERE started_at >= current_date - INTERVAL '3 days'
ORDER BY run_date DESC, source, started_at DESC;

\echo ''
\echo '=== Section B: cumulative bronze row count per source (diff this day-over-day) ==='
SELECT 'cpsc_recalls'               AS source, count(*) AS bronze_rows FROM cpsc_recalls_bronze
UNION ALL SELECT 'fda_recalls',                count(*) FROM fda_recalls_bronze
UNION ALL SELECT 'fda_press_releases',         count(*) FROM fda_press_releases_bronze
UNION ALL SELECT 'nhtsa_recalls',              count(*) FROM nhtsa_recalls_bronze
UNION ALL SELECT 'uscg_recalls',               count(*) FROM uscg_recalls_bronze
UNION ALL SELECT 'uscg_manufacturers',         count(*) FROM uscg_manufacturers_bronze
UNION ALL SELECT 'uscg_manufacturer_details',  count(*) FROM uscg_manufacturer_details_bronze
UNION ALL SELECT 'usda_fsis_recalls',          count(*) FROM usda_fsis_recalls_bronze
UNION ALL SELECT 'usda_fsis_establishments',   count(*) FROM usda_fsis_establishments_bronze
ORDER BY source;
