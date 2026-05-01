-- USDA FSIS bronze data exploration queries (Phase 5b first-extraction findings).
--
-- Scope: "what does the data look like" — distinct from
-- `verify_usda_first_extraction.sql`, which answers "did the load work."
-- Output of this script feeds the section-by-section narrative in
-- `documentation/usda/first_extraction_findings.md` (modeled on the
-- corresponding CPSC and FDA findings docs).
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/explore_usda_bronze.sql
--
-- Re-runnable; queries are read-only.

\echo
\echo '############################################################'
\echo '# CADENCE & VOLUME'
\echo '############################################################'

\echo
\echo '=== 1) Records per recall_date (daily cadence) ==='
\echo '    Reveals publication rhythm. USDA does not publish on weekends/holidays;'
\echo '    expect gaps that align with the federal calendar.'
SELECT recall_date::date AS day, COUNT(*) AS records
  FROM usda_fsis_recalls_bronze
 GROUP BY recall_date::date
 ORDER BY recall_date::date DESC
 LIMIT 30;

\echo
\echo '=== 2) Weekly cadence (recent ~16 weeks) ==='
\echo '    Aggregates daily counts into weeks; reveals trends and active-day distribution.'
SELECT
  DATE_TRUNC('week', recall_date)::date AS week_start,
  COUNT(*) AS records,
  COUNT(DISTINCT recall_date::date) AS active_days
FROM usda_fsis_recalls_bronze
 WHERE recall_date >= NOW() - INTERVAL '16 weeks'
 GROUP BY DATE_TRUNC('week', recall_date)
 ORDER BY week_start DESC;

\echo
\echo '=== 3) Top spike days (highest single-day recall_date counts) ==='
\echo '    USDA should look like CPSC (no large multi-product spikes), not FDA.'
SELECT recall_date::date AS day, COUNT(*) AS records
  FROM usda_fsis_recalls_bronze
 GROUP BY recall_date::date
 ORDER BY records DESC
 LIMIT 5;

\echo
\echo '=== 4) Weekday gap analysis (recent 6 months only) ==='
\echo '    Weekdays with zero USDA activity. Expected = federal holidays.'
\echo '    Restricted to last 6 months because most archived records are decades old.'
WITH date_series AS (
  SELECT generate_series(
    (NOW() - INTERVAL '6 months')::date,
    NOW()::date,
    '1 day'::interval
  )::date AS day
),
active_days AS (
  SELECT DISTINCT recall_date::date AS day FROM usda_fsis_recalls_bronze
)
SELECT d.day, TO_CHAR(d.day, 'Day') AS day_name
  FROM date_series d
  LEFT JOIN active_days a ON d.day = a.day
 WHERE a.day IS NULL AND EXTRACT(DOW FROM d.day) NOT IN (0, 6)
 ORDER BY d.day;


\echo
\echo '############################################################'
\echo '# EDIT DETECTION (composite identity)'
\echo '############################################################'

\echo
\echo '=== 5) (source_recall_id, langcode) pairs with multiple content hashes ==='
\echo '    Validates the bilingual-dedup fix at scale: any rows here represent'
\echo '    real upstream edits FSIS made between snapshots. Zero rows = no edits.'
SELECT source_recall_id, langcode,
       COUNT(DISTINCT content_hash) AS hash_versions,
       COUNT(*) AS total_rows
  FROM usda_fsis_recalls_bronze
 GROUP BY source_recall_id, langcode
HAVING COUNT(DISTINCT content_hash) > 1
 ORDER BY hash_versions DESC, source_recall_id;

\echo
\echo '=== 6) Total rows vs distinct (source_recall_id, langcode) identities ==='
\echo '    Bronze rows may exceed unique identities by the count of captured edits.'
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT (source_recall_id, langcode)) AS unique_identities,
  COUNT(*) - COUNT(DISTINCT (source_recall_id, langcode)) AS history_rows_from_edits
FROM usda_fsis_recalls_bronze;

\echo
\echo '=== 7) Edit-history reference: PHA-04092026-01 versions ==='
\echo '    Phase 5b verification surfaced this record as having been edited'
\echo '    between extraction runs. Shows the bronze layer history pattern.'
SELECT source_recall_id, langcode, recall_date::date,
       last_modified_date::date AS lmd,
       LEFT(content_hash, 16) AS content_hash_prefix,
       extraction_timestamp
  FROM usda_fsis_recalls_bronze
 WHERE source_recall_id = 'PHA-04092026-01'
 ORDER BY extraction_timestamp DESC;


\echo
\echo '############################################################'
\echo '# BILINGUAL PAIR MODEL'
\echo '############################################################'

\echo
\echo '=== 8) Bilingual pair completeness (latest-version per identity) ==='
\echo '    Each unique field_recall_number should have either EN-only or both EN+ES.'
\echo '    Spanish-only would indicate an orphan; the bilingual invariant should'
\echo '    have caught and quarantined those at extract time.'
WITH latest AS (
  SELECT DISTINCT ON (source_recall_id, langcode)
    source_recall_id, langcode
  FROM usda_fsis_recalls_bronze
  ORDER BY source_recall_id, langcode, extraction_timestamp DESC
),
agg AS (
  SELECT source_recall_id,
         BOOL_OR(langcode = 'English') AS has_en,
         BOOL_OR(langcode = 'Spanish') AS has_es
  FROM latest
  GROUP BY source_recall_id
)
SELECT
  COUNT(*) FILTER (WHERE has_en AND has_es)        AS bilingual_pairs,
  COUNT(*) FILTER (WHERE has_en AND NOT has_es)    AS english_only,
  COUNT(*) FILTER (WHERE has_es AND NOT has_en)    AS spanish_only_orphans,
  COUNT(*)                                          AS unique_recall_numbers
FROM agg;

\echo
\echo '=== 9) field_has_spanish vs actual Spanish presence ==='
\echo '    Cross-checks the source-of-truth claim against bronze reality.'
\echo '    Finding G: field_has_spanish is True on BOTH the EN and ES rows of a pair.'
WITH latest_en AS (
  SELECT DISTINCT ON (source_recall_id) source_recall_id, has_spanish
  FROM usda_fsis_recalls_bronze
  WHERE langcode = 'English'
  ORDER BY source_recall_id, extraction_timestamp DESC
),
es_present AS (
  SELECT DISTINCT source_recall_id
  FROM usda_fsis_recalls_bronze
  WHERE langcode = 'Spanish'
)
SELECT
  COUNT(*) FILTER (WHERE has_spanish AND source_recall_id IN (SELECT source_recall_id FROM es_present)) AS claims_es_has_es,
  COUNT(*) FILTER (WHERE has_spanish AND source_recall_id NOT IN (SELECT source_recall_id FROM es_present)) AS claims_es_missing_es,
  COUNT(*) FILTER (WHERE NOT has_spanish AND source_recall_id IN (SELECT source_recall_id FROM es_present)) AS claims_no_es_but_has_es,
  COUNT(*) FILTER (WHERE NOT has_spanish AND source_recall_id NOT IN (SELECT source_recall_id FROM es_present)) AS claims_no_es_correct
FROM latest_en;

\echo
\echo '=== 10) Bilingual pair date alignment (Finding F: same last_modified_date) ==='
\echo '    For each EN+ES pair: do they share the same last_modified_date?'
\echo '    Mismatches would indicate FSIS updates the languages independently.'
WITH latest AS (
  SELECT DISTINCT ON (source_recall_id, langcode)
    source_recall_id, langcode, last_modified_date
  FROM usda_fsis_recalls_bronze
  ORDER BY source_recall_id, langcode, extraction_timestamp DESC
),
pairs AS (
  SELECT source_recall_id,
         MAX(last_modified_date) FILTER (WHERE langcode = 'English') AS en_lmd,
         MAX(last_modified_date) FILTER (WHERE langcode = 'Spanish') AS es_lmd
  FROM latest
  GROUP BY source_recall_id
  HAVING COUNT(*) = 2
)
SELECT
  COUNT(*)                                                                AS bilingual_pairs_checked,
  COUNT(*) FILTER (WHERE en_lmd IS NOT DISTINCT FROM es_lmd)              AS aligned_or_both_null,
  COUNT(*) FILTER (WHERE en_lmd IS DISTINCT FROM es_lmd)                  AS mismatched
FROM pairs;


\echo
\echo '############################################################'
\echo '# CATEGORY DISTRIBUTIONS'
\echo '############################################################'

\echo
\echo '=== 11) recall_type distribution ==='
SELECT recall_type, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY recall_type
 ORDER BY n DESC;

\echo
\echo '=== 12) recall_classification distribution ==='
SELECT recall_classification, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY recall_classification
 ORDER BY n DESC;

\echo
\echo '=== 13) recall_reason distribution (top 15) ==='
SELECT recall_reason, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY recall_reason
 ORDER BY n DESC
 LIMIT 15;

\echo
\echo '=== 14) processing distribution (top 15) ==='
SELECT processing, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY processing
 ORDER BY n DESC
 LIMIT 15;

\echo
\echo '=== 15) risk_level distribution ==='
SELECT risk_level, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY risk_level
 ORDER BY n DESC;


\echo
\echo '############################################################'
\echo '# ACTIVE / ARCHIVED / ACTIVE_NOTICE CROSS-TABS'
\echo '############################################################'

\echo
\echo '=== 16) active_notice × archive_recall cross-tab ==='
\echo '    Explains the 2 true / 1811 false / 189 null split observed in verify.'
SELECT archive_recall, active_notice, COUNT(*) AS n
  FROM usda_fsis_recalls_bronze
 GROUP BY archive_recall, active_notice
 ORDER BY archive_recall, active_notice NULLS LAST;

\echo
\echo '=== 17) last_modified_date population by archive_recall ==='
\echo '    Finding J claimed ~843 of 845 empty-date records are archived.'
\echo '    Confirms with bronze data; informs whether the field is reliable.'
SELECT
  archive_recall,
  COUNT(*) FILTER (WHERE last_modified_date IS NOT NULL) AS lmd_populated,
  COUNT(*) FILTER (WHERE last_modified_date IS NULL)     AS lmd_null,
  COUNT(*)                                                AS total
FROM usda_fsis_recalls_bronze
 GROUP BY archive_recall
 ORDER BY archive_recall;


\echo
\echo '############################################################'
\echo '# UNDOCUMENTED FIELD VALIDATION'
\echo '############################################################'

\echo
\echo '=== 18) recall_url prefix split by langcode ==='
\echo '    Finding H: English uses /recalls-alerts/, Spanish uses /es/retirada/.'
\echo '    Confirms by inspecting the URL prefix on the latest version of each row.'
WITH latest AS (
  SELECT DISTINCT ON (source_recall_id, langcode)
    langcode, recall_url
  FROM usda_fsis_recalls_bronze
  ORDER BY source_recall_id, langcode, extraction_timestamp DESC
)
SELECT
  langcode,
  COUNT(*) FILTER (WHERE recall_url IS NULL)                              AS null_url,
  COUNT(*) FILTER (WHERE recall_url LIKE '%/recalls-alerts/%')            AS recalls_alerts_path,
  COUNT(*) FILTER (WHERE recall_url LIKE '%/es/retirada/%')               AS es_retirada_path,
  COUNT(*) FILTER (WHERE recall_url IS NOT NULL
                     AND recall_url NOT LIKE '%/recalls-alerts/%'
                     AND recall_url NOT LIKE '%/es/retirada/%')           AS other_path
FROM latest
 GROUP BY langcode
 ORDER BY langcode;


\echo
\echo '############################################################'
\echo '# NULL / EMPTY RATES (full field audit)'
\echo '############################################################'

\echo
\echo '=== 19) Null rates across all fields (validates Finding C empirically) ==='
\echo '    Required fields should be 0%. Optional fields should match Finding C'
\echo '    targets. Any surprise here surfaces a Finding C blind spot like the one'
\echo '    that produced the active_notice ~9.4% null discovery on first extraction.'
SELECT
  ROUND(100.0 * SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_title,
  ROUND(100.0 * SUM(CASE WHEN recall_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_recall_date,
  ROUND(100.0 * SUM(CASE WHEN recall_type IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_recall_type,
  ROUND(100.0 * SUM(CASE WHEN recall_classification IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_classification,
  ROUND(100.0 * SUM(CASE WHEN active_notice IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_active_notice,
  ROUND(100.0 * SUM(CASE WHEN last_modified_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_last_modified,
  ROUND(100.0 * SUM(CASE WHEN closed_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_closed_date,
  ROUND(100.0 * SUM(CASE WHEN related_to_outbreak IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_outbreak
FROM usda_fsis_recalls_bronze;

SELECT
  ROUND(100.0 * SUM(CASE WHEN closed_year IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_closed_year,
  ROUND(100.0 * SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_year,
  ROUND(100.0 * SUM(CASE WHEN risk_level IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_risk,
  ROUND(100.0 * SUM(CASE WHEN recall_reason IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_reason,
  ROUND(100.0 * SUM(CASE WHEN processing IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_processing,
  ROUND(100.0 * SUM(CASE WHEN states IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_states,
  ROUND(100.0 * SUM(CASE WHEN establishment IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_establishment,
  ROUND(100.0 * SUM(CASE WHEN labels IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_labels
FROM usda_fsis_recalls_bronze;

SELECT
  ROUND(100.0 * SUM(CASE WHEN qty_recovered IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_qty_recovered,
  ROUND(100.0 * SUM(CASE WHEN summary IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_summary,
  ROUND(100.0 * SUM(CASE WHEN product_items IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_product_items,
  ROUND(100.0 * SUM(CASE WHEN distro_list IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_distro_list,
  ROUND(100.0 * SUM(CASE WHEN media_contact IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_media_contact,
  ROUND(100.0 * SUM(CASE WHEN company_media_contact IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_company_contact,
  ROUND(100.0 * SUM(CASE WHEN recall_url IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_recall_url,
  ROUND(100.0 * SUM(CASE WHEN en_press_release IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_en_press,
  ROUND(100.0 * SUM(CASE WHEN press_release IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_null_press
FROM usda_fsis_recalls_bronze;


\echo
\echo '############################################################'
\echo '# FREE-TEXT FIELD SAMPLES'
\echo '############################################################'

\echo
\echo '=== 20) Top establishments by recall count ==='
\echo '    Hints at firm-resolution heuristics for the silver layer (Phase 6).'
SELECT establishment, COUNT(*) AS recall_rows
  FROM usda_fsis_recalls_bronze
 WHERE establishment IS NOT NULL
 GROUP BY establishment
 ORDER BY recall_rows DESC
 LIMIT 15;

\echo
\echo '=== 21) qty_recovered value samples (free text — silver cleaning preview) ==='
\echo '    Like FDA product_distributed_quantity: free text, no enforced format.'
\echo '    Reveals the range of formats the silver transform must handle.'
SELECT qty_recovered, COUNT(*) AS occurrences
  FROM usda_fsis_recalls_bronze
 WHERE qty_recovered IS NOT NULL
 GROUP BY qty_recovered
 ORDER BY occurrences DESC
 LIMIT 15;
