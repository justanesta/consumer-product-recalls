-- Gold-layer API-refinement audit — per-object COVERAGE probes (population %, enum domains, array
-- cardinality, GROUPING SETS rollup integrity). Companion to scripts/sql/gold/audit_schema_and_indexes.sql
-- (which answers "what columns/indexes EXIST") and to project_scope/gold-audit-workstream.md.
-- Authored per-object by the gold-api-audit workflow (one agent per object), reviewed against the live
-- schema (notably: product_upcs / recall_product_upcs / sidecar arrays are JSONB, not text[]).
--
-- RUN AS THE OWNER (not recalls_readonly): a few probes touch silver recall_event (fct_recalls_by_country
-- denominator) and read sibling marts; SELECT-only throughout. Capture the output for the synthesis:
--     mkdir -p data/exploratory/gold
--     pwr psql -f scripts/sql/gold/audit_coverage.sql > data/exploratory/gold/audit_coverage.txt 2>&1
-- ON_ERROR_STOP is off so one bad statement never aborts the rest of the file.

\set ON_ERROR_STOP off
\pset pager off

/*==================== mart_recall_summary ====================*/
\echo '=== mart_recall_summary ==='

-- overall row count
SELECT count(*) AS total_rows FROM mart_recall_summary;

-- per-source population of API-relevant nullable scalars + text[] non-empty counts
SELECT
    source,
    count(*)                                                                   AS n_rows,
    count(announced_at)                                                        AS announced_at_pop,        -- nullable by design (~FDA nulls)
    count(published_at)                                                        AS published_at_pop,        -- expect = n_rows (not_null)
    count(lifecycle_status)                                                    AS lifecycle_status_pop,    -- null for CPSC/NHTSA
    count(is_active)                                                           AS is_active_pop,           -- tri-state (null CPSC/NHTSA)
    count(risk_level)                                                          AS risk_level_pop,          -- USDA-only
    count(reason_category)                                                     AS reason_category_pop,     -- USDA-only
    count(distribution_states)                                                 AS distribution_states_pop, -- USDA-only raw text
    count(source_recall_id)                                                    AS source_recall_id_pop,
    count(was_ever_retracted)                                                  AS was_ever_retracted_pop,
    count(*) FILTER (WHERE distribution_state_codes IS NOT NULL
                       AND cardinality(distribution_state_codes) > 0)          AS dist_state_codes_nonempty,   -- text[]
    count(*) FILTER (WHERE distribution_country_codes IS NOT NULL
                       AND cardinality(distribution_country_codes) > 0)        AS dist_country_codes_nonempty, -- text[]
    count(*) FILTER (WHERE product_upcs IS NOT NULL
                       AND jsonb_array_length(product_upcs) > 0)               AS product_upcs_nonempty        -- JSONB array (recall-level UPCs), NOT text[]
FROM mart_recall_summary
GROUP BY source
ORDER BY source;

-- enum/cardinality probes for low-cardinality text columns
SELECT distribution_scope, count(*) FROM mart_recall_summary GROUP BY distribution_scope ORDER BY count(*) DESC;       -- confirm exactly 4 values + NOT NULL
SELECT source, classification, count(*) FROM mart_recall_summary GROUP BY source, classification ORDER BY source, count(*) DESC;  -- source-native, mixed domain
SELECT source, lifecycle_status, count(*) FROM mart_recall_summary GROUP BY source, lifecycle_status ORDER BY source, count(*) DESC;  -- per-source domain
SELECT is_active, count(*) FROM mart_recall_summary GROUP BY is_active ORDER BY count(*) DESC;                          -- tri-state split
SELECT risk_level, count(*) FROM mart_recall_summary GROUP BY risk_level ORDER BY count(*) DESC;                        -- USDA-only domain
SELECT has_been_edited, count(*) FROM mart_recall_summary GROUP BY has_been_edited ORDER BY count(*) DESC;              -- edit-flag split

-- text[] cardinality + empty-vs-null split: distribution_state_codes
SELECT 'distribution_state_codes' AS arr, count(*) total, count(distribution_state_codes) non_null,
       count(*) FILTER (WHERE cardinality(distribution_state_codes)=0) empty,
       round(avg(cardinality(distribution_state_codes)),2) avg_card, max(cardinality(distribution_state_codes)) max_card
FROM mart_recall_summary;

-- text[] cardinality + empty-vs-null split: distribution_country_codes (expect 'US' absent, mostly empty)
SELECT 'distribution_country_codes' AS arr, count(*) total, count(distribution_country_codes) non_null,
       count(*) FILTER (WHERE cardinality(distribution_country_codes)=0) empty,
       round(avg(cardinality(distribution_country_codes)),2) avg_card, max(cardinality(distribution_country_codes)) max_card
FROM mart_recall_summary;

-- JSONB array cardinality + empty-vs-null split: product_upcs (jsonb, NOT text[] — recall-level UPCs from recall_event)
SELECT 'product_upcs' AS arr, count(*) total, count(product_upcs) non_null,
       count(*) FILTER (WHERE jsonb_array_length(product_upcs)=0) empty,
       round(avg(jsonb_array_length(product_upcs)),2) avg_card, max(jsonb_array_length(product_upcs)) max_card
FROM mart_recall_summary;

-- confirm 'US' is excluded by design from country codes (expect 0)
SELECT count(*) AS rows_with_US_country FROM mart_recall_summary WHERE distribution_country_codes @> ARRAY['US'];

-- redundancy check: is distribution_state_codes derivable-from / disjoint-with raw distribution_states by source
SELECT source,
       count(*) FILTER (WHERE distribution_states IS NOT NULL) AS raw_text_pop,
       count(*) FILTER (WHERE cardinality(distribution_state_codes) > 0) AS parsed_array_pop
FROM mart_recall_summary GROUP BY source ORDER BY source;

/*==================== mart_product_search ====================*/
\echo '=== mart_product_search ==='

-- overall row count
SELECT count(*) AS total_rows FROM mart_product_search;

-- per-source population of the API-relevant nullable scalars + recall_product_upcs nonempty
SELECT source,
       count(*)                          AS n_rows,
       count(product_name)               AS product_name_pop,
       count(product_description)        AS product_description_pop,
       count(model)                      AS model_pop,
       count(hin)                        AS hin_pop,
       count(model_year)                 AS model_year_pop,
       count(upc)                        AS upc_pop,                      -- expect 0 (all-NULL placeholder, O2)
       count(type)                       AS type_pop,
       count(classification)             AS classification_pop,
       count(risk_level)                 AS risk_level_pop,              -- expect USDA-only
       count(is_active)                  AS is_active_pop,              -- tri-state; null for CPSC/NHTSA
       count(recall_product_upcs)        AS upcs_non_null,
       count(*) FILTER (WHERE recall_product_upcs IS NOT NULL
                        AND jsonb_array_length(recall_product_upcs) > 0) AS recall_product_upcs_nonempty
FROM mart_product_search
GROUP BY source ORDER BY source;

-- enum-like cardinality: source distribution
SELECT source, count(*) FROM mart_product_search GROUP BY source ORDER BY count(*) DESC;

-- enum-like cardinality: classification (source-native, expect Class I/II/III, H/L/M/S, NULL)
SELECT classification, count(*) FROM mart_product_search GROUP BY classification ORDER BY count(*) DESC;

-- enum-like cardinality: risk_level (expect USDA-only)
SELECT risk_level, count(*) FROM mart_product_search GROUP BY risk_level ORDER BY count(*) DESC;

-- enum-like cardinality: is_active (tri-state true/false/NULL)
SELECT is_active, count(*) FROM mart_product_search GROUP BY is_active ORDER BY count(*) DESC;

-- enum-like cardinality: type (overloaded per source — show how messy/multi-vocab it is)
SELECT source, type, count(*) FROM mart_product_search GROUP BY source, type ORDER BY count(*) DESC LIMIT 40;

-- recall_product_upcs jsonb array: empty-vs-null split + cardinality (jsonb, not text[])
SELECT count(*) AS total,
       count(recall_product_upcs)                                       AS non_null,
       count(*) FILTER (WHERE jsonb_array_length(recall_product_upcs)=0) AS empty,
       round(avg(jsonb_array_length(recall_product_upcs)),2)            AS avg_card,
       max(jsonb_array_length(recall_product_upcs))                     AS max_card
FROM mart_product_search
WHERE recall_product_upcs IS NOT NULL;

-- confirm upc is fully NULL (O2 redundant_derivable / empty btree)
SELECT count(*) AS rows_with_nonnull_upc FROM mart_product_search WHERE upc IS NOT NULL;

-- search_vector emptiness check (empty tsvector = no searchable tokens)
SELECT count(*) FILTER (WHERE search_vector = ''::tsvector) AS empty_vectors,
       count(*) AS total
FROM mart_product_search;

-- model exact-lookup coverage: confirm CPSC+NHTSA only carry model
SELECT source, count(model) AS model_pop, count(*) AS n_rows
FROM mart_product_search GROUP BY source ORDER BY source;

-- hin exact-lookup coverage: confirm USCG only carries hin
SELECT source, count(hin) AS hin_pop, count(*) AS n_rows
FROM mart_product_search GROUP BY source ORDER BY source;

/*==================== mart_firm_profile ====================*/
\echo '=== mart_firm_profile ==='
-- overall row count (one row per canonical firm)
SELECT count(*) AS total_firms FROM mart_firm_profile;

-- sidecar-block population: how many firms carry each jsonb attribute block, plus the structural-id array
SELECT
  count(*)                                                                AS total_firms,
  count(observed_company_ids) FILTER (WHERE jsonb_array_length(observed_company_ids) > 0) AS has_structural_ids,
  count(firm_usda_attributes)                                             AS has_usda_block,
  count(firm_uscg_attributes)                                             AS has_uscg_block,
  count(firm_fda_attributes)                                             AS has_fda_block,
  count(*) FILTER (WHERE firm_usda_attributes IS NULL
                     AND firm_uscg_attributes IS NULL
                     AND firm_fda_attributes IS NULL)                     AS no_sidecar_block,
  count(alternate_names)                                                  AS has_alternate_names
FROM mart_firm_profile;

-- firms carrying MORE THAN ONE sidecar block (cross-source span)
SELECT
  (firm_usda_attributes IS NOT NULL)::int
  + (firm_uscg_attributes IS NOT NULL)::int
  + (firm_fda_attributes IS NOT NULL)::int AS n_sidecar_blocks,
  count(*)
FROM mart_firm_profile
GROUP BY 1 ORDER BY 1;

-- total_recalls distribution (right-skew check: how many firms at each bucket)
SELECT
  width_bucket(total_recalls, 0, 100, 10) AS bucket,
  min(total_recalls) AS lo, max(total_recalls) AS hi, count(*) AS n_firms
FROM mart_firm_profile
GROUP BY 1 ORDER BY 1;

-- headline stats: firms with zero recalls (sidecar-only), and the heavy tail
SELECT
  count(*) FILTER (WHERE total_recalls = 0) AS zero_recall_firms,
  count(*) FILTER (WHERE total_recalls = 1) AS one_recall_firms,
  count(*) FILTER (WHERE total_recalls > 50) AS gt50_recall_firms,
  max(total_recalls) AS max_total_recalls,
  round(avg(total_recalls), 2) AS avg_total_recalls,
  count(*) FILTER (WHERE active_recalls > 0) AS firms_with_active
FROM mart_firm_profile;

-- distinct_products distribution (NHTSA component-row inflation check)
SELECT
  count(*) FILTER (WHERE distinct_products = 0) AS zero_products,
  round(avg(distinct_products), 2) AS avg_products,
  max(distinct_products) AS max_products,
  count(*) FILTER (WHERE distinct_products > 100) AS gt100_products
FROM mart_firm_profile;

-- recalls_by_source: how many distinct source keys each firm's jsonb map carries (cross-source breadth)
SELECT
  CASE WHEN recalls_by_source IS NULL THEN 0
       ELSE (SELECT count(*) FROM jsonb_object_keys(recalls_by_source)) END AS n_source_keys,
  count(*) AS n_firms
FROM mart_firm_profile
GROUP BY 1 ORDER BY 1;

-- recalls_by_source: per-source firm coverage (how many firms appear under each source key)
SELECT key AS source, count(*) AS n_firms
FROM mart_firm_profile, jsonb_object_keys(recalls_by_source) AS key
WHERE recalls_by_source IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- roles facet population (manufacturer/importer/distributor distribution across firms)
SELECT role_val AS role, count(*) AS n_firms
FROM mart_firm_profile, jsonb_array_elements_text(roles) AS role_val
WHERE roles IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- normalized_name search readiness: distinctness + any duplicates (PK is firm_id, names need not be unique)
SELECT count(*) AS total, count(DISTINCT normalized_name) AS distinct_norm_names FROM mart_firm_profile;

/*==================== fct_recalls_by_month ====================*/
\echo '=== fct_recalls_by_month ==='
-- overall row count
SELECT count(*) AS total_rows FROM fct_recalls_by_month;

-- per-source pass: row count + measure population/min/max per source (incl 'ALL' rollup)
SELECT source,
       count(*)                         AS n_rows,
       count(event_count)               AS event_count_pop,   -- expect == n_rows (not_null)
       min(event_count)                 AS min_event_count,   -- expect >= 1 (no empty groups)
       sum(event_count)                 AS sum_event_count,
       min(period)                      AS first_month,
       max(period)                      AS last_month
FROM fct_recalls_by_month
GROUP BY source
ORDER BY source;

-- enum cardinality of the source dimension (verify closed set CPSC/FDA/USDA/NHTSA/USCG/ALL, no surprises)
SELECT source, count(*) AS n_rows FROM fct_recalls_by_month GROUP BY source ORDER BY count(*) DESC;

-- rollup integrity: for each month, does the 'ALL' row equal the sum of the per-source rows?
-- (mismatch_months should be 0; proves 'ALL' is a true grand total and consumers must not double-count)
WITH per_src AS (
    SELECT period, sum(event_count) AS sum_sources
    FROM fct_recalls_by_month
    WHERE source <> 'ALL'
    GROUP BY period
), all_row AS (
    SELECT period, event_count AS all_count
    FROM fct_recalls_by_month
    WHERE source = 'ALL'
)
SELECT count(*) FILTER (WHERE p.sum_sources IS DISTINCT FROM a.all_count) AS mismatch_months,
       count(*)                                                          AS total_months
FROM all_row a
LEFT JOIN per_src p USING (period);

-- spine density check: are there gaps? compare distinct months present for 'ALL' vs the
-- count of calendar months between first and last (gaps > 0 => no dense spine, consumer must zero-fill)
SELECT count(*)                                                           AS months_present_all,
       (extract(year  FROM max(period)) * 12 + extract(month FROM max(period)))
     - (extract(year  FROM min(period)) * 12 + extract(month FROM min(period))) + 1 AS months_in_range_all
FROM fct_recalls_by_month
WHERE source = 'ALL';

/*==================== fct_recalls_by_week ====================*/
\echo '=== fct_recalls_by_week ==='
-- overall row count (one row per week x source, plus ALL rollup rows)
SELECT count(*) AS total_rows FROM fct_recalls_by_week;
-- per-source pass: row count + period coverage + event_count totals per source (incl. ALL)
SELECT source,
       count(*)              AS n_week_rows,
       min(period)           AS first_week,
       max(period)           AS last_week,
       sum(event_count)      AS total_events,
       round(avg(event_count),2) AS avg_events_per_week,
       max(event_count)      AS max_events_in_a_week
FROM fct_recalls_by_week
GROUP BY source
ORDER BY source;
-- enum cardinality of the source dimension (confirm closed vocab + presence of 'ALL')
SELECT source, count(*) AS n_rows
FROM fct_recalls_by_week
GROUP BY source
ORDER BY count(*) DESC;
-- sparsity check: how many distinct weeks exist vs. a dense weekly spine over the covered range
-- (large gap => sparse spine; weeks with zero recalls produce NO row)
SELECT count(DISTINCT period)                                   AS distinct_weeks_present,
       (max(period) - min(period)) / 7 + 1                     AS weeks_in_full_range,
       min(period)                                             AS earliest_week,
       max(period)                                             AS latest_week
FROM fct_recalls_by_week;
-- rollup integrity: does the 'ALL' weekly total equal the sum of per-source weekly totals?
-- (any nonzero diff => double-count risk or an unexpected source value)
SELECT a.period,
       a.event_count                       AS all_count,
       sum(p.event_count)                  AS sum_per_source,
       a.event_count - sum(p.event_count)  AS diff
FROM fct_recalls_by_week a
JOIN fct_recalls_by_week p
  ON p.period = a.period AND p.source <> 'ALL'
WHERE a.source = 'ALL'
GROUP BY a.period, a.event_count
HAVING a.event_count <> sum(p.event_count)
ORDER BY a.period DESC
LIMIT 20;

/*==================== fct_recalls_by_year ====================*/
\echo '=== fct_recalls_by_year ==='
-- overall row count (years x sources + ALL rows)
SELECT count(*) AS total_rows FROM fct_recalls_by_year;
-- per-source pass: rows, year span, event_count totals (all three cols are NOT NULL by construction; this confirms population + the ALL=sum-of-sources property)
SELECT source,
       count(*)                       AS n_rows,
       count(period)                  AS period_pop,
       count(event_count)             AS event_count_pop,
       min(period)                    AS first_year,
       max(period)                    AS last_year,
       sum(event_count)               AS sum_event_count
FROM fct_recalls_by_year
GROUP BY source
ORDER BY source;
-- source cardinality (low-cardinality enum): confirm domain = CPSC/FDA/USDA/NHTSA/USCG/ALL and row counts per value
SELECT source, count(*) AS n_year_rows FROM fct_recalls_by_year GROUP BY source ORDER BY count(*) DESC;
-- ALL-row integrity check: does ALL.event_count equal the sum of per-source event_count for each year? (single-source recalls => should match exactly)
SELECT a.period,
       a.event_count                  AS all_count,
       s.sum_sources                  AS sum_per_source,
       a.event_count - s.sum_sources  AS diff
FROM fct_recalls_by_year a
JOIN (
    SELECT period, sum(event_count) AS sum_sources
    FROM fct_recalls_by_year
    WHERE source <> 'ALL'
    GROUP BY period
) s ON s.period = a.period
WHERE a.source = 'ALL'
  AND a.event_count <> s.sum_sources
ORDER BY a.period DESC;
-- distinct years present + partial-current-year visibility (last year may be incomplete)
SELECT extract(year FROM period)::int AS yr,
       max(event_count) FILTER (WHERE source = 'ALL') AS all_recalls_that_year
FROM fct_recalls_by_year
GROUP BY 1
ORDER BY yr DESC
LIMIT 15;

/*==================== fct_recalls_monthly_trend ====================*/
\echo '=== fct_recalls_monthly_trend ==='
-- overall row count
SELECT count(*) AS total_rows FROM fct_recalls_monthly_trend;
-- per-source: row count + population of the nullable derived scalars (NULL leading edge magnitude)
SELECT source,
       count(*)                                   AS n_rows,
       count(rolling_3mo_avg)                     AS rolling_3mo_avg_pop,
       count(rolling_12mo_avg)                    AS rolling_12mo_avg_pop,
       count(event_count_year_ago)               AS event_count_year_ago_pop,
       count(yoy_pct_change)                      AS yoy_pct_change_pop,
       min(month)                                 AS first_month,
       max(month)                                 AS last_month,
       count(*) FILTER (WHERE event_count = 0)    AS zero_filled_months
FROM fct_recalls_monthly_trend
GROUP BY source ORDER BY source;
-- low-cardinality enum: source distribution (confirm no 'ALL' row is present)
SELECT source, count(*) FROM fct_recalls_monthly_trend GROUP BY source ORDER BY count(*) DESC;
-- YoY null decomposition: separate 'no year-ago row' (lag12 NULL) from 'year-ago was 0' (nullif divide-guard)
SELECT source,
       count(*) FILTER (WHERE event_count_year_ago IS NULL)                          AS yoy_null_no_baseline,
       count(*) FILTER (WHERE event_count_year_ago = 0)                              AS yoy_null_div_by_zero,
       count(*) FILTER (WHERE yoy_pct_change IS NOT NULL)                            AS yoy_defined
FROM fct_recalls_monthly_trend
GROUP BY source ORDER BY source;
-- spine sanity: confirm dense contiguous months per source (expected_months should equal n_rows)
SELECT source,
       count(*) AS n_rows,
       (extract(year FROM age(max(month), min(month)))*12
        + extract(month FROM age(max(month), min(month))) + 1)::int AS expected_months
FROM fct_recalls_monthly_trend
GROUP BY source ORDER BY source;
-- value range / sanity for the derived measures (spot extreme YoY swings driven by small bases)
SELECT source,
       max(event_count)        AS max_event_count,
       round(max(yoy_pct_change),1) AS max_yoy_pct,
       round(min(yoy_pct_change),1) AS min_yoy_pct
FROM fct_recalls_monthly_trend
GROUP BY source ORDER BY source;

/*==================== fct_recalls_by_firm ====================*/
\echo '=== fct_recalls_by_firm ==='
-- overall row count = number of canonical firms on the leaderboard
SELECT count(*) AS n_firms FROM fct_recalls_by_firm;

-- per-column population of the nullable date columns + measure distribution (no source column on this view)
SELECT
  count(*)                                            AS n_firms,
  count(first_recall_at)                              AS first_recall_at_pop,   -- null iff event_count=0
  count(last_recall_at)                               AS last_recall_at_pop,    -- co-null with first_recall_at
  count(*) FILTER (WHERE event_count = 0)             AS zero_recall_firms,     -- the null-date tail
  count(*) FILTER (WHERE event_count > 0)             AS active_leaderboard,
  count(*) FILTER (WHERE active_recalls > 0)          AS firms_with_active,     -- lifecycle-bearing sources only
  count(*) FILTER (WHERE product_count > 0)           AS firms_with_products,
  min(event_count) AS min_events, max(event_count) AS max_events,
  round(avg(event_count), 2) AS avg_events
FROM fct_recalls_by_firm;

-- leaderboard head: are the top ranks tie-free? exposes rank() skip-on-tie behavior for the API
SELECT firm_id, canonical_name, event_count, active_recalls, product_count,
       first_recall_at, last_recall_at, event_count_rank
FROM fct_recalls_by_firm
ORDER BY event_count_rank, firm_id
LIMIT 25;

-- rank() tie audit: how often a rank is shared (consumers filtering on rank<=N could get >N rows)
SELECT event_count_rank, count(*) AS firms_at_rank
FROM fct_recalls_by_firm
GROUP BY event_count_rank
HAVING count(*) > 1
ORDER BY event_count_rank
LIMIT 20;

-- event_count distribution buckets (low-cardinality-ish): shape of the long tail for a min_recalls filter
SELECT
  CASE WHEN event_count = 0 THEN '0'
       WHEN event_count = 1 THEN '1'
       WHEN event_count BETWEEN 2 AND 5 THEN '2-5'
       WHEN event_count BETWEEN 6 AND 20 THEN '6-20'
       WHEN event_count BETWEEN 21 AND 100 THEN '21-100'
       ELSE '100+' END AS event_count_bucket,
  count(*) AS n_firms
FROM fct_recalls_by_firm
GROUP BY 1
ORDER BY min(event_count);

-- active_recalls vs event_count sanity: active should never exceed total
SELECT count(*) AS rows_with_active_gt_total
FROM fct_recalls_by_firm
WHERE active_recalls > event_count;

-- recency spread on the populated rows (last_recall_at) — supports a 'recently active firms' feed
SELECT date_trunc('year', last_recall_at)::date AS recall_year, count(*) AS n_firms
FROM fct_recalls_by_firm
WHERE last_recall_at IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC
LIMIT 15;

/*==================== fct_recalls_by_classification ====================*/
\echo '=== fct_recalls_by_classification ==='
-- overall row count of the aggregate (cells, not recalls)
SELECT count(*) AS total_cells FROM fct_recalls_by_classification;

-- per-source: cell count, classification/risk_level population, and summed events; ALL row is the GROUPING SETS rollup
SELECT source,
       count(*)                                            AS n_cells,
       count(classification)                               AS classification_pop,   -- non-NULL classification cells
       count(risk_level)                                   AS risk_level_pop,        -- non-NULL risk_level cells (expect only USDA + ALL)
       sum(event_count)                                    AS events_summed,
       count(*) FILTER (WHERE classification IS NULL)      AS unclassified_cells
FROM fct_recalls_by_classification
GROUP BY source
ORDER BY source;

-- classification enum domain realized (low-cardinality): which source-native codes actually appear + their cell weight
SELECT classification, count(*) AS n_cells, sum(event_count) AS events
FROM fct_recalls_by_classification
GROUP BY classification
ORDER BY events DESC NULLS LAST;

-- risk_level enum domain realized (low-cardinality): expect USDA-derived labels + NULL elsewhere
SELECT risk_level, count(*) AS n_cells, sum(event_count) AS events
FROM fct_recalls_by_classification
GROUP BY risk_level
ORDER BY events DESC NULLS LAST;

-- collinearity check: for USDA, is (classification -> risk_level) strictly 1:1 (redundant_derivable)?
SELECT classification, risk_level, sum(event_count) AS events
FROM fct_recalls_by_classification
WHERE source = 'USDA'
GROUP BY classification, risk_level
ORDER BY classification, risk_level;

-- confirm risk_level is non-NULL ONLY for USDA (and the ALL rollup that aggregates it) -> single_source_param_trap
SELECT source, count(*) FILTER (WHERE risk_level IS NOT NULL) AS risk_level_nonnull_cells
FROM fct_recalls_by_classification
GROUP BY source
ORDER BY source;

/*==================== fct_recall_status ====================*/
\echo '=== fct_recall_status ==='
-- overall row count (number of (source,status) cells incl. ALL rollup)
SELECT count(*) AS total_cells FROM fct_recall_status;

-- per-source pass: distinct statuses present and total events per source (status is never null by construction)
SELECT source,
       count(*)                                   AS n_status_cells,
       count(*) FILTER (WHERE status='active')    AS has_active_cell,
       count(*) FILTER (WHERE status='inactive')  AS has_inactive_cell,
       count(*) FILTER (WHERE status='unknown')   AS has_unknown_cell,
       sum(event_count)                           AS total_events
FROM fct_recall_status
GROUP BY source
ORDER BY source;

-- status cardinality across all rows (should be exactly active/inactive/unknown)
SELECT status, count(*) AS n_cells, sum(event_count) AS total_events
FROM fct_recall_status
GROUP BY status
ORDER BY total_events DESC;

-- structural check: CPSC and NHTSA must be 100% 'unknown' (is_active null in silver) — expect a single unknown row each
SELECT source, status, event_count
FROM fct_recall_status
WHERE source IN ('CPSC','NHTSA')
ORDER BY source, status;

-- the active:inactive:unknown magnitude per status-bearing source (FDA/USDA/USCG) + the ALL rollup
SELECT source, status, event_count
FROM fct_recall_status
WHERE source IN ('FDA','USDA','USCG','ALL')
ORDER BY source, status;

-- cross-check the GROUPING SETS rollup: per-status ALL cell should equal sum of the five per-source cells for that status
SELECT status,
       max(event_count) FILTER (WHERE source='ALL')                 AS all_rollup,
       sum(event_count) FILTER (WHERE source <> 'ALL')              AS sum_of_sources
FROM fct_recall_status
GROUP BY status
ORDER BY status;

/*==================== fct_recalls_by_geography ====================*/
\echo '=== fct_recalls_by_geography ==='
-- overall row count (includes per-source rows AND source='ALL' rollup rows)
SELECT count(*) AS total_rows FROM fct_recalls_by_geography;

-- per-source population pass: rows + distinct states + recall_count distribution per source (and 'ALL')
SELECT source,
       count(*)                              AS n_rows,
       count(DISTINCT state_code)            AS distinct_states,
       count(DISTINCT geography_basis)       AS distinct_bases,
       sum(recall_count)                     AS sum_recall_count,   -- intentionally > distinct recalls (multi-count witness)
       max(recall_count)                     AS max_cell_count
FROM fct_recalls_by_geography
GROUP BY source
ORDER BY source;

-- geography_basis cardinality (enum: distribution / firm_registration) + which sources appear per basis
SELECT geography_basis, source, count(*) AS n_rows, sum(recall_count) AS sum_recall_count
FROM fct_recalls_by_geography
GROUP BY geography_basis, source
ORDER BY geography_basis, source;

-- geography_basis low-cardinality split
SELECT geography_basis, count(*) AS n_rows
FROM fct_recalls_by_geography
GROUP BY geography_basis
ORDER BY count(*) DESC;

-- source low-cardinality split (confirm 'ALL' rollup present + per-source domain per basis)
SELECT source, count(*) AS n_rows
FROM fct_recalls_by_geography
GROUP BY source
ORDER BY count(*) DESC;

-- state_code domain sanity: confirm all values are valid 2-letter USPS-ish codes, no foreign/NULL
SELECT count(DISTINCT state_code) AS distinct_states,
       count(*) FILTER (WHERE state_code IS NULL)            AS null_states,
       count(*) FILTER (WHERE state_code !~ '^[A-Z]{2}$')   AS non_2letter_codes
FROM fct_recalls_by_geography;

-- MULTI-COUNT magnitude: per (basis,source), how much do per-state counts sum to vs the true distinct-recall total?
-- (the 'ALL' source rows give the cross-source picture; compare sum_over_states to a true distinct count
--  obtained from the source facts is out of scope here, so this surfaces the inflation footprint)
SELECT geography_basis, source,
       sum(recall_count)        AS sum_over_states,   -- inflated by multi-counting
       max(recall_count)        AS top_state_count,
       count(DISTINCT state_code) AS states_touched
FROM fct_recalls_by_geography
WHERE source = 'ALL'
GROUP BY geography_basis, source
ORDER BY geography_basis;

-- top states per basis (sanity: distribution should look like a population map; firm_registration skews to HQ states e.g. AR/MN)
SELECT geography_basis, state_code, recall_count
FROM fct_recalls_by_geography
WHERE source = 'ALL'
ORDER BY geography_basis, recall_count DESC
LIMIT 30;

/*==================== fct_recalls_by_country ====================*/
\echo '=== fct_recalls_by_country ==='
-- overall row count (per-source rows + 'ALL' rollup rows)
SELECT count(*) AS total_rows FROM fct_recalls_by_country;
-- per-source pass: row count, distinct countries, total/max recall_count
SELECT source,
       count(*) AS n_rows,
       count(DISTINCT country_code) AS distinct_countries,
       sum(recall_count) AS sum_recall_count,
       max(recall_count) AS max_recall_count
FROM fct_recalls_by_country
GROUP BY source
ORDER BY source;
-- source cardinality (enum-like: confirm only FDA/USDA/ALL appear)
SELECT source, count(*) FROM fct_recalls_by_country GROUP BY source ORDER BY count(*) DESC;
-- US dominance vs foreign tail: is_us split per source (US derived cell vs unnested foreign)
SELECT source,
       (country_code = 'US') AS is_us,
       count(*) AS n_country_rows,
       sum(recall_count) AS sum_recall_count
FROM fct_recalls_by_country
GROUP BY source, (country_code = 'US')
ORDER BY source, is_us DESC;
-- top countries by recall_count for the ALL rollup (confirm US dominance + name the foreign long tail)
SELECT country_code, recall_count
FROM fct_recalls_by_country
WHERE source = 'ALL'
ORDER BY recall_count DESC
LIMIT 25;
-- country_code domain size + whether any non-US, non-2-char codes leaked (ISO alpha-2 sanity)
SELECT count(DISTINCT country_code) AS distinct_country_codes,
       count(*) FILTER (WHERE length(country_code) <> 2) AS non_alpha2_rows,
       count(*) FILTER (WHERE country_code = 'US') AS us_rows
FROM fct_recalls_by_country;
-- multi-valued check: does sum(recall_count) at ALL grain exceed the true distinct-recall total?
-- (compare sum_recall_count for source='ALL' above against this distinct count of FDA+USDA recalls)
SELECT count(DISTINCT recall_event_id) AS distinct_fda_usda_recalls
FROM recall_event
WHERE source IN ('FDA','USDA');

/*==================== fct_units_recalled ====================*/
\echo '=== fct_units_recalled ==='
-- overall row count (cells = source × unit_category × month)
SELECT count(*) AS total_rows FROM fct_units_recalled;
-- per-source population + measure magnitudes (confirms CPSC absent, FDA/USDA coverage)
SELECT source,
       count(*)                         AS n_rows,
       count(DISTINCT unit_category)     AS n_categories,
       count(DISTINCT period)            AS n_months,
       min(period)                       AS earliest_month,
       max(period)                       AS latest_month,
       sum(recalls_with_units)           AS sum_recalls_with_units,
       sum(total_units)                  AS grand_total_units,
       max(max_units)                    AS biggest_single_recall
FROM fct_units_recalled
GROUP BY source ORDER BY source;
-- source enum cardinality (verify only the 4 expected sources; CPSC must be absent)
SELECT source, count(*) FROM fct_units_recalled GROUP BY source ORDER BY count(*) DESC;
-- unit_category enum cardinality (verify count/weight/volume/grouping domain)
SELECT unit_category, count(*) FROM fct_units_recalled GROUP BY unit_category ORDER BY count(*) DESC;
-- source × unit_category crosstab: confirms NHTSA/USCG only 'count'; weight/volume/grouping FDA/USDA-only
SELECT source, unit_category, count(*) AS n_cells,
       sum(recalls_with_units) AS recalls_w_units, sum(total_units) AS total_units
FROM fct_units_recalled GROUP BY source, unit_category ORDER BY source, unit_category;
-- coverage sanity: any cells with recalls_with_units = 0? (should be impossible)
SELECT count(*) AS cells_with_zero_recalls FROM fct_units_recalled WHERE recalls_with_units = 0;
-- avg rounding lossiness: cells where the stored rounded avg disagrees with the raw ratio
SELECT count(*) AS cells_with_rounding_drift
FROM fct_units_recalled
WHERE recalls_with_units > 0
  AND avg_units_per_recall <> round(total_units / recalls_with_units);

/*==================== dim_date ====================*/
\echo '=== dim_date ==='
-- overall row count + min/max + spine boundary sanity (object has NO source column)
SELECT count(*) AS n_rows,
       min(date_day) AS min_date_day,
       max(date_day) AS max_date_day,
       (date_trunc('year', current_date) + interval '2 year')::date AS expected_spine_end_exclusive
FROM dim_date;
-- null check across every generated column (all expected 0 — generated calendar, non-null by construction)
SELECT count(*) FILTER (WHERE date_day        IS NULL) AS null_date_day,
       count(*) FILTER (WHERE year            IS NULL) AS null_year,
       count(*) FILTER (WHERE quarter         IS NULL) AS null_quarter,
       count(*) FILTER (WHERE month           IS NULL) AS null_month,
       count(*) FILTER (WHERE month_name      IS NULL) AS null_month_name,
       count(*) FILTER (WHERE iso_week        IS NULL) AS null_iso_week,
       count(*) FILTER (WHERE iso_day_of_week IS NULL) AS null_iso_day_of_week,
       count(*) FILTER (WHERE day_name        IS NULL) AS null_day_name,
       count(*) FILTER (WHERE day_of_year     IS NULL) AS null_day_of_year,
       count(*) FILTER (WHERE iso_week_start   IS NULL) AS null_iso_week_start,
       count(*) FILTER (WHERE month_start     IS NULL) AS null_month_start,
       count(*) FILTER (WHERE quarter_start   IS NULL) AS null_quarter_start,
       count(*) FILTER (WHERE year_start      IS NULL) AS null_year_start,
       count(*) FILTER (WHERE is_weekend      IS NULL) AS null_is_weekend,
       count(*) FILTER (WHERE us_fiscal_year  IS NULL) AS null_us_fiscal_year
FROM dim_date;
-- contiguity / uniqueness check: distinct days should equal row count and equal the calendar span+1
SELECT count(*) AS n_rows,
       count(DISTINCT date_day) AS distinct_days,
       (max(date_day) - min(date_day) + 1) AS span_days_inclusive
FROM dim_date;
-- low-cardinality enum-like: month_name (expect 12 English labels, even spread)
SELECT month_name, count(*) FROM dim_date GROUP BY month_name ORDER BY count(*) DESC;
-- low-cardinality enum-like: day_name (expect 7 weekday labels)
SELECT day_name, count(*) FROM dim_date GROUP BY day_name ORDER BY count(*) DESC;
-- low-cardinality boolean: is_weekend split (expect ~2/7 weekend)
SELECT is_weekend, count(*) FROM dim_date GROUP BY is_weekend ORDER BY count(*) DESC;
-- fiscal-calendar reuse sanity: fiscal year should lead calendar year by 1 for Oct-Dec rows
SELECT count(*) FILTER (WHERE month >= 10 AND us_fiscal_year <> year + 1) AS oct_dec_mislabeled,
       count(*) FILTER (WHERE month <  10 AND us_fiscal_year <> year)     AS jan_sep_mislabeled
FROM dim_date;
-- join-coverage spot check: does dim_date cover the observed recall published_at range?
-- (read-only; mart_recall_summary is a sibling gold object) expect both rows present in dim_date
SELECT min(published_at)::date AS min_pub, max(published_at)::date AS max_pub,
       (min(published_at)::date >= (SELECT min(date_day) FROM dim_date)) AS lower_covered,
       (max(published_at)::date <= (SELECT max(date_day) FROM dim_date)) AS upper_covered
FROM mart_recall_summary;

/*==================== gold_meta ====================*/
\echo '=== gold_meta ==='
-- gold_meta has NO source column; R4 already validated by verify_gold_readiness Section 4 (passed 2026-06-14), so keep this minimal/confirmatory.
-- overall row count + both columns populated + datatypes (R4: exactly 1 row, both NOT NULL)
SELECT
    count(*)                                            AS row_count,
    count(rebuilt_at)                                   AS rebuilt_at_pop,
    count(schema_version)                               AS schema_version_pop,
    max(rebuilt_at)                                     AS rebuilt_at,
    max(schema_version)                                 AS schema_version,
    (max(rebuilt_at) AT TIME ZONE 'UTC')                AS rebuilt_at_utc,           -- confirm UTC render
    pg_typeof(max(rebuilt_at))                          AS rebuilt_at_type,          -- expect timestamptz
    pg_typeof(max(schema_version))                      AS schema_version_type,      -- expect text
    CASE WHEN count(*) = 1
              AND bool_and(rebuilt_at IS NOT NULL)
              AND bool_and(schema_version IS NOT NULL)
         THEN 'PASS' ELSE 'FAIL <<<<' END              AS r4_verdict
FROM gold_meta;
-- freshness sanity: rebuilt_at should be recent (within the last day if a build just ran) — staleness check
SELECT
    max(rebuilt_at)                                     AS rebuilt_at,
    now() - max(rebuilt_at)                             AS age_since_rebuild         -- large age => stale gold layer
FROM gold_meta;
-- de-facto domain of the contract-version token (expect a single low-cardinality string, today '1')
SELECT schema_version, count(*) FROM gold_meta GROUP BY schema_version ORDER BY count(*) DESC;

/*==================== firm_usda_attributes ====================*/
\echo '=== firm_usda_attributes ==='
-- overall row count + population of API-relevant nullable scalars (no source column on this sidecar)
SELECT
  count(*)                                   AS n_rows,
  count(establishment_id)                    AS establishment_id_pop,
  count(state)                               AS state_pop,
  count(zip)                                 AS zip_pop,
  count(county)                              AS county_pop,
  count(fips_code)                           AS fips_code_pop,
  count(geolocation)                         AS geolocation_pop,
  count(city)                                AS city_pop,
  count(grant_date)                          AS grant_date_pop,
  count(latest_mpi_active_date)              AS latest_mpi_active_date_pop,
  count(status_regulated_est)                AS status_regulated_est_pop,
  count(size)                                AS size_pop,
  count(district)                            AS district_pop,
  count(circuit)                             AS circuit_pop
FROM firm_usda_attributes;

-- state coverage / cardinality (drives the firm_registration geography lens; backed by the state btree index)
SELECT state, count(*) AS n
FROM firm_usda_attributes
GROUP BY state
ORDER BY count(*) DESC;

-- status_regulated_est enum domain ('' = ACTIVE, 'Inactive') — verify the meaningful-empty-string contract
SELECT status_regulated_est, count(*) AS n
FROM firm_usda_attributes
GROUP BY status_regulated_est
ORDER BY count(*) DESC;

-- size enum domain incl. the undocumented 'N / A' dirty value and NULLs
SELECT size, count(*) AS n
FROM firm_usda_attributes
GROUP BY size
ORDER BY count(*) DESC;

-- district / circuit low-cardinality regulatory metadata cardinality
SELECT district, count(*) AS n FROM firm_usda_attributes GROUP BY district ORDER BY count(*) DESC;
SELECT circuit, count(*) AS n FROM firm_usda_attributes GROUP BY circuit ORDER BY count(*) DESC;

-- activities jsonb-array: null vs empty vs cardinality distribution
SELECT
  count(*)                                                              AS total,
  count(activities)                                                     AS non_null,
  count(*) FILTER (WHERE jsonb_array_length(activities) = 0)            AS empty,
  round(avg(jsonb_array_length(activities)), 2)                         AS avg_card,
  max(jsonb_array_length(activities))                                   AS max_card
FROM firm_usda_attributes;

-- dbas jsonb-array: null vs empty vs cardinality (W4 strips placeholders -> all-placeholder lands as NULL, not [])
SELECT
  count(*)                                                              AS total,
  count(dbas)                                                           AS non_null,
  count(*) FILTER (WHERE jsonb_array_length(dbas) = 0)                  AS empty,
  round(avg(jsonb_array_length(dbas)), 2)                               AS avg_card,
  max(jsonb_array_length(dbas))                                         AS max_card
FROM firm_usda_attributes;

-- establishment_id uniqueness sanity (should equal n_rows; backs the unique btree + _silver.yml unique test)
SELECT count(*) AS n_rows, count(DISTINCT establishment_id) AS distinct_ids
FROM firm_usda_attributes;

/*==================== firm_uscg_attributes ====================*/
\echo '=== firm_uscg_attributes ==='
-- overall row count (one row per MIC)
SELECT count(*) AS n_rows, count(DISTINCT mic) AS distinct_mic FROM firm_uscg_attributes;
-- per-column population of API-relevant nullable scalars (no source col; USCG-only)
SELECT
  count(*)                                                   AS n_rows,
  count(company_name)                                        AS company_name_pop,
  count(dba)                                                 AS dba_pop,
  count(parent_company)                                      AS parent_company_pop,
  count(parent_mic)                                          AS parent_mic_pop,
  count(address)                                             AS address_pop,
  count(city)                                                AS city_pop,
  count(state)                                               AS state_pop,
  count(zip)                                                 AS zip_pop,
  count(country)                                             AS country_pop,
  count(status)                                              AS status_pop,
  count(in_business)                                         AS in_business_pop,
  count(out_of_business)                                     AS out_of_business_pop,
  count(date_modified)                                       AS date_modified_pop,
  count(detail_url)                                          AS detail_url_pop,
  count(uscg_directory_id)                                   AS uscg_directory_id_pop
FROM firm_uscg_attributes;
-- temporal-identity flag distribution (the recycle surface; oob is a subset of has_prior)
SELECT
  count(*)                                                   AS n_rows,
  count(*) FILTER (WHERE mic_has_prior_holder)               AS has_prior_holder_true,
  count(*) FILTER (WHERE mic_oob_recycled)                   AS oob_recycled_true,
  count(*) FILTER (WHERE mic_renamed_not_recycled)           AS renamed_not_recycled_true,
  count(*) FILTER (WHERE past_company_1 IS NOT NULL)         AS slot1_pop,
  count(*) FILTER (WHERE past_company_2 IS NOT NULL)         AS slot2_pop,
  count(*) FILTER (WHERE past_company_3 IS NOT NULL)         AS slot3_pop
FROM firm_uscg_attributes;
-- status cardinality (low-cardinality enum-like directory status)
SELECT status, count(*) FROM firm_uscg_attributes GROUP BY status ORDER BY count(*) DESC;
-- country cardinality (verify domain; unlike distribution country, no US-exclusion convention)
SELECT country, count(*) FROM firm_uscg_attributes GROUP BY country ORDER BY count(*) DESC;
-- state cardinality (firm-registration geography; foreign rows -> NULL)
SELECT state, count(*) FROM firm_uscg_attributes GROUP BY state ORDER BY count(*) DESC;
-- prior_holders array: always-array invariant + cardinality split (empty '[]' vs SQL NULL)
SELECT
  count(*)                                                          AS total,
  count(prior_holders)                                             AS non_null,
  count(*) FILTER (WHERE jsonb_typeof(prior_holders) = 'array')    AS is_array,
  count(*) FILTER (WHERE prior_holders = '[]'::jsonb)              AS empty_array,
  round(avg(jsonb_array_length(prior_holders)), 2)                 AS avg_card,
  max(jsonb_array_length(prior_holders))                           AS max_card
FROM firm_uscg_attributes;

/*==================== firm_fda_attributes ====================*/
\echo '=== firm_fda_attributes ==='
-- overall row count (= distinct FDA FEIs / establishments in the current SCD-2 slice)
SELECT count(*) AS n_rows FROM firm_fda_attributes;
-- per-column population of API-relevant nullable scalars (no source col; FDA-only object)
SELECT
    count(*)                                          AS n_rows,
    count(firm_legal_nam)                             AS firm_legal_nam_pop,
    count(firm_city_nam)                              AS firm_city_nam_pop,
    count(firm_state_cd)                              AS firm_state_cd_pop,
    count(firm_state_prvnc_nam)                       AS firm_state_prvnc_nam_pop,
    count(firm_country_nam)                           AS firm_country_nam_pop,
    count(firm_postal_cd)                             AS firm_postal_cd_pop,
    count(firm_line1_adr)                             AS firm_line1_adr_pop,
    count(firm_line2_adr)                             AS firm_line2_adr_pop,
    count(firm_surviving_nam)                         AS firm_surviving_nam_pop,
    count(firm_surviving_fei)                         AS firm_surviving_fei_pop,
    round(100.0*count(firm_state_cd)/nullif(count(*),0),1)        AS firm_state_cd_pct,
    round(100.0*count(firm_postal_cd)/nullif(count(*),0),1)       AS firm_postal_cd_pct,
    round(100.0*count(firm_surviving_nam)/nullif(count(*),0),1)   AS firm_surviving_nam_pct
FROM firm_fda_attributes;
-- low-cardinality enum-like: registered-state distribution (FDA leg of firm_registration)
SELECT firm_state_cd, count(*) FROM firm_fda_attributes GROUP BY firm_state_cd ORDER BY count(*) DESC;
-- low-cardinality enum-like: registered-country distribution (free text; verify override coverage of null-country foreign firms)
SELECT firm_country_nam, count(*) FROM firm_fda_attributes GROUP BY firm_country_nam ORDER BY count(*) DESC;
-- succession signal: how many establishments carry a surviving (current) FEI/name vs. not
SELECT
    count(*) FILTER (WHERE firm_surviving_fei IS NOT NULL) AS has_surviving_fei,
    count(*) FILTER (WHERE firm_surviving_nam IS NOT NULL) AS has_surviving_nam,
    count(*) FILTER (WHERE firm_surviving_fei IS NOT NULL AND firm_surviving_fei <> firm_fei_num) AS surviving_fei_differs
FROM firm_fda_attributes;
-- key integrity sanity: confirm firm_fei_num is unique + non-null (backs unique btree + ::text functional index)
SELECT count(*) AS n_rows, count(DISTINCT firm_fei_num) AS distinct_fei, count(*) - count(firm_fei_num) AS null_fei
FROM firm_fda_attributes;

