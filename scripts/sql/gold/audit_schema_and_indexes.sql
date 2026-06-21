-- Gold-layer API-refinement audit — authoritative schema + index + stats inventory.
-- Companion to project_scope/gold-audit-workstream.md. ONE SELECT-only pass over the 18 audited gold
-- objects (15 gold models incl. gold_meta + the 3 silver firm sidecars feeding mart_firm_profile).
-- Produces the live ground truth the static model-reading cannot: materialized column types/nullability,
-- the indexes actually built by the last `dbt build`, and planner-stats freshness (the ANALYZE concern).
--
-- HOW TO RUN (read-only; safe against prod):
--     pwr psql -f scripts/sql/gold/audit_schema_and_indexes.sql
--   To capture the full output for analysis (recommended — paste it back or share the file):
--     pwr psql -f scripts/sql/gold/audit_schema_and_indexes.sql > data/exploratory/gold/audit_schema.txt 2>&1
--
-- Run alongside audit_coverage_serving.sql / audit_coverage_aggregates.sql / audit_coverage_firm_sidecars.sql
-- (the per-source population + enum-cardinality probes). This file answers "what columns/indexes EXIST";
-- those answer "how populated is each column, per source".

\set ON_ERROR_STOP off
\pset pager off

\echo '############################################################'
\echo '## GOLD API AUDIT — schema + index + stats inventory'
\echo '############################################################'

\echo ''
\echo '=== A. Relation inventory (kind + estimated rows) ==='
\echo '    relkind: r=table  v=view  m=matview.  fct_* are views (no indexes, no ANALYZE stats).'
\echo '    reltuples is the planner ESTIMATE; per-object probes give exact count(*).'
SELECT
    c.relname                                          AS object,
    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view'
                   WHEN 'm' THEN 'matview' ELSE c.relkind::text END AS kind,
    CASE WHEN c.relkind = 'v' THEN NULL
         ELSE c.reltuples::bigint END                  AS est_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN (
      'mart_recall_summary','mart_product_search','mart_firm_profile',
      'fct_recalls_by_month','fct_recalls_by_week','fct_recalls_by_year',
      'fct_recalls_monthly_trend','fct_recalls_by_firm','fct_recalls_by_classification',
      'fct_recall_status','fct_recalls_by_geography','fct_recalls_by_country','fct_units_recalled',
      'dim_date','gold_meta',
      'firm_usda_attributes','firm_uscg_attributes','firm_fda_attributes'
  )
ORDER BY
    CASE WHEN c.relname LIKE 'mart_%' THEN 1 WHEN c.relname LIKE 'fct_%' THEN 2
         WHEN c.relname LIKE 'firm_%' THEN 4 ELSE 3 END,
    c.relname;

\echo ''
\echo '=== B. Column inventory — materialized type + nullability (information_schema) ==='
\echo '    is_nullable=NO means a NOT NULL constraint exists on the relation; for views it reflects the'
\echo '    inferred nullability. udt_name disambiguates arrays (_text), jsonb, timestamptz, etc.'
SELECT
    table_name                                  AS object,
    ordinal_position                            AS ord,
    column_name                                 AS column,
    CASE WHEN data_type = 'ARRAY' THEN udt_name || ' (array)'
         WHEN data_type = 'USER-DEFINED' THEN udt_name
         ELSE data_type END                     AS type,
    is_nullable                                 AS nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'mart_recall_summary','mart_product_search','mart_firm_profile',
      'fct_recalls_by_month','fct_recalls_by_week','fct_recalls_by_year',
      'fct_recalls_monthly_trend','fct_recalls_by_firm','fct_recalls_by_classification',
      'fct_recall_status','fct_recalls_by_geography','fct_recalls_by_country','fct_units_recalled',
      'dim_date','gold_meta',
      'firm_usda_attributes','firm_uscg_attributes','firm_fda_attributes'
  )
ORDER BY table_name, ordinal_position;

\echo ''
\echo '=== C. Index inventory — what the last dbt build actually created (pg_indexes) ==='
\echo '    Confirms the LANDED items: R2 keyset (mart_recall_summary event_date DESC, recall_event_id),'
\echo '    R3 GIN (mart_product_search recall_product_upcs), search_vector GIN, the sidecar (state)/(fei::text).'
\echo '    GAP CHECK: look for any GIN on mart_recall_summary.distribution_state_codes / _country_codes (the'
\echo '    confirmed array filters) — absence here = the headline BLOCKED-ON-GOLD finding.'
\echo '    (CONFIRMED PRESENT 2026-06-16 — G1 landed; both GINs live. See documentation/index_audit.md.)'
SELECT
    tablename                                   AS object,
    indexname                                   AS index,
    CASE WHEN indexdef ILIKE '%using gin%'  THEN 'GIN'
         WHEN indexdef ILIKE '%using btree%' THEN 'btree'
         ELSE 'other' END                       AS method,
    regexp_replace(indexdef, '^CREATE (UNIQUE )?INDEX [^ ]+ ON [^ ]+ USING [a-z]+ ', '') AS keys
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN (
      'mart_recall_summary','mart_product_search','mart_firm_profile',
      'dim_date','gold_meta',
      'firm_usda_attributes','firm_uscg_attributes','firm_fda_attributes'
  )
ORDER BY tablename, indexname;

\echo ''
\echo '    (fct_* are views and carry no indexes — a per-request aggregate scan. Confirm none appear:)'
SELECT count(*) AS fct_index_count
FROM pg_indexes
WHERE schemaname = 'public' AND tablename LIKE 'fct_%';

\echo ''
\echo '=== D. Planner-stats freshness — last_analyze vs the gold build stamp (R7 / ANALYZE concern) ==='
\echo '    Each serving table has an `analyze {{ this }}` post_hook, so last_analyze should be >= the build.'
\echo '    A stale/NULL last_analyze on a heavily-filtered mart = seq-scan risk under API load.'
SELECT
    s.relname                                   AS object,
    s.n_live_tup                                AS live_rows,
    s.last_analyze,
    s.last_autoanalyze,
    (SELECT rebuilt_at FROM gold_meta LIMIT 1)  AS gold_rebuilt_at,
    CASE
        WHEN greatest(s.last_analyze, s.last_autoanalyze) >= (SELECT rebuilt_at FROM gold_meta LIMIT 1)
        THEN 'fresh'
        ELSE 'STALE <<<'
    END                                         AS verdict
FROM pg_stat_user_tables s
WHERE s.schemaname = 'public'
  AND s.relname IN (
      'mart_recall_summary','mart_product_search','mart_firm_profile',
      'dim_date','gold_meta',
      'firm_usda_attributes','firm_uscg_attributes','firm_fda_attributes'
  )
ORDER BY s.relname;

\echo ''
\echo '## END schema + index + stats inventory'
