-- Validate the serving-layer gold-readiness pass (serving-layer-gold-readiness-plan.md, ADR 0042)
-- after `alembic upgrade head` (migration 0034) + a full `dbt build`. One pass over every done-marker:
-- the read-only role (R1), the gold-only grant scope, the R2/R3 indexes, gold_meta (R4), the R5
-- sidecar rename, ANALYZE freshness (R7), and the per-mart row counts (O3).
--
-- HOW TO RUN. Sections 1-7 are catalog/data introspection and are correct regardless of the connected
-- role (has_*_privilege() inspects recalls_readonly's grants, not the caller's), so the quickest pass is
-- as the OWNER right after the build:
--     psql "$NEON_OWNER_URL" -f scripts/sql/_pipeline/verify_gold_readiness.sql
-- Section 8 (the live read-only proof) is only meaningful when you run the file AS recalls_readonly
-- (post-activation), exactly like verify_recalls_app_grants.sql's Section 3 — run it once more via the
-- activated NEON_DATABASE_URL_RO to see the write get rejected.
--
-- PASS = every `verdict` in Sections 1-7 reads PASS, and Section 8's UPDATE fails with "cannot execute
-- UPDATE in a read-only transaction" (or "permission denied") when run as recalls_readonly. If the role
-- does not exist yet, has_*_privilege() raises "role recalls_readonly does not exist" — apply migration
-- 0034 first (operations.md -> "Restricted app role"). If a gold relation is missing, the build was not
-- green — re-run `dbt build`.

\echo '=== Section 0: connected role + database ==='
SELECT current_user                         AS connected_as,
       current_database()                   AS db,
       (current_user = 'recalls_readonly')  AS section8_live_check_meaningful;

\echo ''
\echo '=== Section 1 (R1): recalls_readonly role posture — restricted attrs + read-only belt ==='
\echo '    expect every admin attribute f, member_of_neon_superuser f, read_only_belt t.'
\echo '    can_login is t only AFTER you run ALTER ROLE recalls_readonly LOGIN PASSWORD (info, not gated).'
SELECT
    rolcanlogin     AS can_login,
    rolsuper        AS superuser,
    rolcreatedb     AS createdb,
    rolcreaterole   AS createrole,
    rolreplication  AS replication,
    rolbypassrls    AS bypass_rls,
    EXISTS (
        SELECT 1 FROM pg_auth_members am
        JOIN pg_roles g ON g.oid = am.roleid
        JOIN pg_roles m ON m.oid = am.member
        WHERE m.rolname = 'recalls_readonly' AND g.rolname = 'neon_superuser'
    )                                                       AS member_of_neon_superuser,
    ('default_transaction_read_only=on' = ANY(rolconfig))   AS read_only_belt,
    CASE
        WHEN NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
             AND NOT EXISTS (
                 SELECT 1 FROM pg_auth_members am
                 JOIN pg_roles g ON g.oid = am.roleid
                 JOIN pg_roles m ON m.oid = am.member
                 WHERE m.rolname = 'recalls_readonly' AND g.rolname = 'neon_superuser'
             )
             AND ('default_transaction_read_only=on' = ANY(rolconfig))
        THEN 'PASS (restricted + read-only belt)'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM pg_roles
WHERE rolname = 'recalls_readonly';

\echo ''
\echo '=== Section 2 (R1): GOLD-ONLY grant scope — gold readable, bronze/silver/audit hidden, no writes ==='
\echo '    gold SELECT t proves the dbt grant_gold_readonly post-hook fired; bronze/silver/audit SELECT f'
\echo '    proves the migration did NOT GRANT SELECT ON ALL TABLES (gold-only operator decision, ADR 0042).'
WITH checks (kind, category, obj, priv, expected) AS (
    VALUES
        ('schema', 'schema usage',                 'public',               'USAGE',  true),
        ('table',  'gold serving mart',            'mart_recall_summary',  'SELECT', true),
        ('table',  'gold serving mart',            'mart_product_search',  'SELECT', true),
        ('table',  'gold serving mart',            'mart_firm_profile',    'SELECT', true),
        ('table',  'gold meta (R4)',               'gold_meta',            'SELECT', true),
        ('table',  'gold aggregate view',          'fct_recalls_by_month', 'SELECT', true),
        ('table',  'gold aggregate view',          'fct_units_recalled',   'SELECT', true),
        ('table',  'gold-only: bronze hidden',     'cpsc_recalls_bronze',  'SELECT', false),
        ('table',  'gold-only: silver hidden',     'recall_event',         'SELECT', false),
        ('table',  'gold-only: audit hidden',      'extraction_runs',      'SELECT', false),
        ('table',  'no write on gold',             'mart_recall_summary',  'INSERT', false),
        ('table',  'no write on gold',             'mart_recall_summary',  'UPDATE', false),
        ('table',  'no write on gold',             'mart_recall_summary',  'DELETE', false)
)
SELECT
    category,
    obj,
    priv,
    expected,
    CASE kind
        WHEN 'schema' THEN has_schema_privilege('recalls_readonly', obj, priv)
        ELSE has_table_privilege('recalls_readonly', 'public.' || obj, priv)
    END AS actual,
    CASE
        WHEN (CASE kind
                  WHEN 'schema' THEN has_schema_privilege('recalls_readonly', obj, priv)
                  ELSE has_table_privilege('recalls_readonly', 'public.' || obj, priv)
              END) = expected
        THEN 'PASS'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM checks
ORDER BY category, obj, priv;

\echo ''
\echo '=== Section 3 (R2/R3): serving-mart indexes built by the last dbt build ==='
\echo '    expect found t for both — the R2 keyset index and the R3 recall-level UPC GIN.'
SELECT label, found,
       CASE WHEN found THEN 'PASS' ELSE 'FAIL  <<<<' END AS verdict
FROM (
    SELECT 'R2 mart_recall_summary (published_at DESC, recall_event_id)' AS label,
           EXISTS (
               SELECT 1 FROM pg_indexes
               WHERE schemaname = 'public' AND tablename = 'mart_recall_summary'
                 AND indexdef ILIKE '%published_at DESC%'
                 AND indexdef ILIKE '%recall_event_id%'
           ) AS found
    UNION ALL
    SELECT 'R3 mart_product_search recall_product_upcs GIN',
           EXISTS (
               SELECT 1 FROM pg_indexes
               WHERE schemaname = 'public' AND tablename = 'mart_product_search'
                 AND indexdef ILIKE '%gin%'
                 AND indexdef ILIKE '%recall_product_upcs%'
           )
) t
ORDER BY label;

\echo ''
\echo '=== Section 4 (R4): gold_meta — exactly one row, rebuilt_at + schema_version populated ==='
\echo '    rebuilt_at = the dbt run_started_at; it should ADVANCE after each build (compare across two runs).'
SELECT
    count(*)             AS row_count,
    max(rebuilt_at)      AS rebuilt_at,
    max(schema_version)  AS schema_version,
    CASE
        WHEN count(*) = 1
             AND bool_and(rebuilt_at IS NOT NULL)
             AND bool_and(schema_version IS NOT NULL)
        THEN 'PASS'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM gold_meta;

\echo ''
\echo '=== Section 5 (R5): mart_firm_profile sidecar columns renamed to firm_{usda,uscg,fda}_attributes ==='
\echo '    expect new_names_present t and old_names_present f.'
WITH cols AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'mart_firm_profile'
)
SELECT
    (SELECT bool_and(c IN (SELECT column_name FROM cols))
       FROM (VALUES ('firm_usda_attributes'), ('firm_uscg_attributes'),
                    ('firm_fda_attributes')) v(c))                       AS new_names_present,
    (SELECT bool_or(c IN (SELECT column_name FROM cols))
       FROM (VALUES ('establishment_attributes'), ('manufacturer_attributes'),
                    ('fda_attributes')) v(c))                            AS old_names_present,
    CASE
        WHEN (SELECT bool_and(c IN (SELECT column_name FROM cols))
                FROM (VALUES ('firm_usda_attributes'), ('firm_uscg_attributes'),
                             ('firm_fda_attributes')) v(c))
             AND NOT (SELECT bool_or(c IN (SELECT column_name FROM cols))
                        FROM (VALUES ('establishment_attributes'), ('manufacturer_attributes'),
                                     ('fda_attributes')) v(c))
        THEN 'PASS'
        ELSE 'FAIL  <<<<'
    END AS verdict;

\echo ''
\echo '=== Section 6 (R7): ANALYZE freshness — last_analyze for the serving marts is >= the gold build ==='
\echo '    the analyze post-hooks run during the build, so last_analyze should be >= gold_meta.rebuilt_at.'
SELECT
    relname                                         AS mart,
    last_analyze,
    (SELECT rebuilt_at FROM gold_meta LIMIT 1)      AS gold_rebuilt_at,
    CASE
        WHEN last_analyze >= (SELECT rebuilt_at FROM gold_meta LIMIT 1)
        THEN 'PASS'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname IN ('mart_recall_summary', 'mart_product_search', 'mart_firm_profile')
ORDER BY relname;

\echo ''
\echo '=== Section 7 (O3): per-mart row counts — hand these to the API team for pagination/cache sizing ==='
SELECT 'mart_recall_summary' AS mart, count(*) AS n FROM mart_recall_summary
UNION ALL SELECT 'mart_product_search', count(*) FROM mart_product_search
UNION ALL SELECT 'mart_firm_profile',  count(*) FROM mart_firm_profile
UNION ALL SELECT 'gold_meta',           count(*) FROM gold_meta
ORDER BY mart;

\echo ''
\echo '=== Section 8 (R1 LIVE): read-only enforcement — the next statement MUST fail AS recalls_readonly ==='
\echo '    Run this file AS recalls_readonly (Section 0 = t). If connected as the owner, the UPDATE'
\echo '    SUCCEEDS (UPDATE 0, WHERE false) and proves nothing — re-run via NEON_DATABASE_URL_RO.'
\echo '    WHERE false guarantees zero rows are ever touched even if the write were wrongly permitted.'
UPDATE gold_meta SET schema_version = schema_version WHERE false;
\echo '    If you saw "ERROR: cannot execute UPDATE in a read-only transaction" (or "permission denied")'
\echo '    above, the read-only role is correctly locked down.'
