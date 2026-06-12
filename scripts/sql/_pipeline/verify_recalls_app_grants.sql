-- Validate the restricted production app role (`recalls_app`) before cron go-live (ADR 0013).
--
-- Option A posture: the SQLAlchemy/CLI runtime (extractors, `resolve-firms`, `parse-quantities`,
-- the future API) connects as `recalls_app`; dbt connects as the owner. This script proves the
-- runtime role has exactly the grants it needs and NOTHING more — in particular that the
-- `*_rejected` audit tables are append-only (migration 0031 revoked UPDATE/DELETE/TRUNCATE).
--
-- HOW TO RUN — connect AS `recalls_app` so Section 3's live check is meaningful. During the H-a
-- pre-live window your `NEON_DATABASE_URL` points at the recalls_app DSN, and that URI carries the
-- role, so:
--     psql "$NEON_DATABASE_URL" -f scripts/sql/_pipeline/verify_recalls_app_grants.sql
-- (Sections 1–2 use catalog introspection and are correct regardless of the connected role; only
-- Section 3 needs the live recalls_app session.) See operations.md -> "Local validation pass".
--
-- PASS = every row in Sections 0.5–2 reads `PASS`, and Section 3's DELETE fails with
-- `permission denied`. If `recalls_app` does not exist yet, has_*_privilege() raises
-- "role recalls_app does not exist" — apply migration 0033 (creates the role + grants), then
-- activate it (set password + enable LOGIN) first (operations.md -> "Restricted app role").

\echo '=== Section 0: connected role (must be recalls_app for the Section 3 live check) ==='
SELECT current_user AS connected_as,
       current_database() AS db,
       (current_user = 'recalls_app') AS section3_live_check_meaningful;

\echo ''
\echo '=== Section 0.5: role posture — restricted attributes + NOT a member of neon_superuser ==='
\echo '    expect every attribute f and member_of_neon_superuser f. A t here means the role is'
\echo '    admin-equivalent (Neon auto-grants console/API/CLI roles neon_superuser, whose'
\echo '    pg_write_all_data silently confers DELETE) and Sections 1-2 are moot — recreate the role'
\echo '    from SQL via migration 0033 (operations.md -> "Restricted app role").'
SELECT
    rolsuper        AS superuser,
    rolcreatedb     AS createdb,
    rolcreaterole   AS createrole,
    rolreplication  AS replication,
    rolbypassrls    AS bypass_rls,
    EXISTS (
        SELECT 1 FROM pg_auth_members am
        JOIN pg_roles g ON g.oid = am.roleid
        JOIN pg_roles m ON m.oid = am.member
        WHERE m.rolname = 'recalls_app' AND g.rolname = 'neon_superuser'
    ) AS member_of_neon_superuser,
    CASE
        WHEN NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
             AND NOT EXISTS (
                 SELECT 1 FROM pg_auth_members am
                 JOIN pg_roles g ON g.oid = am.roleid
                 JOIN pg_roles m ON m.oid = am.member
                 WHERE m.rolname = 'recalls_app' AND g.rolname = 'neon_superuser'
             )
        THEN 'PASS (restricted)'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM pg_roles
WHERE rolname = 'recalls_app';

\echo ''
\echo '=== Section 0.6: database TEMP privilege (the set-based dedup lookup creates TEMP tables) ==='
\echo '    expect t — recalls_app needs database TEMPORARY (held via the PUBLIC default) for'
\echo '    BronzeLoader._fetch_existing_hashes_staged. It does NOT need CREATE on schema public:'
\echo '    TEMP tables live in pg_temp, governed by the database TEMPORARY privilege.'
SELECT
    has_database_privilege('recalls_app', current_database(), 'TEMP') AS can_create_temp,
    CASE
        WHEN has_database_privilege('recalls_app', current_database(), 'TEMP') THEN 'PASS'
        ELSE 'FAIL  <<<<  (GRANT TEMPORARY ON DATABASE '
             || current_database() || ' TO recalls_app)'
    END AS verdict;

\echo ''
\echo '=== Section 1: schema, bronze, run-bookkeeping & crosswalk grants (expected vs actual) ==='
WITH checks (kind, category, obj, priv, expected) AS (
    VALUES
        ('schema', 'schema (no DDL for runtime)', 'public',              'USAGE',    true),
        ('schema', 'schema (no DDL for runtime)', 'public',              'CREATE',   false),
        ('table',  'bronze working table',        'cpsc_recalls_bronze', 'SELECT',   true),
        ('table',  'bronze working table',        'cpsc_recalls_bronze', 'INSERT',   true),
        ('table',  'bronze working table',        'cpsc_recalls_bronze', 'UPDATE',   true),
        ('table',  'bronze working table',        'cpsc_recalls_bronze', 'DELETE',   false),
        ('table',  'run bookkeeping',             'extraction_runs',     'INSERT',   true),
        ('table',  'run bookkeeping',             'extraction_runs',     'UPDATE',   true),
        ('table',  'run bookkeeping',             'source_watermarks',   'UPDATE',   true),
        ('table',  'crosswalk (truncate-reload)', 'firm_crosswalk',      'INSERT',   true),
        ('table',  'crosswalk (truncate-reload)', 'firm_crosswalk',      'TRUNCATE', true),
        ('table',  'crosswalk (truncate-reload)', 'quantity_crosswalk',  'TRUNCATE', true)
)
SELECT
    category,
    obj,
    priv,
    expected,
    CASE kind
        WHEN 'schema' THEN has_schema_privilege('recalls_app', obj, priv)
        ELSE has_table_privilege('recalls_app', 'public.' || obj, priv)
    END AS actual,
    CASE
        WHEN (CASE kind
                  WHEN 'schema' THEN has_schema_privilege('recalls_app', obj, priv)
                  ELSE has_table_privilege('recalls_app', 'public.' || obj, priv)
              END) = expected
        THEN 'PASS'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM checks
ORDER BY category, obj, priv;

\echo ''
\echo '=== Section 2: *_rejected append-only guard across ALL source reject tables (migration 0031) ==='
\echo '    expect ins=t sel=t upd=f del=f trunc=f for every table'
SELECT
    tablename AS rejected_table,
    has_table_privilege('recalls_app', 'public.' || tablename, 'INSERT')   AS ins,
    has_table_privilege('recalls_app', 'public.' || tablename, 'SELECT')   AS sel,
    has_table_privilege('recalls_app', 'public.' || tablename, 'UPDATE')   AS upd,
    has_table_privilege('recalls_app', 'public.' || tablename, 'DELETE')   AS del,
    has_table_privilege('recalls_app', 'public.' || tablename, 'TRUNCATE') AS trunc,
    CASE
        WHEN     has_table_privilege('recalls_app', 'public.' || tablename, 'INSERT')
             AND has_table_privilege('recalls_app', 'public.' || tablename, 'SELECT')
             AND NOT has_table_privilege('recalls_app', 'public.' || tablename, 'UPDATE')
             AND NOT has_table_privilege('recalls_app', 'public.' || tablename, 'DELETE')
             AND NOT has_table_privilege('recalls_app', 'public.' || tablename, 'TRUNCATE')
        THEN 'PASS (append-only)'
        ELSE 'FAIL  <<<<'
    END AS verdict
FROM pg_tables
WHERE schemaname = 'public' AND tablename LIKE '%\_rejected'
ORDER BY tablename;

\echo ''
\echo '=== Section 3: LIVE append-only proof — the next statement MUST fail with "permission denied" ==='
\echo '    Run this file AS recalls_app (see Section 0). If connected_as was the owner, the DELETE'
\echo '    SUCCEEDS and proves nothing — re-run as recalls_app. WHERE false guarantees zero rows are'
\echo '    ever touched even if the privilege were wrongly present.'
DELETE FROM cpsc_recalls_rejected WHERE false;
\echo '    If you saw "ERROR: permission denied for table cpsc_recalls_rejected" above, the guard works.'
