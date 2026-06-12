-- Diagnose the SOURCE of recalls_app's effective table privileges: DIRECT grant vs INHERITED
-- (role membership) vs PUBLIC. Read-only; re-runnable; correct from any role that can read the
-- catalog (run it as the owner — you already are).
--
-- WHY THIS EXISTS — verify_recalls_app_grants.sql reports EFFECTIVE privilege via
-- has_table_privilege('recalls_app', ...), which returns TRUE whether the privilege was granted
-- DIRECTLY to recalls_app, INHERITED through a role it is a member of, or granted to PUBLIC. That
-- run flagged recalls_app holding DELETE on bronze and UPDATE/DELETE on the *_rejected tables —
-- privileges the restricted (ADR 0013) posture forbids. The remediation differs by source:
--   * DIRECT grant     -> `REVOKE <priv> ... FROM recalls_app` removes it.
--   * INHERITED         -> revoke from recalls_app is a NO-OP; must drop the membership instead.
--   * PUBLIC            -> `REVOKE <priv> ... FROM PUBLIC` (affects every role).
-- So read the source here BEFORE writing any REVOKE.
--
-- HOW IT WORKS — sections 2/3 expand pg_class.relacl with aclexplode(), i.e. the actual GRANT
-- records in the catalog, NOT the inherited rollup. Section 1 lists recalls_app's memberships; if
-- member_of = (none) then nothing is inherited and every effective privilege is necessarily a
-- direct or PUBLIC grant — both visible in section 2.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/_pipeline/diagnose_recalls_app_grant_sources.sql

\set ON_ERROR_STOP on

\echo
\echo '=== 1) recalls_app role attributes + memberships (the inheritance source, if any) ==='
\echo '    member_of = (none)  => nothing is inherited; every effective privilege is then a DIRECT'
\echo '    grant or a PUBLIC grant, both shown in section 2. A privileged parent here (e.g. an owner'
\echo '    or neon_superuser role) would instead explain blanket privileges via inheritance.'
SELECT r.rolname,
       r.rolinherit     AS inherits,
       r.rolsuper       AS superuser,
       r.rolcreatedb    AS createdb,
       r.rolcreaterole  AS createrole,
       r.rolreplication AS replication,
       r.rolbypassrls   AS bypass_rls,
       r.rolcanlogin    AS can_login,
       COALESCE(
         (SELECT string_agg(m.rolname, ', ' ORDER BY m.rolname)
            FROM pg_auth_members am
            JOIN pg_roles m ON m.oid = am.roleid
           WHERE am.member = r.oid),
         '(none)'
       ) AS member_of
  FROM pg_roles r
 WHERE r.rolname = 'recalls_app';

\echo
\echo '=== 2) DIRECT grants of DELETE/UPDATE/TRUNCATE on public tables, to recalls_app or PUBLIC ==='
\echo '    Catalog ACL entries (the literal GRANT records). A row (table, DELETE, recalls_app) means'
\echo '    `REVOKE DELETE ... FROM recalls_app` will remove it. grantee=PUBLIC => revoke FROM PUBLIC.'
\echo '    NOTE: UPDATE on bronze / extraction_runs / source_watermarks is LEGITIMATE — only UPDATE on'
\echo '    *_rejected and DELETE/TRUNCATE outside the crosswalks are the over-grant to undo.'
SELECT c.relname                     AS table_name,
       COALESCE(g.rolname, 'PUBLIC') AS grantee,
       acl.privilege_type            AS privilege,
       COALESCE(gr.rolname, '?')     AS granted_by
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(c.relacl) AS acl
  LEFT JOIN pg_roles g  ON g.oid  = acl.grantee
  LEFT JOIN pg_roles gr ON gr.oid = acl.grantor
 WHERE n.nspname = 'public'
   AND c.relkind = 'r'
   AND acl.privilege_type IN ('DELETE', 'UPDATE', 'TRUNCATE')
   AND (acl.grantee = 0 OR g.rolname = 'recalls_app')
 ORDER BY c.relname, grantee, privilege;

\echo
\echo '=== 3) Direct-grant tally per privilege (how widespread is the over-grant?) ==='
\echo '    e.g. DELETE | recalls_app | <N tables>  confirms a blanket `GRANT ... DELETE ON ALL TABLES`'
\echo '    and tells the remediation whether to revoke ON ALL TABLES vs a targeted list.'
SELECT acl.privilege_type            AS privilege,
       COALESCE(g.rolname, 'PUBLIC') AS grantee,
       count(*)                      AS tables
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(c.relacl) AS acl
  LEFT JOIN pg_roles g ON g.oid = acl.grantee
 WHERE n.nspname = 'public'
   AND c.relkind = 'r'
   AND acl.privilege_type IN ('DELETE', 'UPDATE', 'TRUNCATE')
   AND (acl.grantee = 0 OR g.rolname = 'recalls_app')
 GROUP BY acl.privilege_type, COALESCE(g.rolname, 'PUBLIC')
 ORDER BY privilege, grantee;
