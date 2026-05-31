-- Connection-target sanity check for the production (main) backfill / cutover
-- runbook (documentation/operations.md → "Production (main) backfill / cutover").
--
-- The cutover temporarily repoints local NEON_DATABASE_URL at the `main` branch,
-- which breaks the ADR 0005 steady-state invariant (local always points at
-- `dev`). Run this BEFORE any write to sanity-check the target, and AFTER the
-- mandatory revert to confirm you are back on `dev`.
--
-- IMPORTANT — what this CANNOT tell you: on Neon the Postgres backend sits
-- behind a proxy over loopback, so `inet_server_addr()` returns ::1 on EVERY
-- branch, and database (neondb) / role (neondb_owner) / version are identical
-- across dev and main. None of those identify the branch. The branch is named
-- only by the endpoint HOST in your connection string, which is not visible
-- in-session. Confirm it OUTSIDE psql:
--     echo "$PGHOST"                                                       # psql / PG* path (.envrc)
--     echo "$NEON_DATABASE_URL" | sed -E 's#://([^:]+):[^@]*@#://\1:****@#'  # alembic/recalls path (.env)
-- Both must show the `main` endpoint (Neon console → Branches → main →
-- Connection details). NB these are TWO separate credentials: psql uses PG*,
-- but the seed (alembic + recalls) uses NEON_DATABASE_URL — verify both.
--
-- What this DOES give you (section B): a branch-STATE fingerprint. A freshly
-- created production `main` is empty/unmigrated (no alembic_version, no bronze
-- tables); `dev` is fully migrated + populated. Caveat: if `main` was branched
-- from a populated `dev` it would inherit that state — then fall back to the
-- host check above.
--
-- No parameters. Run as:  psql -f scripts/sql/_pipeline/whoami.sql

\pset null '<NULL>'

-- A. Session identity. Reported identically on dev and main — for sanity only,
--    NOT a branch discriminator (see header). server_ip is ::1 on Neon.
\echo '=== A. session identity (same on every branch — not a branch signal) ==='
select
    current_database()                  as database,
    current_user                        as connected_as,
    inet_server_addr()                  as server_ip_ignore,
    split_part(version(), ' on ', 1)    as server_version;

-- B. Branch-state fingerprint. Pre-seed `main`: public_tables low,
--    alembic_version_tbl + bronze tables NULL (absent). `dev`: all present.
\echo '=== B. branch-state fingerprint (empty/absent => fresh main; populated => dev) ==='
select
    (select count(*) from information_schema.tables
       where table_schema = 'public' and table_type = 'BASE TABLE')  as public_tables,
    to_regclass('public.alembic_version')                            as alembic_version_tbl,
    to_regclass('public.cpsc_recalls_bronze')                        as cpsc_bronze_tbl,
    to_regclass('public.nhtsa_recalls_bronze')                       as nhtsa_bronze_tbl;
