-- Grant the restricted production app role (`recalls_app`) its runtime privileges.
-- Run as the OWNER role (the privileged migration role), ONCE, after creating the role
-- and BEFORE `alembic upgrade head` applies migration 0031's *_rejected mutation guard.
-- Setup walkthrough: documentation/operations.md -> "Restricted app role".
-- ADR 0013 (2026-06-09 amendment).
--
-- The runtime needs SELECT (reads), INSERT (bronze loads, extraction_runs / _rejected /
-- manifest appends) and UPDATE (source_watermarks, extraction_runs status). The ONLY TRUNCATE it
-- needs is on the two enrichment crosswalk tables, which `recalls resolve-firms` /
-- `recalls parse-quantities` truncate-reload each run (granted at the bottom). Everywhere else it
-- has no DELETE/TRUNCATE, and migration 0031 revokes UPDATE/DELETE/TRUNCATE on the *_rejected
-- tables specifically, leaving them INSERT+SELECT (append-only).

GRANT USAGE ON SCHEMA public TO recalls_app;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO recalls_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO recalls_app;

-- Future tables/sequences created by the owner inherit the same grants automatically,
-- so a new source's bronze table is usable by the runtime without re-granting.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO recalls_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO recalls_app;

-- TRUNCATE on the two rebuilt-each-run crosswalk tables ONLY (the runtime truncate-reloads them;
-- everything else stays no-TRUNCATE). firm_crosswalk (migration 0024) exists when this runs;
-- quantity_crosswalk's grant lives in migration 0032, which creates it after this script runs.
GRANT TRUNCATE ON firm_crosswalk TO recalls_app;
