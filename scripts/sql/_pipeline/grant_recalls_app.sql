-- Grant the restricted production app role (`recalls_app`) its runtime privileges.
-- Run as the OWNER role (the privileged migration role), ONCE, after creating the role
-- and BEFORE `alembic upgrade head` applies migration 0031's *_rejected mutation guard.
-- Setup walkthrough: documentation/operations.md -> "Restricted app role".
-- ADR 0013 (2026-06-09 amendment).
--
-- The runtime needs SELECT (reads), INSERT (bronze loads, extraction_runs / _rejected /
-- manifest appends) and UPDATE (source_watermarks, extraction_runs status). It NEVER needs
-- DELETE or TRUNCATE — and migration 0031 then revokes UPDATE/DELETE/TRUNCATE on the
-- *_rejected tables specifically, leaving them INSERT+SELECT (append-only).

GRANT USAGE ON SCHEMA public TO recalls_app;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO recalls_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO recalls_app;

-- Future tables/sequences created by the owner inherit the same grants automatically,
-- so a new source's bronze table is usable by the runtime without re-granting.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO recalls_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO recalls_app;
