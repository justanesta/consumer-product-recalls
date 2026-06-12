r"""recalls_app restricted-role posture — create (NOLOGIN shell) + the complete grant set.

The single, self-contained source of truth for the restricted runtime role (ADR 0013). On a
cold/clean start ``alembic upgrade head`` reproduces the entire role POSTURE; the operator then sets
the password and enables LOGIN out-of-band. Supersedes the standalone
``scripts/sql/_pipeline/grant_recalls_app.sql`` (removed) and folds in the firm_crosswalk TRUNCATE
grant an earlier draft of this revision carried.

WHY NOLOGIN + no password: a password literal must never be committed. Neon permits a passwordless
role only if it is NOLOGIN, so this creates the role as a NOLOGIN permission shell — nothing secret
lives in the migration. The operator activates it once after the upgrade (see operations.md
"Restricted app role"). Neon requires a PLAINTEXT password (it rejects psql's client-side-hashed
``\password``), so set the password and enable LOGIN in one statement:
    ALTER ROLE recalls_app LOGIN PASSWORD '<strong pw>';

WHY create-via-SQL: Neon auto-adds Console/API/CLI-created roles to ``neon_superuser`` (whose
``pg_write_all_data`` membership silently grants DELETE on every table, defeating the 0031
append-only guard). A SQL-created role is NOT added to neon_superuser and gets standalone-Postgres
default attributes (NOSUPERUSER/NOCREATEDB/NOCREATEROLE/NOREPLICATION/NOBYPASSRLS) — restricted by
construction. So this migration never ALTERs away admin attributes or REVOKEs the membership;
instead it refuses to proceed against a pre-existing *dirty* role (a non-superuser owner cannot
strip BYPASSRLS/REPLICATION anyway) and directs the operator to delete + recreate.

Runs as the OWNER (migrations are operator-run, never in CI). Idempotent on the clean path.

Revision ID: 0033
Revises: 0032
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0033"
down_revision: str | None = "0032"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create the role as a NOLOGIN permission shell if absent (SQL-created => NOT a
    #    neon_superuser member; restricted attributes by default). If it already EXISTS with any
    #    elevated privilege, fail loudly: a migration run as the non-superuser owner cannot strip
    #    BYPASSRLS/REPLICATION nor necessarily REVOKE neon_superuser, so the fix is to delete the
    #    role (Neon console) and re-run — which lands here on the clean CREATE path.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_app') THEN
                IF EXISTS (
                    SELECT 1 FROM pg_roles
                    WHERE rolname = 'recalls_app'
                      AND (rolsuper OR rolcreatedb OR rolcreaterole
                           OR rolreplication OR rolbypassrls)
                ) OR EXISTS (
                    SELECT 1 FROM pg_auth_members am
                    JOIN pg_roles g ON g.oid = am.roleid
                    JOIN pg_roles m ON m.oid = am.member
                    WHERE m.rolname = 'recalls_app' AND g.rolname = 'neon_superuser'
                ) THEN
                    RAISE EXCEPTION USING MESSAGE =
                        'recalls_app exists with elevated privileges (admin attribute '
                        || 'or neon_superuser membership); a non-superuser owner cannot '
                        || 'fully restrict it. Delete the role in the Neon console and '
                        || 're-run alembic upgrade head to recreate it clean via SQL.';
                END IF;
            ELSE
                CREATE ROLE recalls_app NOLOGIN;
            END IF;
        END $$;
        """
    )

    # 2. Base runtime grants: read + append + in-place update on all CURRENT tables/sequences.
    op.execute("GRANT USAGE ON SCHEMA public TO recalls_app;")
    op.execute("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO recalls_app;")
    op.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO recalls_app;")

    # 3. FUTURE owner-created tables/sequences inherit the same grants (a new source's bronze table
    #    is usable without re-granting). Set by the owner, so it applies to objects the owner makes.
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
        "GRANT SELECT, INSERT, UPDATE ON TABLES TO recalls_app;"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO recalls_app;"
    )

    # 4. TRUNCATE on ONLY the two rebuilt-each-run crosswalk tables (resolve-firms /
    #    parse-quantities truncate-reload them). No TRUNCATE anywhere else.
    op.execute("GRANT TRUNCATE ON firm_crosswalk, quantity_crosswalk TO recalls_app;")

    # 5. Re-assert the *_rejected append-only guard (mirrors migration 0031), applied AFTER step 2's
    #    blanket grant so the append-only invariant wins: revoke UPDATE/DELETE/TRUNCATE on every
    #    *_rejected audit table, leaving INSERT + SELECT. (A future *_rejected table needs its own
    #    revoke — same per-table pattern as 0031.)
    op.execute(
        r"""
        DO $$
        DECLARE
            t text;
        BEGIN
            FOR t IN
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' AND tablename LIKE '%\_rejected'
            LOOP
                EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON public.%I FROM recalls_app', t);
            END LOOP;
        END $$;
        """
    )


def downgrade() -> None:
    # Reverse the posture and drop the role (guarded). Revoke every grant first so DROP ROLE is not
    # blocked by dependent privileges; the role owns no objects. The out-of-band password is lost
    # with the role. Clean-teardown only.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_app') THEN
                ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    REVOKE SELECT, INSERT, UPDATE ON TABLES FROM recalls_app;
                ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    REVOKE USAGE, SELECT ON SEQUENCES FROM recalls_app;
                REVOKE ALL ON ALL TABLES IN SCHEMA public FROM recalls_app;
                REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM recalls_app;
                REVOKE ALL ON SCHEMA public FROM recalls_app;
                DROP ROLE recalls_app;
            END IF;
        END $$;
        """
    )
