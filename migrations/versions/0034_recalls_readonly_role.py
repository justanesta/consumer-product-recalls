r"""recalls_readonly — dedicated read-only role for the public serving API (recalls-api).

Separate from recalls_app (0033, the pipeline READ+WRITE runtime role). The open, no-auth API
connects as this role: SELECT on the GOLD serving relations only, plus a session-level
default_transaction_read_only belt so even a SELECT-able function or a planner surprise cannot
mutate. Created as a NOLOGIN SQL shell for the SAME reason as 0033 (a SQL-created role is NOT added
to neon_superuser, whose pg_write_all_data would re-grant write; no password literal is committed).
Operator activates once, out-of-band:
    ALTER ROLE recalls_readonly LOGIN PASSWORD '<strong pw>';
Then expose its connection string to the API as NEON_DATABASE_URL_RO (SecretStr).

Grant scope = GOLD-ONLY (operator decision 2026-06-13). This migration provisions ONLY the role,
schema USAGE, and the read-only session belt — NOT the per-table SELECT grants. dbt rebuilds the
gold tables every run (drop + recreate), wiping any grant on the old object, so the gold grants
live in dbt: a tolerant `grant_gold_readonly` post_hook on the `gold/` folder re-applies the
SELECT grant every build (gold-only; covers new marts + gold_meta; skipped where the role is
absent, e.g. dev/CI). The fct_* are views that run with the OWNER's privileges, so the API reads
the rollups without any silver grant. Deliberately NOT `GRANT SELECT ON ALL TABLES` + `ALTER
DEFAULT PRIVILEGES`, which would expose bronze/silver/audit.

Runs as the OWNER (operator-run, never in CI). Idempotent on the clean path.

Revision ID: 0034
Revises: 0033
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0034"
down_revision: str | None = "0033"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create as a NOLOGIN shell if absent; fail loudly on a pre-existing *dirty* role (any admin
    #    attribute or neon_superuser membership) — a non-superuser owner cannot restrict it, so the
    #    fix is delete-in-Neon-console + re-run, landing on the clean CREATE path (mirrors 0033).
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_readonly') THEN
                IF EXISTS (
                    SELECT 1 FROM pg_roles
                    WHERE rolname = 'recalls_readonly'
                      AND (rolsuper OR rolcreatedb OR rolcreaterole
                           OR rolreplication OR rolbypassrls)
                ) OR EXISTS (
                    SELECT 1 FROM pg_auth_members am
                    JOIN pg_roles g ON g.oid = am.roleid
                    JOIN pg_roles m ON m.oid = am.member
                    WHERE m.rolname = 'recalls_readonly' AND g.rolname = 'neon_superuser'
                ) THEN
                    RAISE EXCEPTION USING MESSAGE =
                        'recalls_readonly exists with elevated privileges; a non-superuser owner '
                        || 'cannot fully restrict it. Delete the role in the Neon console and '
                        || 're-run alembic upgrade head to recreate it clean via SQL.';
                END IF;
            ELSE
                CREATE ROLE recalls_readonly NOLOGIN;
            END IF;
        END $$;
        """
    )

    # 2. Schema usage (needed to resolve objects). NO sequence privileges (read-only never advances
    #    a sequence), NO table writes. The per-table gold SELECT grants are applied by dbt (see the
    #    docstring's grant_gold_readonly post_hook) so they survive the nightly drop+recreate of the
    #    gold tables — a one-time grant here would be wiped on the next build.
    op.execute("GRANT USAGE ON SCHEMA public TO recalls_readonly;")

    # 3. Belt-and-braces: force every session opened by this role to read-only.
    op.execute("ALTER ROLE recalls_readonly SET default_transaction_read_only = on;")


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recalls_readonly') THEN
                ALTER ROLE recalls_readonly RESET default_transaction_read_only;
                REVOKE ALL ON ALL TABLES IN SCHEMA public FROM recalls_readonly;
                REVOKE ALL ON SCHEMA public FROM recalls_readonly;
                DROP ROLE recalls_readonly;
            END IF;
        END $$;
        """
    )
