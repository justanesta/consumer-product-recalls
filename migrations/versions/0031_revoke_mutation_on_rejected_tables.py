"""Revoke TRUNCATE/DELETE/UPDATE on *_rejected tables from the restricted app role (ADR 0013)

Revision ID: 0031
Revises: 0030
Create Date: 2026-06-09

ADR 0013 designs the per-source ``*_rejected`` tables as an **append-only** audit trail
(schema-drift forensics, re-ingest source per ADR 0014, data-loss accounting). This
migration enforces that as a Postgres invariant in **production** rather than relying on
operator discipline: it revokes ``TRUNCATE``, ``DELETE`` and ``UPDATE`` on every
``*_rejected`` table from the **restricted application role**, leaving only ``INSERT`` and
``SELECT``.

Two-role prerequisite (see ``documentation/operations.md`` → "Restricted app role"):

  * **Migrations** run as a privileged role (table owner) — this migration, and all
    future Alembic migrations, retain full DDL/DML rights.
  * **The pipeline runtime** (extractors, transform cron, the future read-only API) connects
    as the restricted role named below. ``NEON_DATABASE_URL`` for those runs points at the
    restricted role; the privileged role is used only by ``alembic``.

The migration is **dynamic** — it enumerates every ``public.*_rejected`` table at apply
time, so it covers all nine source reject tables today and any future source's reject table
automatically (no list to keep in sync). It is **safe**: REVOKE of a privilege the role was
never granted is a Postgres no-op, so this is belt-and-suspenders over a minimally-granted
role. It **requires the role to exist** — if it does not, the migration raises with a pointer
to the runbook (create the role first, then re-run ``alembic upgrade head``).

Dev branches keep full privileges (truncating a ``*_rejected`` table while iterating on a
buggy schema is fine) — only run this against environments using the restricted role.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0031"
down_revision: str | None = "0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The restricted application role the pipeline/API connect as. The operator creates a Neon
# role with this exact name (see the runbook). Change here only if the convention changes.
APP_ROLE = "recalls_app"


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            r record;
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{APP_ROLE}') THEN
                RAISE EXCEPTION
                    'Restricted app role "%" does not exist. Create it first '
                    '(see documentation/operations.md -> "Restricted app role"), then re-run '
                    '"alembic upgrade head".', '{APP_ROLE}';
            END IF;
            FOR r IN
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' AND tablename LIKE '%\\_rejected'
            LOOP
                EXECUTE format(
                    'REVOKE TRUNCATE, DELETE, UPDATE ON public.%I FROM %I',
                    r.tablename, '{APP_ROLE}'
                );
            END LOOP;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            r record;
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{APP_ROLE}') THEN
                RETURN;
            END IF;
            FOR r IN
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' AND tablename LIKE '%\\_rejected'
            LOOP
                EXECUTE format(
                    'GRANT TRUNCATE, DELETE, UPDATE ON public.%I TO %I',
                    r.tablename, '{APP_ROLE}'
                );
            END LOOP;
        END $$;
        """
    )
