from __future__ import annotations

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

# Inject the database URL from the environment.
# Phase 1: reads NEON_DATABASE_URL directly to avoid a Settings() dependency on
# R2 secrets that are not yet provisioned. Phase 2 switches to:
#   from src.config.settings import Settings
#   config.set_main_option("sqlalchemy.url", Settings().neon_database_url.get_secret_value())
neon_url = os.environ.get("NEON_DATABASE_URL")
if neon_url:
    config.set_main_option("sqlalchemy.url", neon_url)

# Phase 2 adds SQLAlchemy declarative Base.metadata here.
target_metadata = None


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
