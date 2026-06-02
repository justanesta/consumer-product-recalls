"""Database engine factory — the single home for SQLAlchemy engine construction.

Every connection across the pipeline (extractors, CLI, recovery) is built here so
they share one Neon-appropriate configuration.

Neon is serverless Postgres: it terminates idle connections (~300s) and can
cold-start, and ``pool_pre_ping`` only validates a connection at *checkout* — it
gives no protection once a connection is handed to application code. These
extractors are single-threaded batch jobs that hold at most one connection at a
time, so we use ``NullPool`` (a fresh DBAPI connection per ``engine.begin()`` /
``engine.connect()`` block, closed on exit) rather than a persistent pool. That
eliminates the idle-in-pool drop class entirely and mirrors the connection
lifecycle of the subprocess-per-chunk seed that survives Neon drops.

``connect_args`` enables libpq TCP keepalives (psycopg2) so a connection held
open across a long single transaction — e.g. NHTSA's ~321k-row deep-rescan load —
is less likely to be reaped by an intermediary. NullPool does NOT protect a drop
that occurs *mid-transaction*; a targeted reconnect-retry around the load covers
that (deep-rescan reliability plan, W5).

See ``documentation/audit/deep_rescan_reliability_audit.md`` for the failure
modes this addresses.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.pool import NullPool

# libpq TCP keepalive tuning (psycopg2). Probes begin after 30s idle, repeat
# every 10s, and drop the connection after 5 failed probes (~80s to detect a
# dead peer); a connect attempt times out at 10s rather than hanging on a Neon
# cold start.
_CONNECT_ARGS: dict[str, Any] = {
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}


def make_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy Engine configured for Neon serverless Postgres.

    Uses ``NullPool`` (no pooling) plus libpq TCP keepalives — see the module
    docstring for the rationale. All pipeline code must build engines through
    this factory; ``tests/config/test_db.py`` asserts that ``src/`` calls no
    other ``create_engine``.
    """
    return sa.create_engine(
        database_url,
        poolclass=NullPool,
        connect_args=_CONNECT_ARGS,
    )
