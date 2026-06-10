"""Run dbt against an ephemeral Neon test branch (C24/C30 e2e harness).

The C23 ``test_db_url`` fixture yields a branch connection URI; ``branch_env`` splits it into the
NEON_HOST/USER/PASSWORD/DBNAME vars dbt's ``profiles.yml`` reads via ``env_var``, and ``run_dbt``
shells out to dbt with that env so the build/test targets the throwaway branch, never prod.
``branch_env`` is pure (URI parsing) → unit-tested in ``test_dbt_runner.py``; ``run_dbt`` is the
subprocess seam (only exercised in the live e2e tests, which skip without NEON_API_KEY).
"""

from __future__ import annotations

import os
import subprocess
from urllib.parse import urlparse


def branch_env(connection_uri: str) -> dict[str, str]:
    """Split a Neon connection URI into the NEON_* env vars dbt connects with."""
    parsed = urlparse(connection_uri)
    host, user, dbname = parsed.hostname, parsed.username, parsed.path.lstrip("/")
    if not (host and user and dbname):
        raise ValueError(f"unparseable Neon connection URI: {connection_uri!r}")
    return {
        "NEON_HOST": host,
        "NEON_USER": user,
        "NEON_PASSWORD": parsed.password or "",
        "NEON_DBNAME": dbname,
    }


def run_dbt(connection_uri: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run ``dbt <args>`` against the branch; the caller asserts on the returncode."""
    return subprocess.run(
        ["dbt", *args, "--project-dir", "dbt", "--profiles-dir", "dbt"],
        env={**os.environ, **branch_env(connection_uri)},
        capture_output=True,
        text=True,
        check=False,
    )
