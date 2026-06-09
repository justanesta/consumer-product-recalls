import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

# scripts/ is not on sys.path by default; add the repo root so conftest (and any test) can
# import helpers from scripts/ as regular modules — same shim as tests/scripts/*.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture(scope="session")
def vcr_config() -> dict[str, Any]:
    return {
        "record_mode": "none",
        "decode_compressed_response": True,
        "before_record_response": _scrub_response_headers,
    }


def _scrub_response_headers(response: dict[str, Any]) -> dict[str, Any]:
    _SENSITIVE = frozenset({"server", "x-powered-by", "cf-ray", "cf-cache-status", "set-cookie"})
    response["headers"] = {
        k: v for k, v in response["headers"].items() if k.lower() not in _SENSITIVE
    }
    return response


@pytest.fixture(scope="session")
def test_db_url() -> Iterator[str]:
    """Yield a throwaway test-database URL, torn down after the session (ADR 0015).

    Swappable via the TEST_DB_PROVIDER env var:
      - ``neon`` (default): provision an ephemeral Neon branch via the REST API and delete it
        on teardown. Needs ``NEON_API_KEY`` + ``NEON_PROJECT_ID`` in the environment (`.envrc`
        locally, repo secrets in CI); ``NEON_TEST_PARENT_BRANCH`` optionally overrides the parent
        (defaults to the project's default branch). Skips cleanly when the creds are absent so a
        plain ``pytest`` run without Neon access stays green.
      - ``local``: Phase-2 local Postgres (not yet implemented).
    """
    provider = os.getenv("TEST_DB_PROVIDER", "neon")
    if provider == "neon":
        from scripts.neon_branch import create_branch, delete_branch

        api_key = os.getenv("NEON_API_KEY")
        project_id = os.getenv("NEON_PROJECT_ID")
        if not api_key or not project_id:
            pytest.skip("NEON_API_KEY / NEON_PROJECT_ID not set — skipping Neon-branch tests")
        parent_id = os.getenv("NEON_TEST_PARENT_BRANCH") or None

        branch_id, connection_uri = create_branch(api_key, project_id, parent_id)
        try:
            yield connection_uri
        finally:
            delete_branch(api_key, project_id, branch_id)
    elif provider == "local":
        raise NotImplementedError("Local Postgres provisioning implemented in Phase 2")
    else:
        raise ValueError(f"Unknown TEST_DB_PROVIDER: {provider}")
