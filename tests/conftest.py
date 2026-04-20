import os
from typing import Any

import pytest


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
def test_db_url() -> str:
    """Provide a test database URL.

    Swappable via TEST_DB_PROVIDER env var. Full implementation in Phase 7
    (Neon branch provisioning) and Phase 2 (local Postgres).
    """
    provider = os.getenv("TEST_DB_PROVIDER", "neon")
    if provider == "neon":
        raise NotImplementedError("Neon branch provisioning implemented in Phase 7")
    elif provider == "local":
        raise NotImplementedError("Local Postgres provisioning implemented in Phase 2")
    raise ValueError(f"Unknown TEST_DB_PROVIDER: {provider}")
