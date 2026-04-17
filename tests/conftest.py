import os

import pytest


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
