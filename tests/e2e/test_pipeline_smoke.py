from __future__ import annotations

from tests.e2e._dbt_runner import run_dbt


def test_dbt_build_smoke_on_branch(test_db_url: str) -> None:
    """E2E smoke (C24): a fresh ephemeral Neon branch builds a model cleanly — proving the C23
    branch-provisioning + the dbt-against-branch wiring work end to end. Skips without NEON_API_KEY
    (the test_db_url fixture), so the normal `pytest` run is unaffected; runs in CI's e2e step."""
    result = run_dbt(test_db_url, "build", "--select", "stg_cpsc_recalls")
    assert result.returncode == 0, (
        f"dbt build failed on the branch:\n{result.stdout}\n{result.stderr}"
    )
