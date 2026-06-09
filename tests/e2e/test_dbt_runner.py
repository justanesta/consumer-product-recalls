from __future__ import annotations

import pytest

from tests.e2e._dbt_runner import branch_env


def test_branch_env_parses_full_uri() -> None:
    env = branch_env("postgresql://u:p@ep-cool-123.us-east-1.aws.neon.tech/neondb?sslmode=require")
    assert env == {
        "NEON_HOST": "ep-cool-123.us-east-1.aws.neon.tech",
        "NEON_USER": "u",
        "NEON_PASSWORD": "p",
        "NEON_DBNAME": "neondb",
    }


def test_branch_env_tolerates_missing_password() -> None:
    env = branch_env("postgres://u@host/db")
    assert env["NEON_PASSWORD"] == ""
    assert (env["NEON_HOST"], env["NEON_USER"], env["NEON_DBNAME"]) == ("host", "u", "db")


@pytest.mark.parametrize("bad", ["", "not-a-uri", "postgres://host/db", "postgres:///db"])
def test_branch_env_raises_on_unparseable(bad: str) -> None:
    with pytest.raises(ValueError):
        branch_env(bad)
