"""Provision ephemeral Neon branches for the integration / e2e test suite (ADR 0015).

`tests/conftest.py::test_db_url` calls `create_branch` at pytest-session start and
`delete_branch` on teardown, so tests run against an isolated copy-on-write clone of the
database and never touch production. It also doubles as an ad-hoc operator tool (spin up a
scratch branch by hand). Transport is the Neon REST API v2; auth is a project-scoped API key
(`NEON_API_KEY`) — see `documentation/development.md`.

Request/response *shaping* is split from the httpx I/O (`_create_payload`, `_parse_create_response`)
so it is unit-testable without live calls — see `tests/scripts/test_neon_branch.py` (respx-mocked).
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import tenacity

_API_BASE = "https://console.neon.tech/api/v2"
_TIMEOUT_S = 30.0

# Retry only transient transport blips (DNS/connect/read errors); a 4xx/5xx is a real
# response and surfaces immediately as NeonBranchError rather than being retried.
_retry_transport = tenacity.retry(
    retry=tenacity.retry_if_exception_type(httpx.TransportError),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)


class NeonBranchError(RuntimeError):
    """A Neon branch create/delete call returned an error or an unexpected shape."""


def _headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def _create_payload(parent_id: str | None) -> dict[str, object]:
    """Body for POST /branches: a read_write endpoint + an optional explicit parent.

    Omitting `parent_id` lets Neon branch from the project's default branch (the common case).
    """
    body: dict[str, object] = {"endpoints": [{"type": "read_write"}]}
    if parent_id:
        body["branch"] = {"parent_id": parent_id}
    return body


def _parse_create_response(payload: dict[str, Any]) -> tuple[str, str, str]:
    """Extract `(branch_id, database_name, owner_role)` from a create-branch response.

    The create response carries `connection_uris` ONLY when the project has a single role (Neon
    can't pick one otherwise), so we fetch the URI separately for the database's owner role.
    """
    try:
        branch_id = str(payload["branch"]["id"])
        db = payload["databases"][0]
        return branch_id, str(db["name"]), str(db["owner_name"])
    except (KeyError, IndexError, TypeError) as exc:
        raise NeonBranchError(f"unexpected create-branch response shape: {payload!r}") from exc


@_retry_transport
def _post_create_branch(api_key: str, project_id: str, parent_id: str | None) -> dict[str, Any]:
    resp = httpx.post(
        f"{_API_BASE}/projects/{project_id}/branches",
        headers=_headers(api_key),
        json=_create_payload(parent_id),
        timeout=_TIMEOUT_S,
    )
    if resp.status_code >= 400:
        raise NeonBranchError(f"create branch failed [{resp.status_code}]: {resp.text}")
    return resp.json()


@_retry_transport
def _fetch_connection_uri(api_key: str, project_id: str, branch_id: str, db: str, role: str) -> str:
    """Neon's password-bearing connection URI for `role` on `branch_id` (the create response omits
    it when the project has >1 role). The branch compute autostarts on first connect."""
    resp = httpx.get(
        f"{_API_BASE}/projects/{project_id}/connection_uri",
        headers=_headers(api_key),
        params={"branch_id": branch_id, "database_name": db, "role_name": role},
        timeout=_TIMEOUT_S,
    )
    if resp.status_code >= 400:
        raise NeonBranchError(f"get connection_uri failed [{resp.status_code}]: {resp.text}")
    try:
        return str(resp.json()["uri"])
    except (KeyError, TypeError) as exc:
        raise NeonBranchError(f"unexpected connection_uri response: {resp.text}") from exc


def create_branch(api_key: str, project_id: str, parent_id: str | None = None) -> tuple[str, str]:
    """Create an ephemeral Neon branch; return `(branch_id, connection_uri)` for the database's
    owner role (dbt needs owner-level DDL on the throwaway branch)."""
    branch_id, db_name, role_name = _parse_create_response(
        _post_create_branch(api_key, project_id, parent_id)
    )
    try:
        uri = _fetch_connection_uri(api_key, project_id, branch_id, db_name, role_name)
    except Exception:
        delete_branch(api_key, project_id, branch_id)  # don't leak a half-provisioned branch
        raise
    return branch_id, uri


@_retry_transport
def delete_branch(api_key: str, project_id: str, branch_id: str) -> None:
    """Delete a Neon branch. A 404 (already gone) is tolerated so teardown is idempotent."""
    resp = httpx.delete(
        f"{_API_BASE}/projects/{project_id}/branches/{branch_id}",
        headers=_headers(api_key),
        timeout=_TIMEOUT_S,
    )
    if resp.status_code == 404:
        return
    if resp.status_code >= 400:
        raise NeonBranchError(f"delete branch failed [{resp.status_code}]: {resp.text}")


if __name__ == "__main__":  # pragma: no cover — live operator smoke (create + delete one branch)
    _key = os.environ["NEON_API_KEY"]
    _project = os.environ["NEON_PROJECT_ID"]
    _parent = os.getenv("NEON_TEST_PARENT_BRANCH") or None
    _branch_id, _uri = create_branch(_key, _project, _parent)
    # Never print _uri — it carries the branch's credentials.
    print(f"created branch {_branch_id} (connection_uri received: {bool(_uri)})")
    delete_branch(_key, _project, _branch_id)
    print(f"deleted branch {_branch_id} — smoke OK")
