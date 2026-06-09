"""Provision ephemeral Neon branches for the integration / e2e test suite (ADR 0015).

`tests/conftest.py::test_db_url` calls `create_branch` at pytest-session start and
`delete_branch` on teardown, so tests run against an isolated copy-on-write clone of the
database and never touch production. It also doubles as an ad-hoc operator tool (spin up a
scratch branch by hand). Transport is the Neon REST API v2; auth is a project-scoped API key
(`NEON_API_KEY`) — see `documentation/development.md`.

Request/response *shaping* is split from the httpx I/O (`_create_payload`, `_parse_connection_uri`)
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


def _parse_connection_uri(payload: dict[str, Any]) -> tuple[str, str]:
    """Extract `(branch_id, connection_uri)` from a create-branch response.

    Neon returns the new branch under `branch` and one or more `connection_uris`; we use the
    first. A missing/empty field means the API shape drifted — fail loud rather than yield a
    half-built URL.
    """
    try:
        branch_id = str(payload["branch"]["id"])
        uri = str(payload["connection_uris"][0]["connection_uri"])
    except (KeyError, IndexError, TypeError) as exc:
        raise NeonBranchError(f"unexpected create-branch response shape: {payload!r}") from exc
    return branch_id, uri


@_retry_transport
def create_branch(api_key: str, project_id: str, parent_id: str | None = None) -> tuple[str, str]:
    """Create an ephemeral Neon branch; return `(branch_id, connection_uri)`."""
    resp = httpx.post(
        f"{_API_BASE}/projects/{project_id}/branches",
        headers=_headers(api_key),
        json=_create_payload(parent_id),
        timeout=_TIMEOUT_S,
    )
    if resp.status_code >= 400:
        raise NeonBranchError(f"create branch failed [{resp.status_code}]: {resp.text}")
    return _parse_connection_uri(resp.json())


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
