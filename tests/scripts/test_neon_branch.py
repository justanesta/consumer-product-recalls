from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest
import respx

# scripts/ is not on sys.path by default; add the repo root so we can import the helper.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.neon_branch import (  # noqa: E402  — sys.path mutated above
    NeonBranchError,
    _create_payload,
    _parse_create_response,
    create_branch,
    delete_branch,
)

_API = "https://console.neon.tech/api/v2"
_PROJECT = "proj-123"
_KEY = "key-abc"


# --------------------------- pure shaping (no I/O) ---------------------------


def test_create_payload_default_branches_from_default() -> None:
    # No parent → no "branch" key → Neon uses the project's default branch.
    assert _create_payload(None) == {"endpoints": [{"type": "read_write"}]}


def test_create_payload_with_explicit_parent() -> None:
    body = _create_payload("br-parent")
    assert body["branch"] == {"parent_id": "br-parent"}
    assert body["endpoints"] == [{"type": "read_write"}]


def test_parse_create_response_happy() -> None:
    payload = {
        "branch": {"id": "br-new"},
        "databases": [{"name": "neondb", "owner_name": "neondb_owner"}],
    }
    assert _parse_create_response(payload) == ("br-new", "neondb", "neondb_owner")


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"branch": {"id": "br-new"}},  # no databases
        {"databases": [{"name": "d", "owner_name": "o"}]},  # no branch
        {"branch": {"id": "x"}, "databases": []},  # empty databases
        {"branch": {}, "databases": [{"name": "d", "owner_name": "o"}]},  # no branch id
        {"branch": {"id": "x"}, "databases": [{"name": "d"}]},  # no owner_name
    ],
)
def test_parse_create_response_raises_on_shape_drift(payload: dict) -> None:
    with pytest.raises(NeonBranchError):
        _parse_create_response(payload)


# --------------------------- I/O (respx-mocked) ---------------------------


@respx.mock
def test_create_branch_creates_then_fetches_uri() -> None:
    respx.post(f"{_API}/projects/{_PROJECT}/branches").mock(
        return_value=httpx.Response(
            201,
            json={
                "branch": {"id": "br-xyz"},
                "databases": [{"name": "neondb", "owner_name": "neondb_owner"}],
            },
        )
    )
    uri_route = respx.get(f"{_API}/projects/{_PROJECT}/connection_uri").mock(
        return_value=httpx.Response(200, json={"uri": "postgres://neondb_owner:pw@ep-x/neondb"})
    )
    assert create_branch(_KEY, _PROJECT) == ("br-xyz", "postgres://neondb_owner:pw@ep-x/neondb")
    req = uri_route.calls.last.request  # the URI fetch targets the branch + db + owner role
    assert req.url.params["branch_id"] == "br-xyz"
    assert req.url.params["role_name"] == "neondb_owner"
    assert req.headers["authorization"] == f"Bearer {_KEY}"


@respx.mock
def test_create_branch_raises_on_create_error() -> None:
    respx.post(f"{_API}/projects/{_PROJECT}/branches").mock(
        return_value=httpx.Response(403, text="forbidden")
    )
    with pytest.raises(NeonBranchError, match="403"):
        create_branch(_KEY, _PROJECT)


@respx.mock
def test_create_branch_deletes_branch_when_uri_fetch_fails() -> None:
    respx.post(f"{_API}/projects/{_PROJECT}/branches").mock(
        return_value=httpx.Response(
            201,
            json={"branch": {"id": "br-xyz"}, "databases": [{"name": "neondb", "owner_name": "o"}]},
        )
    )
    respx.get(f"{_API}/projects/{_PROJECT}/connection_uri").mock(
        return_value=httpx.Response(500, text="boom")
    )
    delete_route = respx.delete(f"{_API}/projects/{_PROJECT}/branches/br-xyz").mock(
        return_value=httpx.Response(200, json={})
    )
    with pytest.raises(NeonBranchError, match="500"):
        create_branch(_KEY, _PROJECT)
    assert delete_route.called  # cleanup ran — no leaked branch


@respx.mock
def test_delete_branch_ok() -> None:
    route = respx.delete(f"{_API}/projects/{_PROJECT}/branches/br-xyz").mock(
        return_value=httpx.Response(200, json={})
    )
    delete_branch(_KEY, _PROJECT, "br-xyz")
    assert route.called


@respx.mock
def test_delete_branch_tolerates_404() -> None:
    respx.delete(f"{_API}/projects/{_PROJECT}/branches/br-gone").mock(
        return_value=httpx.Response(404, text="not found")
    )
    delete_branch(_KEY, _PROJECT, "br-gone")  # must not raise


@respx.mock
def test_delete_branch_raises_on_server_error() -> None:
    respx.delete(f"{_API}/projects/{_PROJECT}/branches/br-x").mock(
        return_value=httpx.Response(500, text="boom")
    )
    with pytest.raises(NeonBranchError, match="500"):
        delete_branch(_KEY, _PROJECT, "br-x")
