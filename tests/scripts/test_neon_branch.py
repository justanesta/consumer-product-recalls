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
    _parse_connection_uri,
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


def test_parse_connection_uri_happy() -> None:
    payload = {
        "branch": {"id": "br-new"},
        "connection_uris": [{"connection_uri": "postgres://u:p@h/db"}],
    }
    assert _parse_connection_uri(payload) == ("br-new", "postgres://u:p@h/db")


def test_parse_connection_uri_uses_first_uri() -> None:
    payload = {
        "branch": {"id": "br-new"},
        "connection_uris": [
            {"connection_uri": "postgres://first"},
            {"connection_uri": "postgres://second"},
        ],
    }
    assert _parse_connection_uri(payload)[1] == "postgres://first"


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"branch": {"id": "br-new"}},  # no connection_uris
        {"connection_uris": [{"connection_uri": "x"}]},  # no branch
        {"branch": {"id": "x"}, "connection_uris": []},  # empty uris
        {"branch": {}, "connection_uris": [{"connection_uri": "x"}]},  # no branch id
    ],
)
def test_parse_connection_uri_raises_on_shape_drift(payload: dict) -> None:
    with pytest.raises(NeonBranchError):
        _parse_connection_uri(payload)


# --------------------------- I/O (respx-mocked) ---------------------------


@respx.mock
def test_create_branch_posts_and_returns_uri() -> None:
    route = respx.post(f"{_API}/projects/{_PROJECT}/branches").mock(
        return_value=httpx.Response(
            201,
            json={
                "branch": {"id": "br-xyz"},
                "connection_uris": [{"connection_uri": "postgres://u:p@ep-x/neondb"}],
            },
        )
    )
    assert create_branch(_KEY, _PROJECT) == ("br-xyz", "postgres://u:p@ep-x/neondb")
    assert route.called
    assert route.calls.last.request.headers["authorization"] == f"Bearer {_KEY}"


@respx.mock
def test_create_branch_raises_on_api_error() -> None:
    respx.post(f"{_API}/projects/{_PROJECT}/branches").mock(
        return_value=httpx.Response(403, text="forbidden")
    )
    with pytest.raises(NeonBranchError, match="403"):
        create_branch(_KEY, _PROJECT)


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
