from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

# scripts/ is not on sys.path by default; add the repo root so we can import
# the script as a regular module for testing.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.refresh_user_agents import (  # noqa: E402  — sys.path mutated above
    build_payload,
    chrome_linux_ua,
    fetch_chrome_version,
    fetch_firefox_version,
    firefox_linux_ua,
    write_payload,
)

# ---------------------------------------------------------------------------
# UA templating — pure functions, no I/O
# ---------------------------------------------------------------------------


class TestFirefoxLinuxUa:
    def test_uses_major_version_only(self) -> None:
        assert (
            firefox_linux_ua("150.0.1")
            == "Mozilla/5.0 (X11; Linux x86_64; rv:150.0) Gecko/20100101 Firefox/150.0"
        )

    def test_rv_and_firefox_versions_match(self) -> None:
        # Mismatched rv/Firefox versions are themselves a bot signal.
        ua = firefox_linux_ua("135.0.2")
        assert "rv:135.0" in ua
        assert "Firefox/135.0" in ua

    def test_handles_single_segment_version(self) -> None:
        # Defensive: shouldn't crash if Mozilla ever publishes a single-segment version.
        ua = firefox_linux_ua("150")
        assert "rv:150.0" in ua


class TestChromeLinuxUa:
    def test_uses_major_version_only(self) -> None:
        ua = chrome_linux_ua("147.0.7727.137")
        assert "Chrome/147.0.0.0" in ua
        assert "AppleWebKit/537.36" in ua
        assert "Safari/537.36" in ua

    def test_minor_versions_are_zeroed(self) -> None:
        # Real Chrome reduces minor versions in its UA per the User-Agent
        # Client Hint reduction effort; preserve that posture.
        ua = chrome_linux_ua("120.5.6099.123")
        assert "Chrome/120.0.0.0" in ua
        assert "Chrome/120.5.6099.123" not in ua


# ---------------------------------------------------------------------------
# Fetchers — mocked HTTP
# ---------------------------------------------------------------------------


def _mock_get(payload: object, status_code: int = 200) -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = payload
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=response
        )
    return response


class TestFetchFirefoxVersion:
    def test_returns_latest_version(self) -> None:
        with patch("httpx.get", return_value=_mock_get({"LATEST_FIREFOX_VERSION": "150.0.1"})):
            assert fetch_firefox_version() == "150.0.1"

    def test_raises_on_missing_key(self) -> None:
        with (
            patch("httpx.get", return_value=_mock_get({"OTHER_KEY": "irrelevant"})),
            pytest.raises(RuntimeError, match="LATEST_FIREFOX_VERSION"),
        ):
            fetch_firefox_version()

    def test_raises_on_non_string_version(self) -> None:
        with (
            patch("httpx.get", return_value=_mock_get({"LATEST_FIREFOX_VERSION": 150})),
            pytest.raises(RuntimeError, match="LATEST_FIREFOX_VERSION"),
        ):
            fetch_firefox_version()


class TestFetchChromeVersion:
    def test_returns_first_version_from_array(self) -> None:
        with patch(
            "httpx.get",
            return_value=_mock_get([{"version": "147.0.7727.137", "channel": "Stable"}]),
        ):
            assert fetch_chrome_version() == "147.0.7727.137"

    def test_raises_on_empty_array(self) -> None:
        with (
            patch("httpx.get", return_value=_mock_get([])),
            pytest.raises(RuntimeError, match="unexpected shape"),
        ):
            fetch_chrome_version()

    def test_raises_on_non_array_response(self) -> None:
        with (
            patch("httpx.get", return_value=_mock_get({"not": "an array"})),
            pytest.raises(RuntimeError, match="unexpected shape"),
        ):
            fetch_chrome_version()

    def test_raises_when_version_missing_on_first_entry(self) -> None:
        with (
            patch("httpx.get", return_value=_mock_get([{"channel": "Stable"}])),
            pytest.raises(RuntimeError, match="version"),
        ):
            fetch_chrome_version()


# ---------------------------------------------------------------------------
# Payload assembly + write — end-to-end determinism
# ---------------------------------------------------------------------------


class TestBuildPayload:
    def test_produces_expected_shape(self) -> None:
        with (
            patch("scripts.refresh_user_agents.fetch_firefox_version", return_value="150.0.1"),
            patch(
                "scripts.refresh_user_agents.fetch_chrome_version",
                return_value="147.0.7727.137",
            ),
        ):
            payload = build_payload()

        assert payload["sources"]["firefox"]["version"] == "150.0.1"
        assert payload["sources"]["chrome"]["version"] == "147.0.7727.137"
        assert "Firefox/150.0" in payload["user_agents"]["firefox_linux"]
        assert "Chrome/147.0.0.0" in payload["user_agents"]["chrome_linux"]


class TestWritePayload:
    def test_writes_deterministic_json(self, tmp_path: Path) -> None:
        # Same input → byte-identical output (sorted keys, trailing newline).
        path = tmp_path / "out.json"
        payload = {"b": 2, "a": 1, "user_agents": {"z": "z", "a": "a"}}
        write_payload(payload, path)
        first = path.read_bytes()
        write_payload(payload, path)
        second = path.read_bytes()
        assert first == second

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        nested = tmp_path / "nested" / "missing" / "out.json"
        write_payload({"a": 1}, nested)
        assert nested.exists()

    def test_round_trips_via_json_load(self, tmp_path: Path) -> None:
        path = tmp_path / "out.json"
        payload = {"sources": {"firefox": {"version": "150.0.1"}}}
        write_payload(payload, path)
        assert json.loads(path.read_text()) == payload
