"""Refresh data/user_agents.json from official browser-vendor release feeds.

Sources (both first-party, free, no auth):
  - Mozilla product-details — https://product-details.mozilla.org/1.0/firefox_versions.json
  - Chromium Dash         — https://chromiumdash.appspot.com/fetch_releases

Run manually or via `.github/workflows/refresh-user-agents.yml` (weekly cron).
The CI workflow opens a PR if `data/user_agents.json` changed.

Why this exists: USDA FSIS sits behind Akamai Bot Manager, which slowloris-es
Python's default httpx User-Agent. Empirically, a real Firefox UA passes the
multi-signal bot scoring (Finding O in documentation/usda/recall_api_observations.md).
This script keeps the vendored UA data current automatically so the extractor
doesn't drift past Akamai's "looks like a real recent browser" threshold.

Output is deterministic (sorted keys, no timestamps): if neither feed has moved,
re-running produces a byte-identical file, so the CI workflow only opens a PR
when versions actually changed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

_FIREFOX_VERSIONS_URL = "https://product-details.mozilla.org/1.0/firefox_versions.json"
_CHROME_RELEASES_URL = (
    "https://chromiumdash.appspot.com/fetch_releases?platform=Linux&num=1&channel=Stable"
)
_OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "user_agents.json"
_REQUEST_TIMEOUT_SECONDS = 30.0


def fetch_firefox_version() -> str:
    """Return Mozilla's current LATEST_FIREFOX_VERSION (e.g. '150.0.1')."""
    response = httpx.get(_FIREFOX_VERSIONS_URL, timeout=_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    version = payload.get("LATEST_FIREFOX_VERSION")
    if not isinstance(version, str) or not version:
        raise RuntimeError(
            f"Mozilla product-details returned unexpected shape; "
            f"LATEST_FIREFOX_VERSION missing or not a string: {payload!r}"
        )
    return version


def fetch_chrome_version() -> str:
    """Return Chromium Dash's current Linux Stable version (e.g. '147.0.7727.137')."""
    response = httpx.get(_CHROME_RELEASES_URL, timeout=_REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"Chromium Dash returned unexpected shape: {payload!r}")
    version = payload[0].get("version")
    if not isinstance(version, str) or not version:
        raise RuntimeError(
            f"Chromium Dash returned no 'version' field on first release entry: {payload[0]!r}"
        )
    return version


def firefox_linux_ua(version: str) -> str:
    """Build a Firefox/Linux UA string from a full version like '150.0.1'.

    Firefox UA convention: rv: and Firefox/ both use the major version only.
    Mismatched rv/Firefox versions are themselves a bot signal; keep them aligned.
    """
    major = version.split(".", 1)[0]
    return f"Mozilla/5.0 (X11; Linux x86_64; rv:{major}.0) Gecko/20100101 Firefox/{major}.0"


def chrome_linux_ua(version: str) -> str:
    """Build a Chrome/Linux UA string from a full version like '147.0.7727.137'.

    Chrome UA convention: only the major version is exposed in the wire UA;
    minors are reduced to .0.0.0 to align with the User-Agent Client Hint
    reduction effort that real Chrome browsers ship.
    """
    major = version.split(".", 1)[0]
    return (
        f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{major}.0.0.0 Safari/537.36"
    )


def build_payload() -> dict[str, Any]:
    """Fetch both feeds and assemble the data/user_agents.json payload."""
    firefox_version = fetch_firefox_version()
    chrome_version = fetch_chrome_version()
    return {
        "sources": {
            "chrome": {
                "url": _CHROME_RELEASES_URL,
                "version": chrome_version,
                "version_key": "version",
            },
            "firefox": {
                "url": _FIREFOX_VERSIONS_URL,
                "version": firefox_version,
                "version_key": "LATEST_FIREFOX_VERSION",
            },
        },
        "user_agents": {
            "chrome_linux": chrome_linux_ua(chrome_version),
            "firefox_linux": firefox_linux_ua(firefox_version),
        },
    }


def write_payload(payload: dict[str, Any], path: Path = _OUTPUT_PATH) -> None:
    """Write payload as deterministic JSON (sorted keys, trailing newline)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main() -> int:
    payload = build_payload()
    write_payload(payload)
    print(f"Wrote {_OUTPUT_PATH}")
    print(f"  firefox: {payload['sources']['firefox']['version']}")
    print(f"  chrome:  {payload['sources']['chrome']['version']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
