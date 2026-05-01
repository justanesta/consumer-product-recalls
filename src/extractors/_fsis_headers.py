"""Shared HTTP header helpers for FSIS APIs.

Both the USDA recall API and the FSIS Establishment Listing API are fronted by
Akamai Bot Manager on `www.fsis.usda.gov`. A request without browser-like
headers triggers slow-loris-style throttling at the Akamai edge: TCP+TLS
complete, the GET is sent, and the server never responds (no body, no close,
no error). Empirically a real Firefox UA + matching Accept / Accept-Language /
Accept-Encoding passes Akamai's multi-signal bot scoring and returns 200 in
<500ms.

UA strings are vendored in `data/user_agents.json` and refreshed weekly by
`.github/workflows/refresh-user-agents.yml` (which fetches Mozilla
product-details and Chromium Dash, templates the UAs, and opens a PR if the
versions changed). The fallback UA below is used only if `data/user_agents.json`
is missing or malformed at runtime — keep it reasonably current.
"""

from __future__ import annotations

import json
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

_USER_AGENTS_PATH = Path(__file__).resolve().parents[2] / "data" / "user_agents.json"
_FALLBACK_FIREFOX_UA = "Mozilla/5.0 (X11; Linux x86_64; rv:150.0) Gecko/20100101 Firefox/150.0"


def _load_user_agent() -> str:
    """Return the current Firefox/Linux UA from data/user_agents.json.

    Falls back to `_FALLBACK_FIREFOX_UA` and emits `fsis.user_agents_load_failed`
    if the vendored file is missing, malformed, or missing the expected key. The
    fallback path is also why this function is callable per-fetch rather than
    cached at import time — a caller running outside the repo (e.g. an embedded
    test harness) that lacks `data/user_agents.json` should still get a working
    UA without import-time failure.
    """
    try:
        data = json.loads(_USER_AGENTS_PATH.read_text())
        ua = data["user_agents"]["firefox_linux"]
        if not isinstance(ua, str) or not ua:
            raise ValueError(f"firefox_linux UA empty or wrong type: {ua!r}")
        return ua
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError) as exc:
        logger.warning(
            "fsis.user_agents_load_failed",
            path=str(_USER_AGENTS_PATH),
            error=str(exc),
            fallback_ua=_FALLBACK_FIREFOX_UA,
        )
        return _FALLBACK_FIREFOX_UA


def browser_headers() -> dict[str, str]:
    """Build the browser-like default headers for httpx.Client (Finding O).

    UA is loaded fresh from `data/user_agents.json` on each call so a CI-merged
    UA refresh takes effect immediately on the next extraction run, with no
    process restart needed.
    """
    return {
        "User-Agent": _load_user_agent(),
        "Accept": "application/json,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }
