"""Probe FDA's bulk POST endpoint with arbitrary displaycolumns.

For the (b) capture-expansion side of the audit: verify proposed-add fields
(e.g., ``codeinformation``, ``pressreleaseurl``, ``productdescriptionshort``)
actually populate in real FDA responses before committing to a schema migration.

Does NOT land to R2, does NOT load to bronze, does NOT update the watermark.
Pure exploratory tool. By default the response is saved to
``data/exploratory/fda/probes/probe_<UTC-timestamp>.json`` so a re-run that
just wants different aggregation doesn't burn another API call.

Auth via FDA_AUTHORIZATION_USER + FDA_AUTHORIZATION_KEY from Settings.

**Anti-bot-detection stack (2026-05-28).** Production's ``src/extractors/fda.py``
runs from GitHub Actions IP space at ~1 request/day and stays under Akamai Bot
Manager's threshold with a simple httpx + UA-override stack. Running probes from
a residential ISP IP trips Akamai's bot-fingerprinting at much lower request
rates (well under Finding N's 33 req/min) because the python-httpx TLS
fingerprint, sparse headers, and lack of cookie persistence across invocations
all read as "automated client." This script therefore uses ``curl_cffi`` to
impersonate a real Chrome TLS handshake + HTTP/2 settings + header order, plus
a disk-backed cookie jar at ``data/exploratory/fda/akamai_cookies.json`` so
Akamai's ``_abck`` / ``bm_sz`` session cookies persist across probe invocations
(the persistence makes us look like a returning browser rather than a forgetful
client). This addresses three of four bot signals; the residual is IP-class
scoring of residential vs datacenter, which Python can't fix.

Usage:

    # Verify high-priority capture-expansion candidates
    python scripts/fda/audit/probe_displaycolumns.py \\
        --columns "productid,recalleventid,firmlegalnam,codeinformation" \\
        --eventlmdfrom 04/01/2026

    # Wider date range, larger sample
    python scripts/fda/audit/probe_displaycolumns.py \\
        --columns "productid,recalleventid,firmcitynam,firmstatecd" \\
        --eventlmdfrom 04/01/2026 --eventlmdto 04/30/2026 \\
        --rows 1000 --print-samples 5

    # Throwaway probe — do not save the response
    python scripts/fda/audit/probe_displaycolumns.py \\
        --columns "codeinformation" --eventlmdfrom 05/01/2026 --no-save

    # Force-clear the persisted Akamai cookies (e.g., session has gone stale
    # and Akamai is silent-204'ing every request)
    python scripts/fda/audit/probe_displaycolumns.py \\
        --columns "productid" --eventlmdfrom 05/20/2026 --clear-cookies
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from curl_cffi import requests as cffi_requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import DEFAULT_CACHE_DIR, summarize_records  # noqa: E402

from src.config.settings import Settings  # noqa: E402

_FDA_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_RECALLS_ENDPOINT = "/recalls/"

# curl_cffi impersonation target — sends a Chrome-class TLS ClientHello (JA3/JA4),
# HTTP/2 SETTINGS frames, browser-typical header set, and consistent header order.
# Akamai Bot Manager fingerprints all three of those. We pick a recent Chrome
# version supported by curl_cffi (chrome131 is broadly available in current
# releases; bump as curl_cffi adds newer impersonation profiles).
_IMPERSONATE = "chrome131"

# Akamai bot-manager session cookies persist across probe invocations so we look
# like a returning browser rather than a forgetful client. Gitignored — under
# data/exploratory/ per .gitignore:64. Treat as sensitive (contains an
# Akamai session token).
_COOKIE_JAR_PATH = DEFAULT_CACHE_DIR / "akamai_cookies.json"

_DEFAULT_PROBE_DIR = DEFAULT_CACHE_DIR / "probes"


def _save_response(records: list[dict[str, Any]], probe_dir: Path, columns: str) -> Path:
    probe_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = probe_dir / f"probe_{ts}.json"
    path.write_text(
        json.dumps({"columns": columns, "fetched_at": ts, "records": records}, indent=2)
    )
    return path


def _load_cookies() -> list[dict[str, str]]:
    """Load persisted Akamai cookies from disk; tolerate corruption / absence."""
    if not _COOKIE_JAR_PATH.exists():
        return []
    try:
        data = json.loads(_COOKIE_JAR_PATH.read_text())
        if isinstance(data, list):
            return [c for c in data if isinstance(c, dict) and "name" in c and "value" in c]
        return []
    except (json.JSONDecodeError, OSError):
        return []


def _save_cookies(session: cffi_requests.Session) -> int:
    """Persist Akamai-domain cookies from ``session`` to disk. Returns count saved."""
    cookies_to_save: list[dict[str, str]] = []
    try:
        for cookie in session.cookies.jar:
            domain = cookie.domain or ""
            if "fda.gov" not in domain:
                continue
            cookies_to_save.append(
                {
                    "name": cookie.name,
                    "value": cookie.value or "",
                    "domain": domain,
                    "path": cookie.path or "/",
                }
            )
    except Exception as exc:  # noqa: BLE001 — best-effort persistence
        print(f"# Warning: could not enumerate session cookies: {exc}", file=sys.stderr)
        return 0
    try:
        _COOKIE_JAR_PATH.parent.mkdir(parents=True, exist_ok=True)
        _COOKIE_JAR_PATH.write_text(json.dumps(cookies_to_save, indent=2))
    except OSError as exc:
        print(f"# Warning: failed to persist Akamai cookies: {exc}", file=sys.stderr)
        return 0
    return len(cookies_to_save)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--columns",
        required=True,
        help="Comma-separated displaycolumns (lowercase FDA field names).",
    )
    default_from = (datetime.now(UTC).date() - timedelta(days=30)).strftime("%m/%d/%Y")
    parser.add_argument(
        "--eventlmdfrom",
        default=default_from,
        help=f"Start date MM/DD/YYYY (default: 30 days ago = {default_from}).",
    )
    parser.add_argument(
        "--eventlmdto",
        default=None,
        help="End date MM/DD/YYYY (default: no upper bound).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=500,
        help=(
            "Page size (default: 500; FDA caps at 5000 normally, or 2500 if "
            "codeinformation is requested)."
        ),
    )
    parser.add_argument(
        "--print-samples",
        type=int,
        default=3,
        help="N records to print verbatim before the per-field summary (default: 3).",
    )
    parser.add_argument(
        "--probe-dir",
        type=Path,
        default=_DEFAULT_PROBE_DIR,
        help=f"Where to save the response (default: {_DEFAULT_PROBE_DIR}). Gitignored.",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Throwaway probe — do not write the response to disk.",
    )
    parser.add_argument(
        "--clear-cookies",
        action="store_true",
        help=(
            "Delete the persisted Akamai cookie jar before this probe runs. "
            "Use when the prior session has been silent-blocked and you want a "
            "fresh handshake with a clean cookie state."
        ),
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    if settings.fda_authorization_user is None or settings.fda_authorization_key is None:
        print(
            "ERROR: FDA_AUTHORIZATION_USER and FDA_AUTHORIZATION_KEY must be set.",
            file=sys.stderr,
        )
        return 2

    if args.clear_cookies and _COOKIE_JAR_PATH.exists():
        _COOKIE_JAR_PATH.unlink()
        print(f"# Cleared {_COOKIE_JAR_PATH}", file=sys.stderr)

    filters = [f"{{'eventlmdfrom':'{args.eventlmdfrom}'}}"]
    if args.eventlmdto:
        filters.append(f"{{'eventlmdto':'{args.eventlmdto}'}}")
    filter_str = f"[{','.join(filters)}]"

    payload = {
        "displaycolumns": args.columns,
        "filter": filter_str,
        "start": 1,
        "rows": args.rows,
        "sort": "eventlmd",
        "sortorder": "desc",
    }
    url = f"{_FDA_BASE_URL}{_RECALLS_ENDPOINT}?signature={int(time.time())}"

    print(f"# Probe: columns={args.columns}", file=sys.stderr)
    print(f"# Probe: filter={filter_str}, rows={args.rows}", file=sys.stderr)
    print(f"# Probe: impersonating {_IMPERSONATE}", file=sys.stderr)

    session = cffi_requests.Session()

    # Pre-populate session with any persisted Akamai cookies — sending them back
    # makes Akamai see a continued browser session rather than a fresh-process
    # "forgetful client" pattern.
    persisted = _load_cookies()
    for c in persisted:
        try:
            session.cookies.set(c["name"], c["value"], domain=c["domain"], path=c["path"])
        except Exception as exc:  # noqa: BLE001
            print(
                f"# Warning: could not apply persisted cookie {c.get('name', '?')}: {exc}",
                file=sys.stderr,
            )
    if persisted:
        print(
            f"# Loaded {len(persisted)} Akamai cookie(s) from {_COOKIE_JAR_PATH}",
            file=sys.stderr,
        )
    else:
        print("# No prior Akamai cookies — fresh browser session", file=sys.stderr)

    try:
        response = session.post(
            url,
            data={"payLoad": json.dumps(payload)},
            headers={
                # Auth headers — curl_cffi merges these with the impersonate defaults.
                "Authorization-User": settings.fda_authorization_user.get_secret_value(),
                "Authorization-Key": settings.fda_authorization_key.get_secret_value(),
                # Browser-typical for cross-origin XHR; Akamai checks Origin /
                # Referer against the page that loads the iRES dashboard.
                "Origin": "https://www.accessdata.fda.gov",
                "Referer": "https://www.accessdata.fda.gov/scripts/ires/apidocs/",
            },
            impersonate=_IMPERSONATE,
            timeout=60,
        )
    except Exception as exc:  # noqa: BLE001 — surface any curl_cffi failure as an error
        print(f"ERROR: network failure: {exc}", file=sys.stderr)
        return 2

    # Persist whatever Akamai cookies the response set, so the next probe
    # invocation continues the same session.
    saved_count = _save_cookies(session)
    if saved_count:
        print(
            f"# Persisted {saved_count} cookie(s) to {_COOKIE_JAR_PATH}",
            file=sys.stderr,
        )

    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type:
        print(
            f"ERROR: FDA anti-abuse throttle detected "
            f"(HTTP {response.status_code}, HTML response in place of JSON). "
            "Wait at least 30 minutes before retrying. "
            "If persistent, --clear-cookies and wait an hour, or run from CI.",
            file=sys.stderr,
        )
        return 2
    if response.status_code == 204:
        print(
            "ERROR: HTTP 204 No Content — Akamai bot-manager silent block. "
            "curl_cffi impersonation + cookie persistence is the strongest "
            "Python-level fix; if this still 204s the residual blocker is "
            "IP-class scoring (residential ISP) — wait an hour, then try "
            "--clear-cookies, or move probing to CI.",
            file=sys.stderr,
        )
        print("  Response headers:", file=sys.stderr)
        for k, v in response.headers.items():
            print(f"    {k}: {v}", file=sys.stderr)
        return 2
    if response.status_code != 200:
        print(f"ERROR: HTTP {response.status_code}", file=sys.stderr)
        print("  Response headers:", file=sys.stderr)
        for k, v in response.headers.items():
            print(f"    {k}: {v}", file=sys.stderr)
        body_preview = response.text[:1000] if response.text else "(empty body)"
        print(f"  Body preview: {body_preview}", file=sys.stderr)
        return 2

    body = response.json()
    status = body.get("STATUSCODE")
    if status == 412:
        print(
            "# FDA STATUSCODE 412 — empty window. Try a wider date range.",
            file=sys.stderr,
        )
        return 0
    if status != 400:
        print(
            f"ERROR: FDA STATUSCODE {status}: {body.get('MESSAGE')}",
            file=sys.stderr,
        )
        return 2

    records: list[dict[str, Any]] = body.get("RESULT", [])
    print(f"# Probe returned {len(records)} record(s).", file=sys.stderr)

    if not args.no_save:
        saved = _save_response(records, args.probe_dir, args.columns)
        print(f"# Saved response to: {saved}", file=sys.stderr)

    if args.print_samples > 0 and records:
        print(f"=== First {args.print_samples} record(s) verbatim ===")
        for i, record in enumerate(records[: args.print_samples], start=1):
            print(f"--- record {i} ---")
            print(json.dumps(record, indent=2, default=str))
            print()
        print()

    print(summarize_records(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())
