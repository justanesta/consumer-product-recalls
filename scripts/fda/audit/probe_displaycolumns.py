"""Probe FDA's bulk POST /recalls/ endpoint with arbitrary displaycolumns.

For the capture-expansion side of the audit: verify proposed-add fields
populate in real FDA responses before committing to a schema migration.

Does NOT land to R2, does NOT load to bronze, does NOT update the watermark.
Pure exploratory tool. By default the response is saved to
``data/exploratory/fda/probes/probe_<UTC-timestamp>.json`` so a re-run that
just wants different aggregation doesn't burn another API call.

Auth via FDA_AUTHORIZATION_USER + FDA_AUTHORIZATION_KEY from Settings.

## HTTP stack

Mirrors production's ``src/extractors/fda.py``: httpx + Mozilla UA + auth
headers only — no TLS impersonation, no Origin/Referer, no cookie persistence.
The production stack is what FDA's Akamai edge expects and runs cleanly from
both GitHub Actions and residential ISP IPs.

## Validation rules at the FDA edge

Two distinct rules to be aware of when designing probes:

  • **Finding O — sort field must be in displaycolumns.** A POST whose ``sort``
    column is absent from ``displaycolumns`` returns HTTP 204 No Content (Akamai
    silent block at the edge, never reaches the FDA app for a STATUSCODE 406).
    This probe enforces the rule at argument-parse time via ``--sort``
    pre-flight to avoid burning probe budget on malformed payloads.

  • **Finding K0 — only 33 columns are valid for the bulk POST datagroup.**
    Per ``iRES_enforcement_reports_api_usage_documentation.pdf`` page 7. Fields
    outside the list (``productlmd``, ``pressreleaseurl``, all ``*short`` and
    ``*indicator`` variants, ``createdt``) return STATUSCODE 406 — those are
    lookup-endpoint columns only. See ``documentation/fda/api_observations.md``
    Finding K0 / K0.1 for the cost-benefit framing on lookup-endpoint
    enrichment.

## Usage

```
# Verify a capture-expansion candidate from the bulk POST datagroup
python scripts/fda/audit/probe_displaycolumns.py \\
    --columns "productid,recalleventid,firmlegalnam,codeinformation,eventlmd" \\
    --eventlmdfrom 05/01/2026 --rows 100 --print-samples 2

# Throwaway probe — do not save the response
python scripts/fda/audit/probe_displaycolumns.py \\
    --columns "productid,recalleventid,firmcitynam,eventlmd" \\
    --eventlmdfrom 05/01/2026 --no-save
```
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from _lib import DEFAULT_CACHE_DIR, summarize_records  # noqa: E402

from src.config.settings import Settings  # noqa: E402

_FDA_BASE_URL = "https://www.accessdata.fda.gov/rest/iresapi"
_RECALLS_ENDPOINT = "/recalls/"

# Exact UA value from src/extractors/fda.py:136 — FDA's own iRES API Python
# sample code uses this string. Sending the default `python-httpx/X.Y.Z` value
# is suspected to trigger FDA's anti-abuse throttle on the very first request
# per Finding N in api_observations.md.
_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

_DEFAULT_PROBE_DIR = DEFAULT_CACHE_DIR / "probes"


def _save_response(records: list[dict[str, Any]], probe_dir: Path, columns: str) -> Path:
    probe_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = probe_dir / f"probe_{ts}.json"
    path.write_text(
        json.dumps(
            {"columns": columns, "fetched_at": ts, "records": records},
            indent=2,
        )
    )
    return path


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
        "--sort",
        default="eventlmd",
        help=(
            "Sort column (default: eventlmd). Per Finding O the sort column "
            "MUST appear in --columns or FDA returns HTTP 204 No Content."
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
    args = parser.parse_args()

    # Finding O pre-flight: sort column must be in displaycolumns or FDA
    # returns HTTP 204. Catch the misconfiguration at parse time so probe
    # budget is not burned on a malformed payload.
    requested_columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    if args.sort not in requested_columns:
        print(
            f"ERROR: --sort '{args.sort}' is not in --columns. "
            "Per Finding O the sort column MUST appear in displaycolumns or "
            "FDA's edge returns HTTP 204 No Content (Akamai silent block). "
            f"Add '{args.sort}' to --columns, or change --sort to a column you "
            "already requested.",
            file=sys.stderr,
        )
        return 2

    settings = Settings()  # type: ignore[call-arg]
    if settings.fda_authorization_user is None or settings.fda_authorization_key is None:
        print(
            "ERROR: FDA_AUTHORIZATION_USER and FDA_AUTHORIZATION_KEY must be set.",
            file=sys.stderr,
        )
        return 2

    filters = [f"{{'eventlmdfrom':'{args.eventlmdfrom}'}}"]
    if args.eventlmdto:
        filters.append(f"{{'eventlmdto':'{args.eventlmdto}'}}")
    filter_str = f"[{','.join(filters)}]"

    payload = {
        "displaycolumns": args.columns,
        "filter": filter_str,
        "start": 1,
        "rows": args.rows,
        "sort": args.sort,
        "sortorder": "desc",
    }
    url = f"{_FDA_BASE_URL}{_RECALLS_ENDPOINT}?signature={int(time.time())}"

    print(f"# Probe: columns={args.columns}", file=sys.stderr)
    print(f"# Probe: filter={filter_str}, rows={args.rows}, sort={args.sort}", file=sys.stderr)

    # Mirror production src/extractors/fda.py:320-329 exactly.
    try:
        with httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        ) as client:
            response = client.post(
                url,
                data={"payLoad": json.dumps(payload)},
                headers={
                    "Authorization-User": settings.fda_authorization_user.get_secret_value(),
                    "Authorization-Key": settings.fda_authorization_key.get_secret_value(),
                },
            )
    except httpx.TransportError as exc:
        print(f"ERROR: network failure: {exc}", file=sys.stderr)
        return 2

    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type:
        print(
            f"ERROR: FDA anti-abuse throttle detected "
            f"(HTTP {response.status_code}, HTML response in place of JSON). "
            "This is the production-extractor 302→/apology_objects/ redirect pattern. "
            "Wait at least 30 minutes before retrying.",
            file=sys.stderr,
        )
        return 2
    if response.status_code == 204:
        print(
            "ERROR: HTTP 204 No Content — Akamai edge silent block. Most common "
            "cause is Finding O (sort column not in displaycolumns) but the "
            "pre-flight check should have caught that. Remaining candidates: "
            "(a) per-IP rate limit burned by recent prior runs, (b) per-auth-key "
            "budget exhausted, (c) IP-class scoring deteriorated today. Wait "
            "an hour, then retry; if still 204, escalate to FDA OII for a "
            "whitelist exception.",
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
    if status == 406:
        # Finding K0: requested column is not in the 33-column bulk POST
        # datagroup. Most likely a lookup-endpoint-only field.
        print(
            f"ERROR: FDA STATUSCODE 406: {body.get('MESSAGE')}. "
            "Per Finding K0, only 33 columns are valid for the bulk POST "
            "datagroup (see api_observations.md). Lookup-endpoint-only fields "
            "(productlmd, pressreleaseurl, pressreleaseissuedt, pressreleasetype, "
            "all *short and *indicator variants, createdt) cannot be requested "
            "here — use the corresponding /recalls/event/{id}, "
            "/recalls/product/{id}, or /search/pressreleaseurls/ endpoint instead.",
            file=sys.stderr,
        )
        return 2
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
