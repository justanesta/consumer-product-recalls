"""Forensic inspector for USCG R2 landing-data NDJSON archives.

A USCG ``recalls deep-rescan`` (or routine ``extract``) lands one
NDJSON.gz file per run at ``uscg/<date>/<uuid>.ndjson.gz``. Each line
of the NDJSON is one fetched HTML page with envelope ``{url,
fetched_at, status, html_sha256, body_base64}``. This script pulls that
artifact from R2 and lets the operator inspect what USCG actually
served us at fetch time — used to confirm "yes, this is exactly what
USCG served us, and our parser/invariant logic mis-classified it" vs
"our parser garbled the input".

Phase 5d Step 3 usage pattern (the 2026-05-17 first-extraction surfaced
218 year-prefix invariant failures and 33 ``company_name=NULL``
Pydantic failures; we suspect the invariant is structurally wrong, but
want to byte-confirm by reading the raw HTML for a few specific
recalls before tearing out the invariant):

    # Print a single details page's raw HTML, for a recall whose
    # source_recall_id year prefix didn't match its opened_on year.
    python scripts/uscg/inspect_landing_ndjson.py \\
        --raw-landing-path uscg/2026-05-17/abc.ndjson.gz \\
        --recall-number 23MF0066

    # Same, but only print HTML around a specific labeled field.
    python scripts/uscg/inspect_landing_ndjson.py \\
        --raw-landing-path uscg/2026-05-17/abc.ndjson.gz \\
        --recall-number 23MF0066 \\
        --show-field "Case Open Date"

    # Print a listing page's raw HTML to confirm pagination behavior.
    python scripts/uscg/inspect_landing_ndjson.py \\
        --raw-landing-path uscg/2026-05-17/abc.ndjson.gz \\
        --listing-page 0

    # Print a one-line manifest of all archived pages — URL + status + SHA.
    python scripts/uscg/inspect_landing_ndjson.py \\
        --raw-landing-path uscg/2026-05-17/abc.ndjson.gz \\
        --manifest

Mirrors NHTSA's ``inspect_archive_row.py`` precedent in spirit:
forensic, ad-hoc, no DB dependency. Reads R2 bytes through the
production ``boto3`` settings, so the same env vars that make
``recalls extract`` work also make this script work.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import sys
from typing import Any

import boto3

from src.config.settings import Settings


def _open_r2_object(settings: Settings, key: str) -> Any:
    """Return a streamable file-like for ``key`` from the configured R2 bucket."""
    r2 = boto3.client(
        "s3",
        endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=settings.r2_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.r2_secret_access_key.get_secret_value(),
    )
    obj = r2.get_object(Bucket=settings.r2_bucket_name, Key=key)
    return obj["Body"]


def _iter_pages(stream: Any):
    """Yield one parsed NDJSON record (dict) per archived page."""
    with gzip.open(stream, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _decode_html(page: dict[str, Any]) -> str:
    """base64 → UTF-8 HTML body, with replacement for non-UTF-8 bytes.

    USCG's pages declare ``charset=UTF-8`` in headers but occasionally
    contain stray Latin-1 / Windows-1252 bytes (e.g., ``0xbc`` = ``¼``,
    common from Word copy-paste). The production extractor uses
    ``BeautifulSoup(body, "lxml")`` which auto-detects encoding; here
    we use ``errors="replace"`` so a single bad byte doesn't abort the
    whole inspection. Replacement characters render as ``�`` and signal
    where the source bytes were non-UTF-8.
    """
    return base64.b64decode(page["body_base64"]).decode("utf-8", errors="replace")


def _print_field_context(html: str, label: str, context_lines: int = 4) -> None:
    """Print HTML lines that mention ``label``, with ±N surrounding lines.

    Cheap grep-with-context — enough to see the label and its sibling
    value cell in the USCG details-page label/value-pair pattern.
    """
    lines = html.split("\n")
    matches = [i for i, ln in enumerate(lines) if label in ln]
    if not matches:
        print(f"[no lines matched {label!r}]", file=sys.stderr)
        return
    for match_idx in matches:
        start = max(0, match_idx - context_lines)
        end = min(len(lines), match_idx + context_lines + 1)
        print(f"--- lines {start}..{end - 1} (match at {match_idx}) ---")
        for i in range(start, end):
            marker = ">>" if i == match_idx else "  "
            print(f"{marker} {i:5d}  {lines[i]}")
        print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect a USCG R2 landing-data NDJSON archive.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--raw-landing-path",
        required=True,
        help="R2 object key (e.g., uscg/2026-05-17/abc.ndjson.gz). "
        "Find this in extraction_runs.raw_landing_path or in bronze rows.",
    )
    filter_group = parser.add_mutually_exclusive_group()
    filter_group.add_argument(
        "--recall-number",
        help="Filter to the details page for this recall id (matches 'id=<number>').",
    )
    filter_group.add_argument(
        "--listing-page",
        type=int,
        help="Filter to listing page N (matches 'pageNum_allRecalls=N').",
    )
    filter_group.add_argument(
        "--manifest",
        action="store_true",
        help="Print one-line summary per archived page (url + status + html_sha256).",
    )
    filter_group.add_argument(
        "--scan-listings-for",
        metavar="RECALL_ID",
        help="Walk every archived LISTING page (URL contains pageNum_allRecalls=) "
        "and print the row(s) containing this recall id with surrounding HTML "
        "context. Use when bronze records a value that didn't come from the "
        "details page — confirms what USCG actually rendered in the listing "
        "column for this recall.",
    )
    parser.add_argument(
        "--show-field",
        help="When printing HTML, only show ±context lines around this label "
        "(e.g., 'Case Open Date', 'Number', 'Disposition').",
    )
    parser.add_argument(
        "--context-lines",
        type=int,
        default=4,
        help="Lines of context above/below each --show-field match (default 4).",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    stream = _open_r2_object(settings, args.raw_landing_path)

    matched = 0
    total = 0
    for page in _iter_pages(stream):
        total += 1
        url = page["url"]

        if args.manifest:
            print(f"{page['status']:>3}  {page['html_sha256'][:12]}  {page['fetched_at']}  {url}")
            matched += 1
            continue

        if args.scan_listings_for:
            # Listing-page filter (URL signature). Skip details pages.
            if "pageNum_allRecalls=" not in url:
                continue
            html = _decode_html(page)
            if args.scan_listings_for not in html:
                continue
            print(
                f"=== {url} (status={page['status']}, sha={page['html_sha256'][:16]}, "
                f"fetched_at={page['fetched_at']}) ==="
            )
            # Wider context default — a USCG listing-row <tr> block spans
            # ~8 lines (6 <td>s + open/close tags), and the anchor with the
            # recall id is the row's first cell, so ±8 lines reliably
            # captures the whole row including the Opened On column.
            _print_field_context(html, args.scan_listings_for, max(args.context_lines, 8))
            matched += 1
            continue

        if args.recall_number:
            if f"id={args.recall_number}" not in url:
                continue
        elif args.listing_page is not None:
            if f"pageNum_allRecalls={args.listing_page}" not in url:
                continue
        else:
            # No filter chosen and no manifest — bail.
            print(
                "Error: choose one of --recall-number / --listing-page / "
                "--manifest / --scan-listings-for",
                file=sys.stderr,
            )
            return 2

        # Matched: print HTML, optionally filtered to field context.
        html = _decode_html(page)
        print(
            f"=== {url} (status={page['status']}, sha={page['html_sha256'][:16]}, "
            f"fetched_at={page['fetched_at']}) ==="
        )
        if args.show_field:
            _print_field_context(html, args.show_field, args.context_lines)
        else:
            print(html)
        matched += 1

    if not args.manifest and matched == 0:
        print(
            f"No matching page found in {total} archived pages. "
            "Check --recall-number / --listing-page spelling.",
            file=sys.stderr,
        )
        return 1
    if args.manifest:
        print(f"--- {matched} pages total ---", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
