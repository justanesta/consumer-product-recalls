"""Offline drift-fence canary for the USCG manufacturer detail extractor.

Runs the **production** parser
(``UscgManufacturerDetailExtractor._parse_details_page`` — with its real
``TransientExtractionError`` drift fence and, unlike the exploratory probe, NO
``len(label) <= 40`` blind spot) over the detail-page HTML already cached by
``scripts/uscg/probe_mic_reassignment_rate.py`` under
``data/exploratory/uscg_manufacturers/detail_probe_cache``. Zero new fetches,
fully offline, runs in seconds.

This is the truest cheap pre-flight before the ~4.5h live
``recalls extract uscg_manufacturer_details`` run: the live extractor aborts the
ENTIRE run (nothing persisted) the moment one page carries a bold label not in
``_DETAIL_LABEL_MAP``. This canary tells you whether the pages you already have
would trip that fence — without fetching anything.

Outcomes (and exit codes):

- **clean (0):** every cached page parses; the fence will not trip on them.
- **trip (1):** a page carries an unknown bold label. The script prints the
  page id, URL, and the fence's own message (which names the offending label).
  Fix: add the label to ``_DETAIL_LABEL_MAP``
  (``src/extractors/uscg_manufacturer_detail.py``), the Pydantic schema
  (``src/schemas/uscg_manufacturer_detail.py``), and the probe's ``_LABEL_MAP``,
  then re-run.
- **inconclusive (2):** the cache dir is missing or empty — run the probe first.

COVERAGE CAVEAT: this only checks pages ALREADY in the cache (the recalled-MIC
pages from a prior ``--recalled-only`` probe run, plus any smoke ids). It fetches
nothing, so it says nothing about un-cached corners of the corpus — pair it with
a small fresh probe sample (``--sample-size 300``) for broader coverage.

Usage:

    python scripts/uscg/check_detail_drift_fence.py
    python scripts/uscg/check_detail_drift_fence.py --cache-dir <dir> --limit 50
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from src.extractors._base import TransientExtractionError
from src.extractors.uscg_manufacturer_detail import (
    _DETAIL_URL,
    UscgManufacturerDetailExtractor,
)

_DEFAULT_CACHE_DIR = Path("data/exploratory/uscg_manufacturers/detail_probe_cache")


def parse_with_production_fence(body: bytes, page_url: str) -> dict[str, object]:
    """Run the PRODUCTION parser + drift fence over one detail page's bytes.

    Raises ``TransientExtractionError`` exactly as the live extractor would: same
    ``_DETAIL_LABEL_MAP``, same raise on an unknown bold label, and (unlike the
    probe) no ``len(label) <= 40`` blind spot.

    ``_parse_details_page`` is, by design, a pure function of ``(body, page_url)``
    over the module-level ``_DETAIL_LABEL_MAP`` — it never touches ``self`` — so
    we invoke it UNBOUND. That keeps this canary fully offline: no DB engine, R2
    client, or ``Settings`` are constructed. If the method ever starts using
    ``self``, the parser tests in ``tests/scripts/`` fail here first.
    """
    return UscgManufacturerDetailExtractor._parse_details_page(
        cast("UscgManufacturerDetailExtractor", None), body, page_url
    )


@dataclass(frozen=True)
class PageCheck:
    """Result of running the production fence over one cached detail page."""

    page_id: str
    page_url: str
    ok: bool
    field_count: int  # recognized fields parsed (0 when the fence tripped)
    message: str | None  # the fence's TransientExtractionError text when tripped


@dataclass(frozen=True)
class CheckSummary:
    """Aggregate outcome over a batch of cached pages."""

    total: int
    ok: int
    tripped: list[PageCheck]

    @property
    def clean(self) -> bool:
        """True only when at least one page was checked and none tripped."""
        return self.total > 0 and not self.tripped


def check_page(page_id: str, body: bytes) -> PageCheck:
    """Run the production fence over one page; capture a trip instead of crashing."""
    page_url = f"{_DETAIL_URL}?id={page_id}"
    try:
        fields = parse_with_production_fence(body, page_url)
    except TransientExtractionError as exc:
        return PageCheck(
            page_id=page_id, page_url=page_url, ok=False, field_count=0, message=str(exc)
        )
    return PageCheck(
        page_id=page_id, page_url=page_url, ok=True, field_count=len(fields), message=None
    )


def run_check(pages: list[tuple[str, bytes]]) -> CheckSummary:
    """Run the production fence over already-read ``(page_id, body)`` pairs (pure)."""
    results = [check_page(page_id, body) for page_id, body in pages]
    tripped = [r for r in results if not r.ok]
    return CheckSummary(total=len(results), ok=sum(1 for r in results if r.ok), tripped=tripped)


def _page_sort_key(path: Path) -> tuple[int, int, str]:
    """Sort cache files by numeric id when the stem is an int, else lexicographically."""
    stem = path.stem
    if stem.isdigit():
        return (0, int(stem), stem)
    return (1, 0, stem)


def iter_cache_pages(cache_dir: Path, limit: int | None = None) -> list[tuple[str, bytes]]:
    """Read cached ``<id>.html`` files into ``(page_id, body)`` pairs (I/O boundary)."""
    paths = sorted(cache_dir.glob("*.html"), key=_page_sort_key)
    if limit is not None:
        paths = paths[:limit]
    return [(path.stem, path.read_bytes()) for path in paths]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cache-dir",
        default=str(_DEFAULT_CACHE_DIR),
        help=f"Dir of cached <id>.html detail pages (default {_DEFAULT_CACHE_DIR}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Check at most N pages (lowest ids first) for a quick spot-check.",
    )
    args = parser.parse_args(argv)

    cache_dir = Path(args.cache_dir)
    if not cache_dir.is_dir():
        print(f"[error] cache dir not found: {cache_dir}", file=sys.stderr)
        print(
            "Run scripts/uscg/probe_mic_reassignment_rate.py first to populate it.",
            file=sys.stderr,
        )
        return 2

    pages = iter_cache_pages(cache_dir, args.limit)
    if not pages:
        print(f"[error] no cached *.html pages in {cache_dir} — nothing to check.", file=sys.stderr)
        return 2

    summary = run_check(pages)

    print(f"Checked {summary.total} cached detail pages with the production drift fence.")
    print(f"  clean:   {summary.ok}")
    print(f"  tripped: {len(summary.tripped)}")
    for result in summary.tripped:
        print(f"\n  [TRIP] id={result.page_id}  {result.page_url}")
        print(f"         {result.message}")

    if summary.tripped:
        print(
            "\nThe production drift fence WOULD trip on the page(s) above. Add the offending "
            "label to _DETAIL_LABEL_MAP (src/extractors/uscg_manufacturer_detail.py), the "
            "Pydantic schema, and the probe's _LABEL_MAP, then re-run.",
            file=sys.stderr,
        )
        return 1

    print(
        "\nAll cached pages clean — the production fence will not trip on them. "
        "(Coverage is limited to cached ids; pair with a fresh probe sample for the rest.)"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
