"""HtmlScrapingExtractor — operation-type subclass of Extractor for scraped sources.

Replaces the stub previously declared in ``src/extractors/_base.py``. Provides
the shared scaffolding any HTML-scraping source needs:

- ``_fetch_page(url) -> (httpx.Response, bytes)``: single GET with
  sleep-before throttling (minimum-inter-request-interval contract), a
  per-fetch tenacity retry budget for transient failures, and status-code
  classification mirroring ``FlatFileExtractor._download_to_temp``
  (200 → success; 401/403 → AuthenticationError; 429 → RateLimitError
  with Retry-After honored; 5xx / network → TransientExtractionError).
  The retry budget lives at the per-fetch layer (3 attempts × short
  backoff) so a transient 503 on page 47 of a 71-page walk doesn't
  restart the entire walk — the outer ``Extractor.run()`` retry only
  catches persistent failures.

- ``_archive_page(url, response, body)``: stash one fetched page's bytes
  plus minimal forensic envelope (URL, fetched-at timestamp, status,
  body SHA-256) for batch upload in ``land_raw``. Subclasses call this
  for every page they fetch (listing + details).

- ``land_raw(raw_records) -> str``: concatenate all archived pages into
  a single NDJSON document, gzip-compress via ``R2LandingClient``, return
  the R2 object key. **Single-artifact-per-run** model (as opposed to
  one R2 object per fetched page): minimizes the number of small R2
  objects (~95k/year/source at 1,834 pages × weekly cadence becomes
  52/year), makes re-ingest a single-read operation, and matches the
  semantic unit "everything we fetched this run" cleanly. Per the
  approved plan, this is the canonical pattern for HTML scrapers.

- ``_capture_response_metadata(response, body)``: analog of
  ``RestApiExtractor._capture_response`` for the page-0 listing
  response. Concrete extractors call this exactly once per run, on the
  first response — the captured headers + body SHA flow into
  ``extraction_runs.response_*`` via ``_record_run``. USCG does not
  populate ``response_inner_content_sha256`` (no wrapper/inner
  distinction for HTML pages).

- ``_default_user_agent()``: identifies the project + provides operator
  contact (the project author's email) per Phase 5d Step 1 Finding N.
  Honest UA — does NOT impersonate Firefox / Chrome — since no observed
  bot management exists on the USCG host (Finding K). If a future
  scrape source has anti-bot machinery requiring browser-string UAs,
  override ``_headers()`` in the concrete subclass.

Concrete subclasses (USCG in Phase 5d Step 2) implement the 5 lifecycle
abstract methods on top of these helpers, plus two USCG-specific
parsers (``_parse_listing_page``, ``_parse_details_page``) declared
abstractly here so the base class signature documents the expected
shape.

Politeness model:
- ``scrape_delay_seconds`` (default 1.0s) enforces a minimum-inter-request
  interval via ``_throttle()`` — sleep-before-fetch, no sleep on the
  first call, ``max(0, last_request_ts + delay - now())`` semantics.
- Fresh ``httpx.Client`` per fetch — no cookie persistence (Finding M).
  USCG's PHP session cookie carries no useful state for an anonymous
  scraper and pinning to one session could trigger per-session rate
  limits.
- ``Retry-After`` honored on 429 via base-class ``RateLimitError`` +
  the outer ``_TRANSIENT_RETRY`` policy.
"""

from __future__ import annotations

import abc
import base64
import hashlib
import json
import time
from datetime import UTC, datetime
from importlib.metadata import version as _pkg_version
from typing import Any

import httpx
import tenacity
from pydantic import BaseModel, PrivateAttr

from src.extractors._base import (
    AuthenticationError,
    Extractor,
    RateLimitError,
    TransientExtractionError,
)

# Per-fetch retry budget: short, tight, in addition to the outer
# Extractor.run() retry. Three attempts × jitter-bounded exponential
# backoff (0.5s initial → 5s cap) keeps a transient 503 from tearing
# down the whole 1,834-fetch walk. The outer retry catches whatever
# slips through these three attempts.
_PER_FETCH_RETRY = tenacity.Retrying(
    retry=tenacity.retry_if_exception_type((TransientExtractionError, RateLimitError)),
    wait=tenacity.wait_exponential_jitter(initial=0.5, max=5),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)


class HtmlScrapingExtractor[T: BaseModel](Extractor[T]):
    """Base for extractors that scrape HTML pages (USCG and any future scrape source).

    Concrete subclasses set ``start_url`` to the entry point. The default
    timeout (30s) matches REST sources; HTML pages are typically small
    and fast. ``scrape_delay_seconds`` enforces polite throttling between
    fetches.

    The five ``Extractor`` lifecycle methods remain abstract — concrete
    subclasses implement them on top of ``_fetch_page``, ``_archive_page``,
    ``_capture_response_metadata``, and the abstract parsing helpers.
    """

    start_url: str
    timeout_seconds: float = 30.0
    scrape_delay_seconds: float = 1.0
    # Expected listing-page column headers for schema-drift detection
    # (Phase 5d Step 2 requirement: "Schema drift on HTML structure
    # changes raises ValidationError"). When set, concrete subclasses
    # assert observed headers match this list before parsing rows;
    # mismatch raises TransientExtractionError so the run aborts
    # cleanly rather than writing partial bronze rows from a
    # mis-mapped table. None = no drift fence (development mode).
    expected_columns: list[str] | None = None

    # Forensic state for extraction_runs (universal columns from
    # migrations 0010 + 0011). Mirror of RestApiExtractor's
    # ``_captured_response_*`` PrivateAttrs. ``response_inner_content_sha256``
    # is not applicable to HTML (no wrapper/inner distinction) — leave
    # NULL.
    _captured_response_status_code: int | None = PrivateAttr(default=None)
    _captured_response_etag: str | None = PrivateAttr(default=None)
    _captured_response_last_modified: str | None = PrivateAttr(default=None)
    _captured_response_body_sha256: str | None = PrivateAttr(default=None)
    _captured_response_headers: dict[str, str] | None = PrivateAttr(default=None)

    # Throttle state: monotonic timestamp of the last completed fetch.
    # Initialized to 0.0 so the first call elides the sleep (the
    # throttle math `max(0, 0 + delay - now()) == 0` because now() >> 0).
    _last_request_ts: float = PrivateAttr(default=0.0)

    # Accumulator for archived pages — one entry per ``_archive_page``
    # call. ``land_raw`` consumes this to produce the single NDJSON-per-run
    # R2 artifact. Subclasses must clear this if they need run isolation
    # within a single process (the default is fresh-per-instance, which
    # is the conventional usage).
    _archived_pages: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    # --- Throttling + HTTP fetch ---

    def _throttle(self) -> None:
        """Sleep just long enough that the next fetch is ≥``scrape_delay_seconds`` after the last.

        Minimum-inter-request-interval contract. No sleep on the first
        call (``_last_request_ts == 0.0`` so the math evaluates to 0).
        Politeness rule of the polite-scraper convention.
        """
        wait = self._last_request_ts + self.scrape_delay_seconds - time.monotonic()
        if wait > 0:
            time.sleep(wait)

    def _headers(self) -> dict[str, str]:
        """Default request headers — honest UA per Finding N.

        Format: ``consumer-product-recalls/<version> (contact: <email>)``.
        Override in subclasses if a host requires browser-impersonation
        UAs (none observed for USCG; USDA/FDA use ``_fsis_headers.py``
        for Akamai Bot Manager workarounds).
        """
        try:
            ver = _pkg_version("consumer-product-recalls")
        except Exception:  # noqa: BLE001 — best-effort UA construction
            ver = "0.0.0"
        return {
            "User-Agent": f"consumer-product-recalls/{ver} (contact: adriannesta@gmail.com)",
        }

    def _fetch_page(self, url: str) -> tuple[httpx.Response, bytes]:
        """Single GET with throttle, per-fetch retry, status-code classification.

        Sleep-before semantics; fresh ``httpx.Client`` per call so no
        cookies persist across fetches (Finding M).

        Status code routing mirrors ``FlatFileExtractor._download_to_temp``:
        200 → return; 429 → ``RateLimitError`` (Retry-After honored);
        401/403 → ``AuthenticationError`` (fail fast); 5xx / network →
        ``TransientExtractionError`` (retried by ``_PER_FETCH_RETRY``).

        Returns:
            (httpx.Response, body_bytes) — caller is responsible for
            archiving via ``_archive_page`` and (for the page-0 response)
            capturing metadata via ``_capture_response_metadata``.
        """
        return _PER_FETCH_RETRY(self._fetch_page_once, url)

    def _fetch_page_once(self, url: str) -> tuple[httpx.Response, bytes]:
        """Single attempt — separated so ``_PER_FETCH_RETRY`` can wrap it."""
        self._throttle()
        try:
            with httpx.Client(timeout=self.timeout_seconds, headers=self._headers()) as client:
                response = client.get(url)
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"HTML scrape network error: {exc}") from exc
        finally:
            # Update throttle state even on failed fetches so retry-induced
            # back-to-back calls still honor the politeness interval.
            self._last_request_ts = time.monotonic()

        if response.status_code == 200:
            return response, response.content
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(f"HTML scrape URL returned {response.status_code}: {url}")
        raise TransientExtractionError(f"HTML scrape URL returned {response.status_code}: {url}")

    # --- Archiving + forensic capture ---

    def _archive_page(self, url: str, response: httpx.Response, body: bytes) -> None:
        """Stash one fetched page's bytes + envelope for batch upload in ``land_raw``.

        Envelope shape (matches the NDJSON line format produced by
        ``land_raw``): URL, fetched-at ISO-8601 timestamp, response
        status, body SHA-256, and the body itself base64-encoded so the
        NDJSON document is fully self-contained and re-parseable
        without re-fetching.
        """
        self._archived_pages.append(
            {
                "url": url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "status": response.status_code,
                "html_sha256": hashlib.sha256(body).hexdigest(),
                "body_base64": base64.b64encode(body).decode("ascii"),
            }
        )

    def _capture_response_metadata(self, response: httpx.Response, body: bytes) -> None:
        """Stash forensic metadata for ``extraction_runs.response_*`` persistence.

        Convention: call exactly once per run, on the **first listing
        page** response. Subsequent fetches' headers are not captured
        (the per-page archive in ``_archive_page`` preserves the full
        body trail). USCG returns no useful ``Last-Modified`` or
        ``ETag`` (Finding K) so those columns persist as NULL even
        when the response is 200 — that's correct behavior per the
        forensic-column semantics.
        """
        self._captured_response_status_code = response.status_code
        self._captured_response_etag = response.headers.get("etag")
        self._captured_response_last_modified = response.headers.get("last-modified")
        self._captured_response_body_sha256 = hashlib.sha256(body).hexdigest()
        self._captured_response_headers = dict(response.headers)

    # --- land_raw template ---

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """Concatenate archived pages to NDJSON, gzip-upload to R2, return the path.

        Single artifact per run — see module docstring for the
        small-objects-per-run vs single-artifact tradeoff. The
        ``ndjson`` suffix is already supported in
        ``R2LandingClient._CONTENT_TYPE`` (``application/x-ndjson``).

        ``raw_records`` is unused — the archive is keyed off
        ``self._archived_pages`` which the subclass's ``extract()``
        populated as a side-effect of fetching. Required by the
        ``Extractor`` ABC signature.
        """
        if not self._archived_pages:
            raise TransientExtractionError(
                "HTML scrape produced zero archived pages — extract() likely failed "
                "to call _archive_page() for each fetch."
            )
        ndjson_bytes = b"\n".join(
            json.dumps(page, separators=(",", ":")).encode("utf-8") for page in self._archived_pages
        )
        path = self._r2_client.land(  # type: ignore[attr-defined]
            source=self.source_name,
            content=ndjson_bytes,
            suffix="ndjson",
        )
        return path

    # --- Abstract parsing helpers (subclass-implemented) ---

    @abc.abstractmethod
    def _parse_listing_page(self, body: bytes, page_url: str) -> list[dict[str, Any]]:
        """Parse a listing page's HTML; return one dict per row.

        Concrete subclasses raise ``TransientExtractionError`` on
        schema drift (e.g., table header mismatch with
        ``self.expected_columns``) so the run aborts cleanly rather
        than emitting mis-mapped rows. Empty list = end-of-pagination
        signal for the caller's walk loop.
        """

    @abc.abstractmethod
    def _parse_details_page(self, body: bytes, page_url: str) -> dict[str, Any]:
        """Parse a single details page's HTML; return the field dict.

        Concrete subclasses extract label-value pairs from the
        page's structured HTML. Empty values yield missing keys (or
        ``None`` values, subclass's choice) so downstream Pydantic
        validation can route Type-A drift (missing field) distinctly
        from Type-B (field present but wrongly typed).
        """
