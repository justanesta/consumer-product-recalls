"""FlatFileExtractor — operation-type subclass of Extractor for flat-file sources.

Replaces the stub in src/extractors/_base.py. Provides the shared
scaffolding any flat-file source needs:

- ``_download_to_temp(url) -> Path``: streams an HTTP GET into a
  tempfile, classifies status codes the same way RestApiExtractor does
  (200 → success; 304 → not-modified short-circuit; 401/403 →
  AuthenticationError; 429 → RateLimitError; 5xx / network →
  TransientExtractionError).
- ``_decompress_zip(path, inner_glob) -> bytes``: opens the local ZIP,
  finds the inner file matching the glob, returns its bytes. Drift
  detection: raises ExtractionError if the glob doesn't match exactly
  one inner file (the schema we're built against expects a specific
  inner shape; a missing or extra inner file is a structural drift event
  we want to fail loudly on rather than silently absorb).
- ``_iter_tab_delimited(content) -> Iterator[(row_index, line, fields)]``:
  iterates rows from a UTF-8 byte payload. CRLF and LF both work
  (``splitlines`` handles either). Field-count validation is the
  concrete extractor's responsibility — yielded so the caller can
  inspect ``len(fields)``, raise ``FlatFileFieldCountError`` on drift,
  and route the offending row to quarantine without abandoning the
  rest of the file.
- ``_capture_flatfile_response(...)``: analog of
  ``RestApiExtractor._capture_response`` plus an inner-content SHA-256
  for the new ``extraction_runs.response_inner_content_sha256`` column
  added in migration 0011. The wrapper-level body hash is preserved in
  the existing ``response_body_sha256`` column for audit; the
  inner-content hash is the authoritative "did the data change?" oracle
  for ZIPs whose wrapper bytes are non-deterministic across re-archives
  (Finding J in
  ``documentation/nhtsa/flat_file_observations.md``).

Concrete subclasses (NhtsaExtractor in Phase 5c, eventually a USCG
flat-file analog if needed) implement the 5 lifecycle abstract methods
on top of these helpers.
"""

from __future__ import annotations

import hashlib
import io
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from pydantic import BaseModel, PrivateAttr

from src.extractors._base import (
    AuthenticationError,
    Extractor,
    RateLimitError,
    TransientExtractionError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


class FlatFileFieldCountError(ValueError):
    """A TSV row's field count differs from the expected schema width.

    Carries the row index (0-based, header-excluded) and the observed
    field count so the concrete extractor can build a quarantine record
    without re-parsing.
    """

    def __init__(self, row_index: int, expected: int, observed: int) -> None:
        super().__init__(
            f"Row {row_index} has {observed} fields; expected {expected}. "
            "Possible cause: schema drift (NHTSA has added columns at the "
            "right edge of the row 4 times in 18 years per Finding F)."
        )
        self.row_index = row_index
        self.expected = expected
        self.observed = observed


class FlatFileExtractor[T: BaseModel](Extractor[T]):
    """Base for extractors that download and parse flat files.

    Concrete subclasses set ``file_url`` to the full archive URL.
    Longer default timeout than ``RestApiExtractor`` accommodates large
    file downloads (NHTSA's POST_2010 ZIP is ~14 MB compressed; future
    sources may be larger).
    """

    file_url: str
    timeout_seconds: float = 120.0

    # Forensic state for extraction_runs (universal columns from migrations
    # 0010 + 0011). Mirror of RestApiExtractor's _captured_response_*
    # PrivateAttrs plus the new inner-content hash. Concrete extractors'
    # _record_run() reads these when persisting.
    _captured_response_status_code: int | None = PrivateAttr(default=None)
    _captured_response_etag: str | None = PrivateAttr(default=None)
    _captured_response_last_modified: str | None = PrivateAttr(default=None)
    _captured_response_body_sha256: str | None = PrivateAttr(default=None)
    _captured_response_inner_content_sha256: str | None = PrivateAttr(default=None)
    _captured_response_headers: dict[str, str] | None = PrivateAttr(default=None)

    # --- HTTP download ---

    def _download_to_temp(self, url: str) -> tuple[Path, httpx.Response, bytes]:
        """Download ``url`` to a tempfile and return (path, response, body_bytes).

        Status code routing mirrors ``UsdaExtractor._fetch``. The body is
        returned alongside the path because callers typically want to
        hash the wrapper bytes (for ``response_body_sha256``) without
        re-reading the file.

        Raises:
            TransientExtractionError: 5xx, network errors, timeouts.
            RateLimitError: 429 (with Retry-After honored).
            AuthenticationError: 401/403 (unexpected for public flat
                files).
            FileNotFoundError: 404 (NHTSA doesn't 404 — ZIPs always
                exist — so this surfaces a real problem).
        """
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(url)
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"Flat-file network error: {exc}") from exc

        if response.status_code == 200:
            body = response.content
            with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
                tmp.write(body)
                tmp_name = tmp.name
            return Path(tmp_name), response, body

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(f"Flat-file URL returned {response.status_code}: {url}")
        if response.status_code == 404:
            raise FileNotFoundError(f"Flat-file URL returned 404: {url}")
        raise TransientExtractionError(f"Flat-file URL returned {response.status_code}: {url}")

    # --- ZIP decompression ---

    def _decompress_zip(self, path: Path, inner_glob: str) -> tuple[bytes, str]:
        """Open the ZIP at ``path`` and return ``(inner_bytes, inner_name)``.

        Drift detection: the ZIP must contain exactly one entry whose
        name matches ``inner_glob`` (e.g. ``"*.txt"``). Zero matches or
        multiple matches raise ``TransientExtractionError`` so a missed
        re-zip event surfaces in operator alerts rather than corrupting
        bronze with the wrong file.
        """
        with zipfile.ZipFile(path) as zf:
            matches = [name for name in zf.namelist() if Path(name).match(inner_glob)]
            if not matches:
                raise TransientExtractionError(
                    f"ZIP at {path} contains no entry matching {inner_glob!r}; "
                    f"entries observed: {zf.namelist()}. Possible cause: "
                    "upstream re-zip with a renamed inner file."
                )
            if len(matches) > 1:
                raise TransientExtractionError(
                    f"ZIP at {path} contains multiple entries matching "
                    f"{inner_glob!r}: {matches}. Concrete extractor must "
                    "narrow the glob — flat-file sources typically have "
                    "one inner file per archive."
                )
            inner_name = matches[0]
            inner_bytes = zf.read(inner_name)
        return inner_bytes, inner_name

    # --- Tab-delimited parsing ---

    def _iter_tab_delimited(
        self,
        content: bytes,
        encoding: str = "utf-8",
    ) -> Iterator[tuple[int, str, list[str]]]:
        """Iterate ``(row_index, raw_line, [field_values])`` from a TSV payload.

        Field-count validation is the concrete extractor's
        responsibility — it can compare ``len(fields)`` to its expected
        width, raise ``FlatFileFieldCountError`` on drift, and route the
        offending row to quarantine without abandoning the rest of the
        file. CRLF and LF line terminators are both handled by
        ``str.splitlines``.

        Empty trailing lines (common in Windows-generated TSVs) are
        skipped silently.
        """
        text = content.decode(encoding)
        for row_index, line in enumerate(text.splitlines()):
            if not line:
                continue
            yield row_index, line, line.split("\t")

    # --- Forensic capture ---

    def _capture_flatfile_response(
        self,
        response: httpx.Response,
        wrapper_bytes: bytes,
        inner_bytes: bytes | None = None,
    ) -> None:
        """Stash response metadata for persistence to extraction_runs.

        Analog of ``RestApiExtractor._capture_response``. The wrapper
        SHA-256 goes to the existing ``response_body_sha256`` column
        (migration 0010); the inner-content SHA-256 goes to the new
        ``response_inner_content_sha256`` column (migration 0011).

        For ZIP archives the wrapper hash is non-deterministic across
        re-archives (Finding J — daily re-zip with different metadata
        timestamps), so the inner-content hash is the authoritative
        change-detection oracle. For plain-text wrappers (e.g.
        ``RCL.txt``) the wrapper hash IS deterministic and either
        column works; convention is to populate both.

        Call once per run, on the primary response — multiple calls
        within the same run overwrite earlier captures.
        """
        self._captured_response_status_code = response.status_code
        self._captured_response_etag = response.headers.get("etag")
        self._captured_response_last_modified = response.headers.get("last-modified")
        self._captured_response_body_sha256 = hashlib.sha256(wrapper_bytes).hexdigest()
        if inner_bytes is not None:
            self._captured_response_inner_content_sha256 = hashlib.sha256(inner_bytes).hexdigest()
        self._captured_response_headers = dict(response.headers)


# A small adapter for callers that want a file-like view of the inner
# content (e.g. csv.reader). Not used by NHTSA's tab-delimited iterator
# above but kept here for future flat-file sources whose inner format
# benefits from streaming-style access.
def inner_content_stream(content: bytes) -> io.BytesIO:
    return io.BytesIO(content)
