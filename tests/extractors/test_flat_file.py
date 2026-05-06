"""Unit tests for the FlatFileExtractor base helpers.

The base class is abstract (inherits Extractor's abstract methods); tests
use a minimal concrete subclass that no-ops the lifecycle methods so the
helpers can be exercised in isolation.
"""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from src.extractors._base import (
    AuthenticationError,
    QuarantineRecord,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors._flat_file import (
    FlatFileExtractor,
    FlatFileFieldCountError,
    inner_content_stream,
)

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "nhtsa"
_FIXTURE_ZIP = _FIXTURE_DIR / "sample_recalls.zip"
_DRIFT_TSV = _FIXTURE_DIR / "drift_30col.tsv"


class _DummyRecord(BaseModel):
    source_recall_id: str


class _DummyFlatFile(FlatFileExtractor[_DummyRecord]):
    """Minimal concrete subclass — abstract lifecycle methods are no-ops."""

    source_name: str = "dummy"
    file_url: str = "https://example.test/dummy.zip"

    def extract(self) -> list[dict[str, Any]]:  # pragma: no cover — no-op
        return []

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:  # pragma: no cover
        return ""

    def validate_records(  # pragma: no cover — no-op
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[_DummyRecord], list[QuarantineRecord]]:
        return [], []

    def check_invariants(  # pragma: no cover — no-op
        self, records: list[_DummyRecord]
    ) -> tuple[list[_DummyRecord], list[QuarantineRecord]]:
        return records, []

    def load_bronze(  # pragma: no cover — no-op
        self,
        records: list[_DummyRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        return 0


@pytest.fixture
def extractor() -> _DummyFlatFile:
    return _DummyFlatFile()


def _make_response(
    status_code: int,
    *,
    content: bytes = b"",
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    request = httpx.Request("GET", "https://example.test/dummy.zip")
    return httpx.Response(
        status_code,
        request=request,
        content=content,
        headers=headers or {},
    )


# ---------------------------------------------------------------------------
# FlatFileFieldCountError
# ---------------------------------------------------------------------------


class TestFlatFileFieldCountError:
    def test_attributes_preserved(self) -> None:
        err = FlatFileFieldCountError(row_index=42, expected=29, observed=30)
        assert err.row_index == 42
        assert err.expected == 29
        assert err.observed == 30

    def test_message_includes_drift_hint(self) -> None:
        err = FlatFileFieldCountError(row_index=0, expected=29, observed=30)
        assert "29" in str(err)
        assert "30" in str(err)
        assert "schema drift" in str(err)


# ---------------------------------------------------------------------------
# _download_to_temp
# ---------------------------------------------------------------------------


class TestDownloadToTemp:
    def test_200_writes_tempfile_and_returns_body(self, extractor: _DummyFlatFile) -> None:
        body = b"hello flat file"
        response = _make_response(200, content=body)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            path, returned_response, returned_body = extractor._download_to_temp(
                "https://example.test/dummy.zip"
            )
        try:
            assert path.exists()
            assert path.read_bytes() == body
            assert returned_response is response
            assert returned_body == body
        finally:
            path.unlink(missing_ok=True)

    def test_429_raises_rate_limit(self, extractor: _DummyFlatFile) -> None:
        response = _make_response(429, headers={"Retry-After": "30"})
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(RateLimitError) as exc_info:
                extractor._download_to_temp("https://example.test/dummy.zip")
        assert exc_info.value.retry_after == 30.0

    def test_401_raises_authentication_error(self, extractor: _DummyFlatFile) -> None:
        response = _make_response(401)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(AuthenticationError):
                extractor._download_to_temp("https://example.test/dummy.zip")

    def test_403_raises_authentication_error(self, extractor: _DummyFlatFile) -> None:
        # Same branch as 401 — both are auth failures per the source's tuple
        # check. Covered separately because the two status codes carry
        # different operator semantics (403 = present-but-forbidden, often a
        # bot-block; 401 = credential failure) and we want a regression to
        # surface if someone narrows the tuple to just one.
        response = _make_response(403)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(AuthenticationError):
                extractor._download_to_temp("https://example.test/dummy.zip")

    def test_404_raises_file_not_found(self, extractor: _DummyFlatFile) -> None:
        response = _make_response(404)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(FileNotFoundError):
                extractor._download_to_temp("https://example.test/dummy.zip")

    def test_503_raises_transient(self, extractor: _DummyFlatFile) -> None:
        response = _make_response(503)
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = response
            with pytest.raises(TransientExtractionError):
                extractor._download_to_temp("https://example.test/dummy.zip")

    def test_network_error_raises_transient(self, extractor: _DummyFlatFile) -> None:
        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.ConnectError(
                "nope"
            )
            with pytest.raises(TransientExtractionError):
                extractor._download_to_temp("https://example.test/dummy.zip")


# ---------------------------------------------------------------------------
# _decompress_zip
# ---------------------------------------------------------------------------


class TestDecompressZip:
    def test_happy_path_returns_inner_bytes(self, extractor: _DummyFlatFile) -> None:
        inner_bytes, inner_name = extractor._decompress_zip(_FIXTURE_ZIP, "*.txt")
        assert inner_name.endswith(".txt")
        assert inner_bytes
        # Smoke check — first row's RECORD_ID is 200001 per the fixture.
        first_line = inner_bytes.split(b"\r\n", 1)[0].decode("utf-8")
        assert first_line.startswith("200001\t")

    def test_no_match_raises_transient(self, extractor: _DummyFlatFile, tmp_path: Path) -> None:
        zip_path = tmp_path / "no_txt.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("readme.md", "not a txt")
        with pytest.raises(TransientExtractionError) as exc_info:
            extractor._decompress_zip(zip_path, "*.txt")
        assert "no entry matching" in str(exc_info.value)

    def test_multiple_matches_raises_transient(
        self, extractor: _DummyFlatFile, tmp_path: Path
    ) -> None:
        zip_path = tmp_path / "multi_txt.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("a.txt", "first")
            zf.writestr("b.txt", "second")
        with pytest.raises(TransientExtractionError) as exc_info:
            extractor._decompress_zip(zip_path, "*.txt")
        assert "multiple entries" in str(exc_info.value)


# ---------------------------------------------------------------------------
# _iter_tab_delimited
# ---------------------------------------------------------------------------


class TestIterTabDelimited:
    def test_yields_row_index_line_fields(self, extractor: _DummyFlatFile) -> None:
        content = b"a\tb\tc\r\nd\te\tf\r\n"
        rows = list(extractor._iter_tab_delimited(content))
        assert rows == [
            (0, "a\tb\tc", ["a", "b", "c"]),
            (1, "d\te\tf", ["d", "e", "f"]),
        ]

    def test_handles_lf_only_line_terminators(self, extractor: _DummyFlatFile) -> None:
        content = b"a\tb\nc\td\n"
        rows = list(extractor._iter_tab_delimited(content))
        assert len(rows) == 2

    def test_skips_empty_trailing_lines(self, extractor: _DummyFlatFile) -> None:
        content = b"a\tb\r\n\r\n\r\n"
        rows = list(extractor._iter_tab_delimited(content))
        assert rows == [(0, "a\tb", ["a", "b"])]

    def test_caller_validates_field_count(self, extractor: _DummyFlatFile) -> None:
        # Drift detection lives in the caller, not the iterator. Here we
        # confirm the iterator simply yields whatever it splits — with a
        # row whose field count is 30 (the drift fixture).
        content = _DRIFT_TSV.read_bytes()
        rows = list(extractor._iter_tab_delimited(content))
        assert len(rows) == 1
        _row_index, _line, fields = rows[0]
        assert len(fields) == 30  # caller now raises FlatFileFieldCountError

    def test_utf8_preserved(self, extractor: _DummyFlatFile) -> None:
        # Finding E confirmed UTF-8 — non-ASCII bytes survive round-trip.
        content = "café\tnoël\r\n".encode()
        rows = list(extractor._iter_tab_delimited(content))
        assert rows[0][2] == ["café", "noël"]


# ---------------------------------------------------------------------------
# _capture_flatfile_response
# ---------------------------------------------------------------------------


class TestCaptureFlatfileResponse:
    def test_populates_all_columns(self, extractor: _DummyFlatFile) -> None:
        wrapper = b"wrapper bytes here"
        inner = b"inner content bytes"
        response = _make_response(
            200,
            content=wrapper,
            headers={
                "etag": '"abc123"',
                "last-modified": "Mon, 04 May 2026 07:04:23 GMT",
                "content-type": "application/octet-stream",
                "x-amz-version-id": "VERSIONXYZ",
            },
        )

        extractor._capture_flatfile_response(response, wrapper, inner)

        assert extractor._captured_response_status_code == 200
        assert extractor._captured_response_etag == '"abc123"'
        assert extractor._captured_response_last_modified == "Mon, 04 May 2026 07:04:23 GMT"
        assert extractor._captured_response_body_sha256 == hashlib.sha256(wrapper).hexdigest()
        assert (
            extractor._captured_response_inner_content_sha256 == hashlib.sha256(inner).hexdigest()
        )
        # Full headers preserved for forensic queries — including
        # x-amz-version-id which Finding C designated an audit anchor.
        assert extractor._captured_response_headers is not None
        assert extractor._captured_response_headers.get("x-amz-version-id") == "VERSIONXYZ"

    def test_inner_bytes_optional(self, extractor: _DummyFlatFile) -> None:
        # Plain-text wrappers like RCL.txt have no inner archive — only the
        # wrapper hash gets populated.
        wrapper = b"plain text body"
        response = _make_response(200, content=wrapper)
        extractor._capture_flatfile_response(response, wrapper)
        assert extractor._captured_response_body_sha256 == hashlib.sha256(wrapper).hexdigest()
        assert extractor._captured_response_inner_content_sha256 is None

    def test_inner_hash_distinct_from_wrapper_hash(self, extractor: _DummyFlatFile) -> None:
        # Finding J: ZIP wrapper bytes and inner bytes are hashed
        # separately precisely because the wrapper is non-deterministic
        # while the inner content is the real change-detection oracle.
        wrapper = b"this is the zip wrapper"
        inner = b"this is the decompressed tsv"
        response = _make_response(200, content=wrapper)
        extractor._capture_flatfile_response(response, wrapper, inner)
        assert (
            extractor._captured_response_body_sha256
            != extractor._captured_response_inner_content_sha256
        )


# ---------------------------------------------------------------------------
# inner_content_stream — file-like adapter for future flat-file sources
# ---------------------------------------------------------------------------


class TestInnerContentStream:
    def test_returns_seekable_bytesio(self) -> None:
        # The adapter exists for callers (e.g., csv.reader) that want a
        # file-like view of the inner bytes. Verify it round-trips and is
        # seekable — both behaviors a streaming consumer relies on.
        content = b"row1\trow1b\r\nrow2\trow2b\r\n"
        stream = inner_content_stream(content)
        assert stream.read() == content
        stream.seek(0)
        assert stream.read(4) == b"row1"


# ---------------------------------------------------------------------------
# _iter_tab_delimited — property-based roundtrip
# ---------------------------------------------------------------------------
#
# The iterator is a simple parser; example-based tests above pin its
# happy paths and edge cases (CRLF/LF, empty trailing lines, drift width,
# UTF-8). These properties target the broader invariant: for any
# tab-delimited body we hand it, what comes back must equal what we put
# in (modulo the documented "skip empty lines" rule). A property test is
# the natural fit — the parser's contract is universal across inputs,
# not tied to specific examples.

# Cells exclude only the structural delimiters of the parser: tab
# (separates fields) and the LF / CR pair (separate rows). Surrogate
# code points (``Cs``) are valid Python ``str`` but invalid UTF-8 source
# bytes — the test's ``body.encode('utf-8')`` step would trip on them
# before the iterator ever runs. NUL is excluded as a defensive choice
# (decode surprises in the wild).
_FIELD_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters="\t\n\r\x00",
    ),
    min_size=0,
    max_size=20,
)
_ROW = st.lists(_FIELD_TEXT, min_size=1, max_size=10)
_ROWS = st.lists(_ROW, min_size=0, max_size=15)


@given(rows=_ROWS)
@settings(max_examples=50, deadline=None)
def test_iter_tab_delimited_roundtrips_arbitrary_tsv(rows: list[list[str]]) -> None:
    """For any well-formed TSV body, the iterator yields back rows whose
    fields, re-joined with ``\\t``, equal the original line; and whose
    count equals the number of non-empty input lines.

    Hypothesis explores the input space — empty rows, single-cell rows,
    rows of empty strings, mixed widths — surfacing edge cases that
    example-based tests would have to enumerate by hand.
    """
    extractor = _DummyFlatFile()
    body_lines = ["\t".join(fields) for fields in rows]
    body = ("\n".join(body_lines)).encode("utf-8")

    out = list(extractor._iter_tab_delimited(body))

    # The iterator skips empty lines (per its own contract — "Empty
    # trailing lines (common in Windows-generated TSVs) are skipped
    # silently"). Mirror that here when computing expectations.
    expected_lines = [line for line in body_lines if line]
    assert len(out) == len(expected_lines)

    # Each yielded tuple's `line` and `fields` are consistent with each
    # other and with the original input.
    for (_row_index, line, fields), expected_line in zip(out, expected_lines, strict=True):
        assert line == expected_line
        assert "\t".join(fields) == expected_line
