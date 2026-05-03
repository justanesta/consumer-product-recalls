"""Unit tests for the HTTP response-capture path on RestApiExtractor.

Covers the `_capture_response()` helper and PrivateAttr fields added in
migration 0010 to support the ETag-viability study at
scripts/sql/_pipeline/etag_viability.sql.
"""

from __future__ import annotations

import hashlib
from typing import Any

import httpx

from src.extractors._base import (
    QuarantineRecord,
    RestApiExtractor,
)

# --- Test scaffolding ---


class _StubRecord:
    """Stand-in for the generic record type T: BaseModel.

    RestApiExtractor[T] is generic over a BaseModel subclass; for these tests
    the type is never used at runtime, so a bare placeholder suffices.
    """


class _TestableExtractor(RestApiExtractor[Any]):  # type: ignore[type-var]
    """Minimal concrete RestApiExtractor for isolated testing of _capture_response.

    Implements every abstract method as a no-op — none are exercised by these
    tests. The real lifecycle is covered by per-source integration tests.
    """

    source_name: str = "test"

    def extract(self) -> list[dict[str, Any]]:
        return []

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        return ""

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[Any], list[QuarantineRecord]]:
        return [], []

    def check_invariants(self, records: list[Any]) -> tuple[list[Any], list[QuarantineRecord]]:
        return [], []

    def load_bronze(
        self,
        records: list[Any],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        return 0


def _make_response(
    *,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
    body: bytes = b'{"test": true}',
) -> httpx.Response:
    return httpx.Response(
        status_code=status_code,
        headers=headers or {},
        content=body,
    )


def _new_extractor() -> _TestableExtractor:
    return _TestableExtractor(base_url="https://example.test")


# --- Tests ---


def test_capture_populates_all_five_fields() -> None:
    extractor = _new_extractor()
    response = _make_response(
        status_code=200,
        headers={
            "etag": '"abc123"',
            "last-modified": "Fri, 01 May 2026 20:51:23 GMT",
            "content-type": "application/json",
        },
        body=b'{"hello": "world"}',
    )

    extractor._capture_response(response)

    assert extractor._captured_response_status_code == 200
    assert extractor._captured_response_etag == '"abc123"'
    assert extractor._captured_response_last_modified == "Fri, 01 May 2026 20:51:23 GMT"
    assert (
        extractor._captured_response_body_sha256
        == hashlib.sha256(b'{"hello": "world"}').hexdigest()
    )
    assert extractor._captured_response_headers is not None
    assert extractor._captured_response_headers["etag"] == '"abc123"'
    assert extractor._captured_response_headers["content-type"] == "application/json"


def test_capture_with_missing_etag_yields_none() -> None:
    """CPSC / FDA case: their APIs don't emit ETag; column must be NULL, not crash."""
    extractor = _new_extractor()
    response = _make_response(headers={"content-type": "application/json"})

    extractor._capture_response(response)

    assert extractor._captured_response_etag is None
    assert extractor._captured_response_last_modified is None
    # Body hash and status code still populate.
    assert extractor._captured_response_status_code == 200
    assert extractor._captured_response_body_sha256 is not None


def test_capture_body_sha256_is_deterministic_for_known_input() -> None:
    """Regression: SHA-256 of empty bytes is a well-known constant."""
    extractor = _new_extractor()
    response = _make_response(body=b"")

    extractor._capture_response(response)

    assert extractor._captured_response_body_sha256 == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_capture_body_sha256_is_64_lowercase_hex_chars() -> None:
    """Format invariant: hash column should always be exactly 64 lowercase hex chars."""
    extractor = _new_extractor()
    response = _make_response(body=b"any payload here")

    extractor._capture_response(response)

    digest = extractor._captured_response_body_sha256
    assert digest is not None
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


def test_body_override_hashes_override_not_response_content() -> None:
    """Passing body=... explicitly should hash the override, e.g. when the
    response body has already been consumed by the caller."""
    extractor = _new_extractor()
    actual = b"original body bytes"
    override = b"explicit override bytes"
    response = _make_response(body=actual)

    extractor._capture_response(response, body=override)

    assert extractor._captured_response_body_sha256 == hashlib.sha256(override).hexdigest()
    # And it's NOT the response.content hash.
    assert extractor._captured_response_body_sha256 != hashlib.sha256(actual).hexdigest()


def test_repeated_capture_overwrites_prior_state() -> None:
    """Multiple captures in the same run (e.g. paginated source calling on
    every page) overwrite — last wins. This is documented behavior; concrete
    extractors call once on the primary/first response."""
    extractor = _new_extractor()
    extractor._capture_response(_make_response(headers={"etag": '"v1"'}))
    extractor._capture_response(_make_response(headers={"etag": '"v2"'}))

    assert extractor._captured_response_etag == '"v2"'


def test_capture_handles_304_not_modified() -> None:
    """304 path: empty body, headers may be present. Body hash is the
    well-known empty-bytes sha256 — useful as a sentinel if etag_enabled
    flips on and 304s start arriving."""
    extractor = _new_extractor()
    response = _make_response(
        status_code=304,
        headers={"etag": '"unchanged"'},
        body=b"",
    )

    extractor._capture_response(response)

    assert extractor._captured_response_status_code == 304
    assert extractor._captured_response_etag == '"unchanged"'
    assert extractor._captured_response_body_sha256 == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_initial_state_is_all_none() -> None:
    """Fresh extractor has no captured state until _capture_response is called."""
    extractor = _new_extractor()

    assert extractor._captured_response_status_code is None
    assert extractor._captured_response_etag is None
    assert extractor._captured_response_last_modified is None
    assert extractor._captured_response_body_sha256 is None
    assert extractor._captured_response_headers is None
