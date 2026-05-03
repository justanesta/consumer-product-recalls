"""Verify each extractor's _record_run() persists the response-capture fields.

The capture path has two halves: (a) `_capture_response()` populates the
PrivateAttrs (covered in test_response_capture.py), and (b) `_record_run()`
reads those PrivateAttrs and includes them in the row inserted into
`extraction_runs`. These tests cover (b) for all four concrete extractors.

Strategy: monkey-patch `_extraction_runs.insert()` in each extractor module
so the chain `_extraction_runs.insert().values(**row)` captures the row
dict instead of producing a real SQL statement. Mock the engine so the
`with self._engine.begin() as conn: conn.execute(...)` path is a no-op.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from pydantic import SecretStr

from src.config.settings import Settings
from src.extractors._base import ExtractionResult

# --- Fixtures ---


@pytest.fixture
def fake_settings() -> Settings:
    """Settings with placeholder values; never used to actually connect."""
    return Settings(
        neon_database_url=SecretStr("postgresql://test:test@localhost/test"),
        r2_account_id="test",
        r2_access_key_id=SecretStr("test"),
        r2_secret_access_key=SecretStr("test"),
        r2_bucket_name="test",
    )


@pytest.fixture
def patch_extractor_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace `sa.create_engine` and `R2LandingClient` with mocks so extractor
    instantiation doesn't try to connect to a real database or R2."""
    monkeypatch.setattr(sa, "create_engine", lambda *args, **kwargs: MagicMock())
    monkeypatch.setattr("src.landing.r2.R2LandingClient", lambda *args, **kwargs: MagicMock())


def _intercept_insert_values(
    monkeypatch: pytest.MonkeyPatch, extraction_runs_table: sa.Table
) -> dict[str, Any]:
    """Replace `_extraction_runs.insert()` with a chain that captures the
    dict passed to `.values(**row)`. Returns the captured dict — populated
    after `_record_run` runs."""
    captured: dict[str, Any] = {}

    def fake_insert() -> Any:
        fake_stmt = MagicMock()

        def fake_values(**kwargs: Any) -> Any:
            captured.update(kwargs)
            return fake_stmt

        fake_stmt.values = fake_values
        return fake_stmt

    monkeypatch.setattr(extraction_runs_table, "insert", fake_insert)
    return captured


def _populate_capture_state(extractor: Any) -> None:
    """Set every captured-response PrivateAttr to a known sentinel."""
    extractor._captured_response_status_code = 200
    extractor._captured_response_etag = '"test-etag"'
    extractor._captured_response_last_modified = "Fri, 01 May 2026 20:51:23 GMT"
    extractor._captured_response_body_sha256 = "f" * 64
    extractor._captured_response_headers = {
        "etag": '"test-etag"',
        "content-type": "application/json",
    }


def _make_result(source: str) -> ExtractionResult:
    return ExtractionResult(
        source=source,
        run_id="test-run-id",
        records_fetched=10,
        records_landed=10,
        records_valid=10,
        records_rejected_validate=0,
        records_rejected_invariants=0,
        records_loaded=5,
        raw_landing_path=f"{source}/2026-05-03/test.json",
    )


def _assert_response_fields_present(captured: dict[str, Any]) -> None:
    """Common shape assertion across the four extractor tests."""
    assert captured["response_status_code"] == 200
    assert captured["response_etag"] == '"test-etag"'
    assert captured["response_last_modified"] == "Fri, 01 May 2026 20:51:23 GMT"
    assert captured["response_body_sha256"] == "f" * 64
    assert captured["response_headers"] == {
        "etag": '"test-etag"',
        "content-type": "application/json",
    }


# --- Tests, one per concrete extractor ---


def test_cpsc_record_run_persists_response_capture_fields(
    monkeypatch: pytest.MonkeyPatch,
    patch_extractor_dependencies: None,
    fake_settings: Settings,
) -> None:
    from src.extractors import cpsc as mod

    captured = _intercept_insert_values(monkeypatch, mod._extraction_runs)

    extractor = mod.CpscExtractor(base_url="https://example.test/cpsc", settings=fake_settings)
    _populate_capture_state(extractor)

    extractor._record_run(
        run_id="test-run-id",
        started_at=datetime.now(UTC),
        status="success",
        result=_make_result("cpsc"),
    )

    _assert_response_fields_present(captured)


def test_fda_record_run_persists_response_capture_fields(
    monkeypatch: pytest.MonkeyPatch,
    patch_extractor_dependencies: None,
    fake_settings: Settings,
) -> None:
    from src.extractors import fda as mod

    captured = _intercept_insert_values(monkeypatch, mod._extraction_runs)

    extractor = mod.FdaExtractor(base_url="https://example.test/fda", settings=fake_settings)
    _populate_capture_state(extractor)

    extractor._record_run(
        run_id="test-run-id",
        started_at=datetime.now(UTC),
        status="success",
        result=_make_result("fda"),
    )

    _assert_response_fields_present(captured)


def test_usda_record_run_persists_response_capture_fields(
    monkeypatch: pytest.MonkeyPatch,
    patch_extractor_dependencies: None,
    fake_settings: Settings,
) -> None:
    from src.extractors import usda as mod

    captured = _intercept_insert_values(monkeypatch, mod._extraction_runs)

    extractor = mod.UsdaExtractor(base_url="https://example.test/usda", settings=fake_settings)
    _populate_capture_state(extractor)

    extractor._record_run(
        run_id="test-run-id",
        started_at=datetime.now(UTC),
        status="success",
        result=_make_result("usda"),
    )

    _assert_response_fields_present(captured)


def test_usda_establishment_record_run_persists_response_capture_fields(
    monkeypatch: pytest.MonkeyPatch,
    patch_extractor_dependencies: None,
    fake_settings: Settings,
) -> None:
    from src.extractors import usda_establishment as mod

    captured = _intercept_insert_values(monkeypatch, mod._extraction_runs)

    extractor = mod.UsdaEstablishmentExtractor(
        base_url="https://example.test/usda-establishments", settings=fake_settings
    )
    _populate_capture_state(extractor)

    extractor._record_run(
        run_id="test-run-id",
        started_at=datetime.now(UTC),
        status="success",
        result=_make_result("usda_establishments"),
    )

    _assert_response_fields_present(captured)


# --- Negative case: empty capture state ---


def test_record_run_skips_response_fields_when_capture_state_empty(
    monkeypatch: pytest.MonkeyPatch,
    patch_extractor_dependencies: None,
    fake_settings: Settings,
) -> None:
    """Failed runs (network errors, exceptions before fetch) leave capture
    state at None. _record_run should NOT include the response_* keys in the
    insert dict — leaving them NULL by absence rather than explicitly NULL.
    Tested on CPSC; the gating logic is identical across all four extractors."""
    from src.extractors import cpsc as mod

    captured = _intercept_insert_values(monkeypatch, mod._extraction_runs)

    extractor = mod.CpscExtractor(base_url="https://example.test/cpsc", settings=fake_settings)
    # Deliberately do NOT call _populate_capture_state — captures stay None.

    extractor._record_run(
        run_id="test-run-id",
        started_at=datetime.now(UTC),
        status="failed",
        error_message="simulated network error",
    )

    assert "response_status_code" not in captured
    assert "response_etag" not in captured
    assert "response_last_modified" not in captured
    assert "response_body_sha256" not in captured
    assert "response_headers" not in captured
    # Sanity check: the row dict still has the standard fields.
    assert captured["status"] == "failed"
    assert captured["error_message"] == "simulated network error"
