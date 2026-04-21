"""
Live-recorded VCR integration tests for CpscExtractor.

These 4 tests replay real CPSC API responses captured as cassettes. They verify
that the Pydantic schema correctly handles actual API response shapes — the key
value real cassettes provide over hand-crafted respx mocks.

Cassette inventory (the CPSC API returns all matching records in a single
response — there is no pagination to exercise, so the matrix is just
{recent, wide window, narrow window, empty}):
  - test_happy_path_recent.yaml        — 1-day window, recent watermark
  - test_happy_path_wide_window.yaml   — wide date window, many records
  - test_happy_path_narrow_window.yaml — narrow window on a different time slice
  - test_empty_result.yaml             — 0-record response

To record cassettes (requires network access; no auth needed for CPSC):
    uv run pytest --vcr-record=all tests/integration/test_cpsc_live_cassettes.py

Commit the generated YAML files under tests/fixtures/cassettes/cpsc/.
Until cassettes are recorded, tests skip automatically.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa

from src.config.settings import Settings
from src.extractors.cpsc import CpscExtractor

_BASE_URL = "https://www.saferproducts.gov/RestWebServices/Recall"
_FAKE_R2_PATH = "cpsc/cassette-test/placeholder.json.gz"

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


@pytest.fixture(scope="module")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent.parent / "fixtures" / "cassettes" / "cpsc")


@pytest.fixture(autouse=True)
def skip_if_no_cassette(request: pytest.FixtureRequest, vcr_cassette_dir: str) -> None:
    record_mode = request.config.getoption("--vcr-record", default="none")
    if record_mode in ("all", "new_episodes"):
        return  # recording — let VCR create the cassette
    cassette_path = Path(vcr_cassette_dir) / (request.node.name + ".yaml")
    if not cassette_path.exists():
        pytest.skip(
            "Cassette not yet recorded — run: "
            "uv run pytest --vcr-record=all tests/integration/test_cpsc_live_cassettes.py"
        )


@pytest.fixture
def vcr_extractor(monkeypatch: pytest.MonkeyPatch) -> CpscExtractor:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    mock_engine = MagicMock(spec=sa.Engine)
    mock_r2 = MagicMock()
    mock_r2.land.return_value = _FAKE_R2_PATH
    with (
        patch("sqlalchemy.create_engine", return_value=mock_engine),
        patch("src.extractors.cpsc.R2LandingClient", return_value=mock_r2),
    ):
        settings = Settings()  # type: ignore[call-arg]
        return CpscExtractor(base_url=_BASE_URL, settings=settings)


def _run(extractor: CpscExtractor, watermark: date) -> Any:
    """Run the extractor with DB/R2 mocked; HTTP goes through VCR."""
    with (
        patch.object(extractor, "_get_watermark", return_value=watermark),
        patch("src.extractors.cpsc.BronzeLoader") as mock_loader_cls,
        patch.object(extractor, "_update_watermark"),
    ):
        mock_loader_cls.return_value.load.return_value = 0
        mock_engine: MagicMock = extractor._engine  # type: ignore[assignment]
        mock_engine.begin.return_value.__enter__ = lambda _: MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        return extractor.run()


# ---------------------------------------------------------------------------
# Scenario 1: Happy path, recent — 1-day watermark
# Watermark: 2024-03-15
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_recent(vcr_extractor: CpscExtractor) -> None:
    result = _run(vcr_extractor, date(2024, 3, 15))
    assert result.records_fetched > 0
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 2: Happy path, wide window — long date range, many records
# Watermark: 2024-01-01 (all recalls from Jan 2024 onward at record time)
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_wide_window(vcr_extractor: CpscExtractor) -> None:
    result = _run(vcr_extractor, date(2024, 1, 1))
    assert result.records_fetched > 10
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0


# ---------------------------------------------------------------------------
# Scenario 3: Empty result — future watermark date always returns []
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_empty_result(vcr_extractor: CpscExtractor) -> None:
    result = _run(vcr_extractor, date(2099, 1, 1))
    assert result.records_fetched == 0
    assert result.records_loaded == 0
    assert result.rejection_rate == 0.0


# ---------------------------------------------------------------------------
# Scenario 4: Happy path, narrow window — spot-check schema on a different slice
# Watermark: 2024-06-15
# ---------------------------------------------------------------------------


@pytest.mark.vcr
def test_happy_path_narrow_window(vcr_extractor: CpscExtractor) -> None:
    result = _run(vcr_extractor, date(2024, 6, 15))
    assert result.records_rejected_validate == 0
    assert result.records_rejected_invariants == 0
