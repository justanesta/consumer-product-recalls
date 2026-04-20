from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

if TYPE_CHECKING:
    import pytest

from src.cli.main import app

runner = CliRunner()


def test_version_command_prints_expected_string() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "consumer-product-recalls 0.1.0" in result.output


def test_version_command_exits_with_zero_exit_code() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# extract command
# ---------------------------------------------------------------------------

_REQUIRED_ENV = {
    "NEON_DATABASE_URL": "postgresql://user:pass@localhost/test",
    "R2_ACCOUNT_ID": "test-account",
    "R2_ACCESS_KEY_ID": "test-key-id",
    "R2_SECRET_ACCESS_KEY": "test-secret",
    "R2_BUCKET_NAME": "test-bucket",
}


def _fake_run_result(
    fetched: int = 5,
    loaded: int = 5,
    rejected_validate: int = 0,
    rejected_invariants: int = 0,
) -> MagicMock:
    r = MagicMock()
    r.records_fetched = fetched
    r.records_loaded = loaded
    r.records_rejected_validate = rejected_validate
    r.records_rejected_invariants = rejected_invariants
    return r


def test_extract_cpsc_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=10, loaded=9)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.cpsc.R2LandingClient"),
        patch("sqlalchemy.create_engine"),
        patch("src.extractors.cpsc.CpscExtractor", return_value=mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "cpsc"])

    assert result.exit_code == 0
    assert "fetched=10" in result.output
    assert "loaded=9" in result.output
    assert "rejected=0" in result.output


def test_extract_cpsc_lookback_days_updates_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()
    mock_conn = MagicMock()
    mock_extractor._engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_extractor._engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.cpsc.R2LandingClient"),
        patch("sqlalchemy.create_engine"),
        patch("src.extractors.cpsc.CpscExtractor", return_value=mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "cpsc", "--lookback-days", "7"])

    assert result.exit_code == 0
    mock_conn.execute.assert_called_once()


def test_extract_unknown_source_exits_with_error() -> None:
    result = runner.invoke(app, ["extract", "unknown_source"])
    assert result.exit_code == 1
    assert "Unknown source" in result.output
