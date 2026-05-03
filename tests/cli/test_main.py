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


def test_extract_invalid_change_type_exits_with_error() -> None:
    result = runner.invoke(app, ["extract", "cpsc", "--change-type", "bogus"])
    assert result.exit_code == 1
    assert "Invalid --change-type" in result.output
    assert "must be one of" in result.output


def test_extract_fda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=3, loaded=3)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.fda.FdaExtractor", return_value=mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "fda"])

    assert result.exit_code == 0
    assert "fda:" in result.output
    assert "fetched=3" in result.output
    assert "loaded=3" in result.output
    assert "rejected=0" in result.output


def test_extract_fda_lookback_days_updates_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result()
    mock_conn = MagicMock()
    mock_extractor._engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_extractor._engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.fda.FdaExtractor", return_value=mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "fda", "--lookback-days", "7"])

    assert result.exit_code == 0
    mock_conn.execute.assert_called_once()


# ---------------------------------------------------------------------------
# deep-rescan command
# ---------------------------------------------------------------------------


def test_deep_rescan_fda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=150, loaded=149, rejected_validate=1)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.fda.FdaDeepRescanLoader", return_value=mock_loader),
    ):
        result = runner.invoke(
            app,
            [
                "deep-rescan",
                "fda",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-04-26",
            ],
        )

    assert result.exit_code == 0
    assert "fda deep-rescan" in result.output
    assert "fetched=150" in result.output
    assert "loaded=149" in result.output
    assert "rejected=1" in result.output
    mock_loader.set_date_range.assert_called_once()


def test_deep_rescan_unknown_source_exits_with_error() -> None:
    result = runner.invoke(
        app,
        ["deep-rescan", "unknown", "--start-date", "2026-01-01", "--end-date", "2026-04-26"],
    )
    assert result.exit_code == 1
    assert "not implemented" in result.output


# ---------------------------------------------------------------------------
# USDA dispatch — extract and deep-rescan
# ---------------------------------------------------------------------------


def test_extract_usda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=2001, loaded=2001)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.usda.UsdaExtractor", return_value=mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda"])

    assert result.exit_code == 0
    assert "usda:" in result.output
    assert "fetched=2001" in result.output


def test_extract_usda_lookback_days_warns_but_does_not_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=2001, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.usda.UsdaExtractor", return_value=mock_extractor),
    ):
        result = runner.invoke(app, ["extract", "usda", "--lookback-days", "7"])

    assert result.exit_code == 0
    assert "no effect" in result.output


def test_deep_rescan_usda_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=2001, loaded=12)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.usda.UsdaDeepRescanLoader", return_value=mock_loader),
    ):
        result = runner.invoke(app, ["deep-rescan", "usda"])

    assert result.exit_code == 0
    assert "usda deep-rescan" in result.output
    assert "fetched=2001" in result.output


def test_deep_rescan_usda_ignores_date_args(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_loader = MagicMock()
    mock_loader.run.return_value = _fake_run_result(fetched=2001, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        patch("src.extractors.usda.UsdaDeepRescanLoader", return_value=mock_loader),
    ):
        result = runner.invoke(
            app,
            [
                "deep-rescan",
                "usda",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-04-26",
            ],
        )

    assert result.exit_code == 0
    assert "ignored" in result.output


def test_deep_rescan_fda_missing_dates_exits_with_error(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    with patch("src.cli.main.configure_logging"):
        result = runner.invoke(app, ["deep-rescan", "fda"])
    assert result.exit_code == 1
    assert "requires" in result.output


# ---------------------------------------------------------------------------
# USDA establishments dispatch — extract only (no deep-rescan path; full-dump every run)
# ---------------------------------------------------------------------------


def test_extract_usda_establishments_prints_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=7945, loaded=7945)

    with (
        patch("src.cli.main.configure_logging"),
        patch(
            "src.extractors.usda_establishment.UsdaEstablishmentExtractor",
            return_value=mock_extractor,
        ),
    ):
        result = runner.invoke(app, ["extract", "usda_establishments"])

    assert result.exit_code == 0
    assert "usda_establishments:" in result.output
    assert "fetched=7945" in result.output
    assert "loaded=7945" in result.output


def test_extract_usda_establishments_lookback_days_warns_but_does_not_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)

    mock_extractor = MagicMock()
    mock_extractor.run.return_value = _fake_run_result(fetched=7945, loaded=0)

    with (
        patch("src.cli.main.configure_logging"),
        patch(
            "src.extractors.usda_establishment.UsdaEstablishmentExtractor",
            return_value=mock_extractor,
        ),
    ):
        result = runner.invoke(app, ["extract", "usda_establishments", "--lookback-days", "7"])

    assert result.exit_code == 0
    assert "no effect" in result.output
