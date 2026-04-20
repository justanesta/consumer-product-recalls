from __future__ import annotations

from typer.testing import CliRunner

from src.cli.main import app

runner = CliRunner()


def test_version_command_prints_expected_string() -> None:
    result = runner.invoke(app, [])
    assert result.exit_code == 0
    assert "consumer-product-recalls 0.1.0" in result.output


def test_version_command_exits_with_zero_exit_code() -> None:
    result = runner.invoke(app, [])
    assert result.exit_code == 0
