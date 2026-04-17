"""Smoke tests to verify the package is importable and the CLI app is wired up."""

from typer.testing import CliRunner

from src.cli.main import app
from src.config.settings import Settings


def test_cli_version() -> None:
    result = CliRunner().invoke(app, [])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_settings_class_is_importable() -> None:
    assert Settings.__name__ == "Settings"
