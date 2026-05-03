"""Smoke tests to verify the package is importable and the CLI app is wired up."""

from typer.testing import CliRunner

from src.cli.main import app
from src.config.settings import Settings


def test_cli_version() -> None:
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    # Don't assert a specific version number — that would couple this test to
    # the current pyproject.toml value and force a test edit on every bump.
    # The version command reads from importlib.metadata; verifying the prefix
    # confirms the wiring without pinning to a value.
    assert "consumer-product-recalls " in result.output


def test_settings_class_is_importable() -> None:
    assert Settings.__name__ == "Settings"
