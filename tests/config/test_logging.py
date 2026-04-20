from __future__ import annotations

import logging

import pytest
import structlog.contextvars
import structlog.dev
import structlog.processors
import structlog.stdlib

from src.config.logging import _bind_github_context, configure_logging

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clear_structlog_context() -> None:
    structlog.contextvars.clear_contextvars()


# ---------------------------------------------------------------------------
# configure_logging — root logger setup
# ---------------------------------------------------------------------------


def test_configure_logging_sets_root_logger_to_given_level() -> None:
    configure_logging("DEBUG")
    root = logging.getLogger()
    assert root.level == logging.DEBUG


def test_configure_logging_sets_root_logger_to_info_by_default() -> None:
    configure_logging()
    root = logging.getLogger()
    assert root.level == logging.INFO


def test_configure_logging_sets_root_logger_to_warning_level() -> None:
    configure_logging("WARNING")
    root = logging.getLogger()
    assert root.level == logging.WARNING


def test_configure_logging_clears_existing_handlers_before_adding_new_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    root = logging.getLogger()
    # Pre-install a dummy handler so we can verify it is replaced.
    dummy_handler = logging.NullHandler()
    root.addHandler(dummy_handler)

    configure_logging("INFO")

    assert dummy_handler not in root.handlers


def test_configure_logging_adds_exactly_one_stream_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    configure_logging("INFO")
    root = logging.getLogger()
    stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
    assert len(stream_handlers) == 1


def test_configure_logging_stream_handler_has_processor_formatter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    configure_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    assert isinstance(handler.formatter, structlog.stdlib.ProcessorFormatter)


# ---------------------------------------------------------------------------
# configure_logging — JSON mode (non-tty / CI)
# ---------------------------------------------------------------------------


def test_configure_logging_uses_json_renderer_when_stderr_is_not_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    configure_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    renderer_types = [type(p).__name__ for p in formatter.processors]
    assert "JSONRenderer" in renderer_types


# ---------------------------------------------------------------------------
# configure_logging — console mode (tty or LOG_FORMAT=console)
# ---------------------------------------------------------------------------


def test_configure_logging_uses_console_renderer_when_stderr_is_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: True)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    configure_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    renderer_types = [type(p).__name__ for p in formatter.processors]
    assert "ConsoleRenderer" in renderer_types


def test_configure_logging_uses_console_renderer_when_log_format_env_is_console(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    monkeypatch.setenv("LOG_FORMAT", "console")
    configure_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    renderer_types = [type(p).__name__ for p in formatter.processors]
    assert "ConsoleRenderer" in renderer_types


def test_configure_logging_log_format_env_is_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    monkeypatch.setenv("LOG_FORMAT", "CONSOLE")
    configure_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    renderer_types = [type(p).__name__ for p in formatter.processors]
    assert "ConsoleRenderer" in renderer_types


def test_configure_logging_non_console_log_format_uses_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    renderer_types = [type(p).__name__ for p in formatter.processors]
    assert "JSONRenderer" in renderer_types


# ---------------------------------------------------------------------------
# configure_logging — noisy third-party loggers are silenced
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "logger_name",
    ["sqlalchemy.engine", "sqlalchemy.pool", "httpx", "tenacity", "dbt"],
)
def test_configure_logging_silences_noisy_third_party_loggers(
    monkeypatch: pytest.MonkeyPatch, logger_name: str
) -> None:
    monkeypatch.setattr("sys.stderr.isatty", lambda: False)
    configure_logging("DEBUG")
    noisy = logging.getLogger(logger_name)
    assert noisy.level == logging.WARNING


# ---------------------------------------------------------------------------
# _bind_github_context
# ---------------------------------------------------------------------------


def test_bind_github_context_binds_run_url_when_all_three_vars_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_structlog_context()
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "99999")

    _bind_github_context()

    ctx = structlog.contextvars.get_contextvars()
    assert "github_run_url" in ctx
    assert ctx["github_run_url"] == "https://github.com/org/repo/actions/runs/99999"
    _clear_structlog_context()


def test_bind_github_context_does_not_bind_when_server_url_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_structlog_context()
    monkeypatch.delenv("GITHUB_SERVER_URL", raising=False)
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "99999")

    _bind_github_context()

    ctx = structlog.contextvars.get_contextvars()
    assert "github_run_url" not in ctx
    _clear_structlog_context()


def test_bind_github_context_does_not_bind_when_repository_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_structlog_context()
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.setenv("GITHUB_RUN_ID", "99999")

    _bind_github_context()

    ctx = structlog.contextvars.get_contextvars()
    assert "github_run_url" not in ctx
    _clear_structlog_context()


def test_bind_github_context_does_not_bind_when_run_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_structlog_context()
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "org/repo")
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)

    _bind_github_context()

    ctx = structlog.contextvars.get_contextvars()
    assert "github_run_url" not in ctx
    _clear_structlog_context()


def test_bind_github_context_does_not_bind_when_all_vars_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_structlog_context()
    monkeypatch.delenv("GITHUB_SERVER_URL", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)

    _bind_github_context()

    ctx = structlog.contextvars.get_contextvars()
    assert "github_run_url" not in ctx
    _clear_structlog_context()


def test_bind_github_context_constructs_correct_url_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_structlog_context()
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/recalls-pipeline")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345678")

    _bind_github_context()

    ctx = structlog.contextvars.get_contextvars()
    expected = "https://github.com/acme/recalls-pipeline/actions/runs/12345678"
    assert ctx["github_run_url"] == expected
    _clear_structlog_context()
