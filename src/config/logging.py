from __future__ import annotations

import logging
import os
import sys

import structlog
import structlog.contextvars
import structlog.dev
import structlog.processors
import structlog.stdlib


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structlog for the recall pipeline. Call once at application startup.

    Output format (ADR 0021):
    - Interactive terminal (stderr isatty) or LOG_FORMAT=console → colored ConsoleRenderer.
    - CI / production → JSON to stdout. GitHub Actions captures stdout into its log archive.

    Correlation (ADR 0021):
    - run_id, source, and stage are bound via structlog.contextvars.bind_contextvars()
      in Extractor.run() and flow into every log line automatically.
    - github_run_url is bound here at startup when GHA env vars are present.

    Stdlib bridge (ADR 0021):
    - All output — structlog-native and third-party (SQLAlchemy, httpx, tenacity, dbt) —
      routes through a single stdlib StreamHandler so both streams share one processor
      chain and one final renderer, with no double-encoding.
    - Third-party libraries are set to WARNING to suppress debug/info chatter.

    Implementation pattern:
    - structlog.configure() processors end with ProcessorFormatter.wrap_for_formatter
      instead of a renderer. This packages the event dict for the stdlib handler.
    - ProcessorFormatter's own processors list applies remove_processors_meta then the
      final renderer exactly once, regardless of whether the log came from structlog or
      a third-party stdlib logger.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    use_console = sys.stderr.isatty() or os.getenv("LOG_FORMAT", "").lower() == "console"

    # Processors applied to every log line — structlog-native and third-party alike.
    # merge_contextvars pulls run_id / source / stage / github_run_url from the
    # contextvars dict that Extractor.run() populates at the start of each run.
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
    ]

    if use_console:
        # ConsoleRenderer handles exception rendering internally.
        final_renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        # format_exc_info turns exception info into a structured dict field before
        # JSONRenderer serialises the whole event to a single-line JSON object.
        shared_processors.append(structlog.processors.format_exc_info)
        final_renderer = structlog.processors.JSONRenderer()

    # structlog-native loggers: wrap_for_formatter is the last processor, NOT a
    # renderer. It packages the event dict so ProcessorFormatter can pick it up and
    # apply the final renderer exactly once (avoiding double-encoding).
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Single stdlib handler for all output: structlog-native records arrive already
    # processed by the chain above; foreign (third-party) records get foreign_pre_chain
    # applied first. Both then pass through processors (remove_meta + final_renderer).
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                final_renderer,
            ],
            foreign_pre_chain=shared_processors,
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Silence chatty third-party libraries at WARNING regardless of pipeline log_level.
    for noisy in ("sqlalchemy.engine", "sqlalchemy.pool", "httpx", "tenacity", "dbt"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _bind_github_context()


def _bind_github_context() -> None:
    """
    Bind GitHub Actions run URL to structlog contextvars when running in CI.
    The URL appears on every log line emitted during the run, giving a one-click hop
    from a log query to the Actions run that produced it (ADR 0021 + ADR 0020).
    """
    server = os.getenv("GITHUB_SERVER_URL")
    repo = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if server and repo and run_id:
        structlog.contextvars.bind_contextvars(
            github_run_url=f"{server}/{repo}/actions/runs/{run_id}"
        )
