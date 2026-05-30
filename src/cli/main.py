from __future__ import annotations

from datetime import date
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING, Annotated

import typer

from src.config.logging import configure_logging
from src.config.settings import Settings
from src.config.source_loader import load_source_config
from src.config.source_registry import (
    DEEP_RESCAN_BY_SOURCE_NAME,
    EXTRACTOR_BY_SOURCE_NAME,
    build_extractor_kwargs,
)

if TYPE_CHECKING:
    from src.extractors._base import Extractor

app = typer.Typer(name="recalls", help="Consumer product recalls pipeline CLI")

# Allowed values for extraction_runs.change_type (per ADR 0027 + ADR 0028,
# extended 2026-05-10 with etag_audit). DB-level CHECK constraint enforces
# the same set (migrations 0009 + 0012); the CLI validates here so the
# operator gets a clear error before the run even starts.
_ALLOWED_CHANGE_TYPES = {
    "routine",
    "schema_rebaseline",
    "hash_helper_rebaseline",
    "historical_seed",
    "etag_audit",
}

# Sources that support change_type=etag_audit. Restricted to the two USDA
# endpoints because they are the only sources currently using HTTP
# conditional GET (If-None-Match / If-Modified-Since); CPSC and FDA capture
# ETag headers passively, NHTSA uses content-MD5 on flat files. Audit-run
# semantics are only meaningful for the conditional-GET case.
_ETAG_AUDIT_SOURCES = {"usda", "usda_establishments"}

# Sources that implement ``override_watermark_lookback()`` (= sources with a
# date-watermarked incremental cursor). Other sources accept --lookback-days
# for CLI shape parity but emit a per-source notice. Driven by source name
# rather than ``hasattr(extractor, ...)`` because MagicMock auto-attributes
# would defeat the latter under test.
_LOOKBACK_DAYS_SOURCES = {"cpsc", "fda"}

# Per-source notices when --lookback-days is passed to a source that does not
# honor it (no override_watermark_lookback method). Keeps the user's existing
# CLI feedback intact under the new generic dispatch.
_LOOKBACK_NO_OP_MESSAGES: dict[str, str] = {
    "usda": "usda: --lookback-days has no effect (full-dump every run; see Finding D).",
    "usda_establishments": (
        "usda_establishments: --lookback-days has no effect (full-dump every run; see Finding A)."
    ),
    "nhtsa": (
        "nhtsa: --lookback-days has no effect (flat-file full-dump every run; see Findings B + C)."
    ),
    "uscg": (
        "uscg: --lookback-days has no effect (page-0 precheck + full HTML "
        "re-scrape on cache miss; see scraping_observations.md Finding J)."
    ),
    "uscg_manufacturers": (
        "uscg_manufacturers: --lookback-days has no effect (page-0 Records-Found "
        "precheck + full HTML re-scrape on cache miss; see "
        "manufacturer_scraping_observations.md Finding K)."
    ),
    "uscg_manufacturer_details": (
        "uscg_manufacturer_details: --lookback-days has no effect (work-list is a "
        "listing-delta cursor over bronze; see phase-5d-uscg-manufacturers-detail.md)."
    ),
}

# Per-source notices when deep-rescan ignores --start-date/--end-date.
_DEEP_RESCAN_NO_DATE_WINDOW_MESSAGES: dict[str, str] = {
    "usda": "usda: --start-date / --end-date are ignored (full-dump every run; see Finding D).",
    "nhtsa": (
        "nhtsa: --start-date / --end-date are ignored "
        "(archives partitioned by DATEA at the source; see Finding H Q2)."
    ),
    "uscg": (
        "uscg: --start-date / --end-date are ignored "
        "(USCG listing has no date-range query surface; full re-scrape every run)."
    ),
    "uscg_manufacturers": (
        "uscg_manufacturers: --start-date / --end-date are ignored "
        "(directory has no date-range query surface; full re-scrape every run)."
    ),
    "uscg_manufacturer_details": (
        "uscg_manufacturer_details: --start-date / --end-date are ignored "
        "(detail pages have no date-range query surface; full sweep on deep-rescan)."
    ),
}


def _validate_change_type(value: str) -> str:
    """Return the value or raise typer.Exit with a clear message."""
    if value not in _ALLOWED_CHANGE_TYPES:
        allowed = ", ".join(sorted(_ALLOWED_CHANGE_TYPES))
        typer.echo(
            f"Invalid --change-type {value!r}; must be one of: {allowed}",
            err=True,
        )
        raise typer.Exit(code=1)
    return value


def _validate_etag_audit_source(change_type: str, source: str) -> None:
    """Reject change_type=etag_audit for sources that don't use conditional GET."""
    if change_type == "etag_audit" and source not in _ETAG_AUDIT_SOURCES:
        allowed = ", ".join(sorted(_ETAG_AUDIT_SOURCES))
        typer.echo(
            f"--change-type=etag_audit is only supported for: {allowed} "
            f"(got source={source!r}); other sources don't use HTTP conditional GET.",
            err=True,
        )
        raise typer.Exit(code=1)


def _parse_nhtsa_since(value: str | None) -> date | None:
    """Parse the NHTSA --since flag (YYYY-MM-DD) and emit the dev-mode notice."""
    if value is None:
        return None
    try:
        since_date = date.fromisoformat(value)
    except ValueError:
        typer.echo(f"nhtsa: --since must be YYYY-MM-DD; got {value!r}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(
        f"nhtsa: --since {since_date.isoformat()} active — "
        "dev-mode RCDATE filter; bronze will be a date-bounded subset."
    )
    return since_date


def _print_run_summary(prefix: str, result: object) -> None:
    """Render the standard fetched/loaded/rejected summary line."""
    fetched = result.records_fetched  # type: ignore[attr-defined]
    loaded = result.records_loaded  # type: ignore[attr-defined]
    rejected = (
        result.records_rejected_validate  # type: ignore[attr-defined]
        + result.records_rejected_invariants  # type: ignore[attr-defined]
    )
    typer.echo(f"{prefix}fetched={fetched} loaded={loaded} rejected={rejected}")


@app.command()
def version() -> None:
    """Print the current version."""
    typer.echo(f"consumer-product-recalls {_pkg_version('consumer-product-recalls')}")


@app.command()
def extract(
    source: Annotated[str, typer.Argument(help="Source to extract (e.g. cpsc, fda)")],
    lookback_days: Annotated[
        int | None,
        typer.Option("--lookback-days", help="Override watermark with N days ago"),
    ] = None,
    change_type: Annotated[
        str,
        typer.Option(
            "--change-type",
            help=(
                "How to label this run in extraction_runs. One of: routine "
                "(default), schema_rebaseline, hash_helper_rebaseline, "
                "historical_seed, etag_audit. Required to be set explicitly when "
                "re-baselining after a schema or hashing-helper change so "
                "recall_event_history can filter the wave out of edit detection. "
                "Use etag_audit (usda + usda_establishments only) to force an "
                "unconditional GET and verify ETag-validation honesty against the "
                "most recent prior 200 — see scripts/sql/_pipeline/etag_audit_check.sql."
            ),
        ),
    ] = "routine",
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help=(
                "NHTSA only: drop rows whose RCDATE is earlier than YYYY-MM-DD. "
                "Intended for free-tier-aware dev workflows on the Neon dev "
                "branch — production historical seed uses the deep-rescan path "
                "which has no --since filter and lands the full corpus. "
                "Ignored for non-NHTSA sources."
            ),
        ),
    ] = None,
) -> None:
    """Run the incremental extractor for a given source.

    Per ADR 0012, source-specific configuration (URL, timeout, etag_enabled)
    lives in ``config/sources/<source>.yaml`` and is loaded by the
    ``source_loader``. The CLI's role is to materialize CLI-flag-specific
    behavior (lookback, since, etag_audit) on top of the YAML-driven
    extractor instance.
    """
    configure_logging()
    change_type = _validate_change_type(change_type)
    _validate_etag_audit_source(change_type, source)

    if source not in EXTRACTOR_BY_SOURCE_NAME:
        typer.echo(f"Unknown source: {source}", err=True)
        raise typer.Exit(code=1)

    if since is not None and source != "nhtsa":
        typer.echo(f"{source}: --since is only honored for nhtsa; ignored.")

    # Validate CLI flag values upfront so format errors surface BEFORE the
    # heavier ``Settings()`` / ``load_source_config`` work — keeps
    # "must be YYYY-MM-DD" errors visible without an env-var setup ritual.
    since_date: date | None = None
    if source == "nhtsa":
        since_date = _parse_nhtsa_since(since)

    config = load_source_config(source)
    settings = Settings()  # type: ignore[call-arg]  # reads from env vars
    extractor_cls = EXTRACTOR_BY_SOURCE_NAME[source]
    kwargs = build_extractor_kwargs(config, extractor_cls, settings)

    # NHTSA-only: --since is a CLI flag, not a YAML field. Always set the
    # constructor kwarg (None when the flag is absent) since NhtsaExtractor
    # declares ``since`` as a required Pydantic field with default None.
    if source == "nhtsa":
        kwargs["since"] = since_date

    extractor: Extractor = extractor_cls(**kwargs)

    # --lookback-days: only CPSC + FDA implement override_watermark_lookback;
    # other sources ignore the flag with a per-source notice.
    if lookback_days is not None:
        if source in _LOOKBACK_DAYS_SOURCES:
            extractor.override_watermark_lookback(lookback_days)  # type: ignore[attr-defined]
        else:
            typer.echo(_LOOKBACK_NO_OP_MESSAGES[source])

    # --change-type=etag_audit: post-construction mutation. Only reachable for
    # USDA recall + USDA establishments (gated by _validate_etag_audit_source).
    # Forces unconditional GET so the audit-check SQL can verify ETag honesty.
    if change_type == "etag_audit":
        extractor.etag_enabled = False  # type: ignore[attr-defined]

    result = extractor.run(change_type=change_type)
    _print_run_summary(f"{source}: ", result)


@app.command(name="deep-rescan")
def deep_rescan(
    source: Annotated[str, typer.Argument(help="Source to deep-rescan (e.g. fda, usda)")],
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="Start date (YYYY-MM-DD); required for FDA"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="End date (YYYY-MM-DD); required for FDA"),
    ] = None,
    change_type: Annotated[
        str,
        typer.Option(
            "--change-type",
            help=(
                "How to label this run in extraction_runs. Use historical_seed for "
                "one-time multi-year backfills (e.g., the CPSC 2005-2024 gap per "
                "ADR 0028 Mechanism A); leave at routine for periodic edit-detection "
                "rescans. One of: routine (default), schema_rebaseline, "
                "hash_helper_rebaseline, historical_seed."
            ),
        ),
    ] = "routine",
) -> None:
    """Run a historical / deep-rescan load for a given source over a date window.

    Same loader+registry pattern as ``extract``; the source-specific extras
    are FDA's ``set_date_range`` mutator (post-construction) and the
    ``etag_enabled`` exclusion that preserves ``UsdaDeepRescanLoader``'s
    class invariant against YAML's ``etag_enabled: true`` (correct for the
    routine path but invariant-violating for deep-rescan).
    """
    configure_logging()
    change_type = _validate_change_type(change_type)

    if source not in DEEP_RESCAN_BY_SOURCE_NAME:
        typer.echo(f"Deep-rescan not implemented for source: {source}", err=True)
        raise typer.Exit(code=1)

    # Date-window pre-validation per source.
    if source == "fda":
        if start_date is None or end_date is None:
            typer.echo("fda deep-rescan requires --start-date and --end-date", err=True)
            raise typer.Exit(code=1)
    elif start_date is not None or end_date is not None:
        typer.echo(_DEEP_RESCAN_NO_DATE_WINDOW_MESSAGES[source])

    config = load_source_config(source)
    settings = Settings()  # type: ignore[call-arg]
    loader_cls = DEEP_RESCAN_BY_SOURCE_NAME[source]

    # Deep-rescan invariant: ``UsdaDeepRescanLoader.etag_enabled = False`` is
    # a class fact, not a YAML knob. Drop the YAML value before construction
    # so a routine-path ``etag_enabled: true`` doesn't override the invariant.
    # FDA + NHTSA deep-rescan loaders don't have this concern (their classes
    # don't declare etag_enabled) but the exclude is harmless across the board.
    kwargs = build_extractor_kwargs(
        config,
        loader_cls,
        settings,
        exclude=frozenset({"etag_enabled"}),
    )
    loader: Extractor = loader_cls(**kwargs)

    # FDA-only: post-construction date-range mutation (the date fields are
    # PrivateAttrs, so they cannot flow through constructor kwargs).
    if source == "fda":
        assert start_date is not None and end_date is not None  # validated above
        loader.set_date_range(  # type: ignore[attr-defined]
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
        )

    result = loader.run(change_type=change_type)
    if source == "fda":
        prefix = f"fda deep-rescan [{start_date} → {end_date}]: "
    else:
        prefix = f"{source} deep-rescan: "
    _print_run_summary(prefix, result)


if __name__ == "__main__":  # pragma: no cover
    app()
