from __future__ import annotations

from typing import Annotated

import typer

from src.config.logging import configure_logging

app = typer.Typer(name="recalls", help="Consumer product recalls pipeline CLI")


@app.command()
def version() -> None:
    """Print the current version."""
    typer.echo("consumer-product-recalls 0.1.0")


@app.command()
def extract(
    source: Annotated[str, typer.Argument(help="Source to extract (e.g. cpsc, fda)")],
    lookback_days: Annotated[
        int | None,
        typer.Option("--lookback-days", help="Override watermark with N days ago"),
    ] = None,
) -> None:
    """Run the incremental extractor for a given source."""
    configure_logging()

    if source == "cpsc":
        from src.config.settings import Settings
        from src.extractors.cpsc import CpscExtractor

        settings = Settings()  # type: ignore[call-arg]  # reads from env vars
        extractor = CpscExtractor(
            base_url="https://www.saferproducts.gov/RestWebServices/Recall",
            settings=settings,
        )
        if lookback_days is not None:
            from datetime import UTC, datetime, timedelta

            import sqlalchemy as sa

            from src.extractors.cpsc import _source_watermarks

            override_date = datetime.now(UTC).date() - timedelta(days=lookback_days)
            with extractor._engine.begin() as conn:
                conn.execute(
                    sa.update(_source_watermarks)
                    .where(_source_watermarks.c.source == "cpsc")
                    .values(last_cursor=override_date.isoformat())
                )

        result = extractor.run()
        typer.echo(
            f"cpsc: fetched={result.records_fetched} "
            f"loaded={result.records_loaded} "
            f"rejected={result.records_rejected_validate + result.records_rejected_invariants}"
        )

    elif source == "fda":
        from src.config.settings import Settings
        from src.extractors.fda import FdaExtractor, _source_watermarks

        settings = Settings()  # type: ignore[call-arg]
        extractor = FdaExtractor(
            base_url="https://www.accessdata.fda.gov/rest/iresapi",
            settings=settings,
        )
        if lookback_days is not None:
            from datetime import UTC, datetime, timedelta

            import sqlalchemy as sa

            override_date = datetime.now(UTC).date() - timedelta(days=lookback_days)
            with extractor._engine.begin() as conn:
                conn.execute(
                    sa.update(_source_watermarks)
                    .where(_source_watermarks.c.source == "fda")
                    .values(last_cursor=override_date.isoformat())
                )

        result = extractor.run()
        typer.echo(
            f"fda: fetched={result.records_fetched} "
            f"loaded={result.records_loaded} "
            f"rejected={result.records_rejected_validate + result.records_rejected_invariants}"
        )

    elif source == "usda":
        from src.config.settings import Settings
        from src.extractors.usda import UsdaExtractor

        settings = Settings()  # type: ignore[call-arg]
        # USDA has no usable server-side date filter (Finding D in
        # documentation/usda/recall_api_observations.md), so --lookback-days has no
        # meaningful effect on the request; the extractor pulls the full payload every run.
        # The flag is accepted for CLI shape parity but ignored with a notice.
        if lookback_days is not None:
            typer.echo("usda: --lookback-days has no effect (full-dump every run; see Finding D).")
        extractor = UsdaExtractor(
            base_url="https://www.fsis.usda.gov/fsis/api/recall/v/1",
            timeout_seconds=60.0,
            settings=settings,
        )
        result = extractor.run()
        typer.echo(
            f"usda: fetched={result.records_fetched} "
            f"loaded={result.records_loaded} "
            f"rejected={result.records_rejected_validate + result.records_rejected_invariants}"
        )

    elif source == "usda_establishments":
        from src.config.settings import Settings
        from src.extractors.usda_establishment import UsdaEstablishmentExtractor

        settings = Settings()  # type: ignore[call-arg]
        # No incremental cursor exists (Finding A); --lookback-days has no
        # effect. Accepted for CLI shape parity but ignored with a notice.
        if lookback_days is not None:
            typer.echo(
                "usda_establishments: --lookback-days has no effect "
                "(full-dump every run; see Finding A)."
            )
        extractor = UsdaEstablishmentExtractor(
            base_url="https://www.fsis.usda.gov/fsis/api/establishments/v/1",
            timeout_seconds=60.0,
            settings=settings,
        )
        result = extractor.run()
        typer.echo(
            f"usda_establishments: fetched={result.records_fetched} "
            f"loaded={result.records_loaded} "
            f"rejected={result.records_rejected_validate + result.records_rejected_invariants}"
        )

    else:
        typer.echo(f"Unknown source: {source}", err=True)
        raise typer.Exit(code=1)


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
) -> None:
    """Run a historical / deep-rescan load for a given source over a date window."""
    configure_logging()

    if source == "fda":
        from datetime import date

        from src.config.settings import Settings
        from src.extractors.fda import FdaDeepRescanLoader

        if start_date is None or end_date is None:
            typer.echo("fda deep-rescan requires --start-date and --end-date", err=True)
            raise typer.Exit(code=1)

        settings = Settings()  # type: ignore[call-arg]
        loader = FdaDeepRescanLoader(
            base_url="https://www.accessdata.fda.gov/rest/iresapi",
            settings=settings,
        )
        loader.set_date_range(
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
        )
        result = loader.run()
        typer.echo(
            f"fda deep-rescan [{start_date} → {end_date}]: "
            f"fetched={result.records_fetched} "
            f"loaded={result.records_loaded} "
            f"rejected={result.records_rejected_validate + result.records_rejected_invariants}"
        )

    elif source == "usda":
        from src.config.settings import Settings
        from src.extractors.usda import UsdaDeepRescanLoader

        # USDA's deep-rescan path fetches the full payload (same as incremental) but
        # never touches source_watermarks and never sends If-None-Match — the workflow
        # exists as an operator-triggered "force a full re-pull" knob and as a weekly
        # safety net that self-corrects any silent ETag bug (Finding N in
        # documentation/usda/recall_api_observations.md).
        if start_date is not None or end_date is not None:
            typer.echo(
                "usda: --start-date / --end-date are ignored (full-dump every run; see Finding D)."
            )
        settings = Settings()  # type: ignore[call-arg]
        loader = UsdaDeepRescanLoader(
            base_url="https://www.fsis.usda.gov/fsis/api/recall/v/1",
            timeout_seconds=60.0,
            settings=settings,
        )
        result = loader.run()
        typer.echo(
            f"usda deep-rescan: "
            f"fetched={result.records_fetched} "
            f"loaded={result.records_loaded} "
            f"rejected={result.records_rejected_validate + result.records_rejected_invariants}"
        )

    else:
        typer.echo(f"Deep-rescan not implemented for source: {source}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":  # pragma: no cover
    app()
