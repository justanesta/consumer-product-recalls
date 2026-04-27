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

    else:
        typer.echo(f"Unknown source: {source}", err=True)
        raise typer.Exit(code=1)


@app.command(name="deep-rescan")
def deep_rescan(
    source: Annotated[str, typer.Argument(help="Source to deep-rescan (e.g. fda)")],
    start_date: Annotated[
        str,
        typer.Option("--start-date", help="Start date (YYYY-MM-DD)"),
    ],
    end_date: Annotated[
        str,
        typer.Option("--end-date", help="End date (YYYY-MM-DD)"),
    ],
) -> None:
    """Run a historical / deep-rescan load for a given source over a date window."""
    configure_logging()

    if source == "fda":
        from datetime import date

        from src.config.settings import Settings
        from src.extractors.fda import FdaDeepRescanLoader

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

    else:
        typer.echo(f"Deep-rescan not implemented for source: {source}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":  # pragma: no cover
    app()
