import typer

app = typer.Typer(name="recalls", help="Consumer product recalls pipeline CLI")


@app.command()
def version() -> None:
    """Print the current version."""
    typer.echo("consumer-product-recalls 0.1.0")


if __name__ == "__main__":  # pragma: no cover
    app()
