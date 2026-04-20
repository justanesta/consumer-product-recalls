"""
Promote a captured error response from R2 to a VCR cassette YAML file.

Error responses are captured automatically when the extractor receives a non-2xx
status from any source API. They are stored under:
    {source}/errors/{YYYY-MM-DD}/{uuid}_{status_code}.json.gz

To promote one to a test cassette:

    uv run python scripts/promote_error_to_cassette.py \\
        cpsc/errors/2026-04-20/abc123_429.json.gz \\
        tests/fixtures/cassettes/cpsc/cpsc_rate_limit.yaml

Then update the relevant test to use @pytest.mark.vcr("cpsc_rate_limit.yaml").

Note: this script handles error captures only ({source}/errors/... keys).
For 200 responses containing malformed records, the raw landing artifact is
already stored at {source}/{date}/{uuid}.json.gz and can be inspected directly
via the raw_landing_path column in cpsc_recalls_rejected.

Requires R2 credentials in the environment (same as extraction runs).
"""

from __future__ import annotations

import gzip
import json
from http import HTTPStatus
from pathlib import Path  # noqa: TCH003 — used at runtime by Typer for annotation resolution
from typing import Annotated

import boto3
import typer
import yaml

from src.config.settings import Settings

app = typer.Typer(add_completion=False)

_R2KeyArg = Annotated[
    str,
    typer.Argument(
        help="R2 key of the captured error, e.g. cpsc/errors/2026-04-20/abc_429.json.gz"
    ),
]
_OutputArg = Annotated[
    Path,
    typer.Argument(
        help="Output path for the VCR cassette YAML, e.g. tests/fixtures/cassettes/cpsc/cpsc_rate_limit.yaml"  # noqa: E501
    ),
]


@app.command()
def promote(r2_key: _R2KeyArg, output: _OutputArg) -> None:
    """Promote a captured non-2xx API response from R2 to a VCR cassette YAML."""
    settings = Settings()  # type: ignore[call-arg]

    client = boto3.client(
        "s3",
        endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=settings.r2_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.r2_secret_access_key.get_secret_value(),
        region_name="auto",
    )

    r2_response = client.get_object(Bucket=settings.r2_bucket_name, Key=r2_key)
    captured: dict = json.loads(gzip.decompress(r2_response["Body"].read()))

    status_code: int = captured["status_code"]
    try:
        reason = HTTPStatus(status_code).phrase
    except ValueError:
        reason = "Unknown"

    # vcrpy cassette format: header values are lists
    response_headers = {k: [v] for k, v in captured["response_headers"].items()}

    cassette = {
        "interactions": [
            {
                "request": {
                    "body": None,
                    "headers": {},
                    "method": "GET",
                    "uri": captured["request_url"],
                },
                "response": {
                    "body": {"string": captured["response_body"]},
                    "headers": response_headers,
                    "status": {"code": status_code, "message": reason},
                    "url": captured["request_url"],
                },
            }
        ],
        "version": 1,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(yaml.dump(cassette, default_flow_style=False, sort_keys=False))

    typer.echo(f"Cassette written: {output}")
    typer.echo(f"  URL:         {captured['request_url']}")
    typer.echo(f"  Status:      {status_code} {reason}")
    typer.echo(f"  Captured at: {captured['captured_at']}")
    typer.echo("")
    typer.echo("Review before committing — check for secrets and verify the response body")
    typer.echo("is representative of a real failure before replacing the existing respx mock.")


if __name__ == "__main__":
    app()
