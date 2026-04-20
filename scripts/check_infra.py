"""
Local infrastructure connectivity check.

Verifies that Neon Postgres and Cloudflare R2 are reachable and correctly
configured before running migrations or the first live extraction.

Usage:
    uv run python scripts/check_infra.py
"""

from __future__ import annotations

import sys

import boto3
import botocore.exceptions
import sqlalchemy as sa

from src.config.settings import Settings


def check_neon(settings: Settings) -> bool:
    print("Neon Postgres...")
    try:
        engine = sa.create_engine(settings.neon_database_url.get_secret_value())
        with engine.connect() as conn:
            version = conn.execute(sa.text("SELECT version()")).scalar()
        print(f"  OK — {str(version)[:60]}")
        return True
    except Exception as exc:
        print(f"  FAIL — {exc}")
        return False


def check_r2(settings: Settings) -> bool:
    print("Cloudflare R2...")
    try:
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=settings.r2_access_key_id.get_secret_value(),
            aws_secret_access_key=settings.r2_secret_access_key.get_secret_value(),
            region_name="auto",
        )
        client.head_bucket(Bucket=settings.r2_bucket_name)
        print(f"  OK — bucket '{settings.r2_bucket_name}' accessible")
        return True
    except botocore.exceptions.ClientError as exc:
        code = exc.response["Error"]["Code"]
        print(f"  FAIL — {code}: {exc}")
        return False
    except Exception as exc:
        print(f"  FAIL — {exc}")
        return False


def main() -> None:
    print("Loading settings from .env...\n")
    try:
        settings = Settings()  # type: ignore[call-arg]
    except Exception as exc:
        print(f"Settings failed to load — missing env vars?\n  {exc}")
        sys.exit(1)

    results = [
        check_neon(settings),
        check_r2(settings),
    ]

    print()
    if all(results):
        print("All checks passed. Ready to run migrations and extract.")
    else:
        print("One or more checks failed. Fix the issues above before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()
