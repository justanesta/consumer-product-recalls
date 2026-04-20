from __future__ import annotations

import gzip
import uuid
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
import structlog

from src.extractors._base import TransientExtractionError

if TYPE_CHECKING:
    from src.config.settings import Settings

logger = structlog.get_logger()

# Maps file suffix to MIME type for the Content-Type header.
_CONTENT_TYPE: dict[str, str] = {
    "json": "application/json",
    "jsonl": "application/x-ndjson",
    "html": "text/html",
    "tsv": "text/tab-separated-values",
}


class R2LandingClient:
    """
    Boto3 S3-compatible client for Cloudflare R2 raw payload landing (Layer 0).

    All objects are gzip-compressed and partitioned as:
        {source}/{YYYY-MM-DD}/{uuid}.{suffix}.gz

    Partitioning matches ADR 0004's "source/extraction_date/" convention.
    The returned object key is stored as raw_landing_path in QuarantineRecord
    and in bronze row metadata so every value can be traced back to its raw file.
    """

    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.r2_bucket_name
        self._client: Any = boto3.client(
            "s3",
            endpoint_url=f"https://{settings.r2_account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=settings.r2_access_key_id.get_secret_value(),
            aws_secret_access_key=settings.r2_secret_access_key.get_secret_value(),
            region_name="auto",
        )

    def land(
        self,
        source: str,
        content: bytes,
        suffix: str,
        extraction_date: date | None = None,
    ) -> str:
        """
        Write a raw payload to R2, gzip-compressed.

        Args:
            source: Source identifier used as the top-level partition key (e.g. "cpsc").
            content: Raw bytes to store (JSON, HTML, TSV, etc.).
            suffix: File extension without the dot: "json", "jsonl", "html", or "tsv".
            extraction_date: Partition date; defaults to today UTC if not provided.

        Returns:
            The R2 object key, suitable for use as raw_landing_path.

        Raises:
            TransientExtractionError: Wraps any boto3/R2 error so the R2 retry
                policy in the Extractor lifecycle can catch and retry it.
        """
        if extraction_date is None:
            extraction_date = datetime.now(UTC).date()

        key = f"{source}/{extraction_date.isoformat()}/{uuid.uuid4()}.{suffix}.gz"
        content_type = _CONTENT_TYPE.get(suffix, "application/octet-stream")
        compressed = gzip.compress(content)

        log = logger.bind(bucket=self._bucket, key=key)
        log.debug(
            "r2.land.started",
            raw_bytes=len(content),
            compressed_bytes=len(compressed),
        )

        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=compressed,
                ContentType=content_type,
                ContentEncoding="gzip",
            )
        except botocore.exceptions.ClientError as exc:
            raise TransientExtractionError(f"R2 put_object failed: {exc}") from exc
        except botocore.exceptions.BotoCoreError as exc:
            raise TransientExtractionError(f"R2 connection error: {exc}") from exc

        log.debug("r2.land.completed")
        return key

    def get_raw(self, key: str) -> bytes:
        """
        Retrieve and decompress a previously landed object.
        Used by the re-ingest path (Phase 6) to replay raw landing artifacts.

        Raises:
            TransientExtractionError: Wraps boto3/R2 errors.
        """
        log = logger.bind(bucket=self._bucket, key=key)
        log.debug("r2.get_raw.started")

        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            compressed: bytes = response["Body"].read()
        except botocore.exceptions.ClientError as exc:
            raise TransientExtractionError(f"R2 get_object failed: {exc}") from exc
        except botocore.exceptions.BotoCoreError as exc:
            raise TransientExtractionError(f"R2 connection error: {exc}") from exc

        raw = gzip.decompress(compressed)
        log.debug("r2.get_raw.completed", raw_bytes=len(raw))
        return raw
