from __future__ import annotations

import gzip
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.extractors._base import TransientExtractionError
from src.landing.r2 import R2LandingClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(
    bucket: str = "test-bucket",
    account_id: str = "abc123",
    access_key: str = "key-id",
    secret_key: str = "secret",
) -> MagicMock:
    settings = MagicMock()
    settings.r2_bucket_name = bucket
    settings.r2_account_id = account_id
    settings.r2_access_key_id.get_secret_value.return_value = access_key
    settings.r2_secret_access_key.get_secret_value.return_value = secret_key
    return settings


def _make_client(settings: MagicMock | None = None) -> tuple[R2LandingClient, MagicMock]:
    if settings is None:
        settings = _make_settings()
    mock_boto_client = MagicMock()
    with patch("boto3.client", return_value=mock_boto_client):
        client = R2LandingClient(settings)
    return client, mock_boto_client


# ---------------------------------------------------------------------------
# __init__ — constructor
# ---------------------------------------------------------------------------


def test_init_builds_boto3_client_with_r2_endpoint_and_auto_region() -> None:
    settings = _make_settings(account_id="myaccount")
    mock_boto_client = MagicMock()
    with patch("boto3.client", return_value=mock_boto_client) as mock_factory:
        R2LandingClient(settings)

    assert mock_factory.call_count == 1
    args, kwargs = mock_factory.call_args
    assert args == ("s3",)
    assert kwargs["endpoint_url"] == "https://myaccount.r2.cloudflarestorage.com"
    assert kwargs["aws_access_key_id"] == "key-id"
    assert kwargs["aws_secret_access_key"] == "secret"
    assert kwargs["region_name"] == "auto"
    # boto3 >=1.36 enforces S3 response-integrity checksums by default and
    # R2's implementation trips the validator; see r2.py docstring.
    assert kwargs["config"].response_checksum_validation == "when_required"


def test_init_calls_get_secret_value_on_credentials() -> None:
    settings = _make_settings()
    with patch("boto3.client"):
        R2LandingClient(settings)

    settings.r2_access_key_id.get_secret_value.assert_called_once()
    settings.r2_secret_access_key.get_secret_value.assert_called_once()


# ---------------------------------------------------------------------------
# land — happy path
# ---------------------------------------------------------------------------


def test_land_returns_key_with_correct_prefix_and_suffix() -> None:
    client, _ = _make_client()
    key = client.land("cpsc", b"data", "json", extraction_date=date(2024, 6, 1))
    assert key.startswith("cpsc/2024-06-01/")
    assert key.endswith(".json.gz")


def test_land_calls_put_object_with_gzip_compressed_content() -> None:
    client, mock_s3 = _make_client()
    content = b'{"recall_id": "1"}'
    client.land("cpsc", content, "json", extraction_date=date(2024, 6, 1))

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert gzip.decompress(call_kwargs["Body"]) == content


def test_land_calls_put_object_with_correct_bucket_and_content_type() -> None:
    client, mock_s3 = _make_client(_make_settings(bucket="my-bucket"))
    client.land("cpsc", b"data", "json", extraction_date=date(2024, 6, 1))

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "my-bucket"
    assert call_kwargs["ContentType"] == "application/json"
    # ContentEncoding must NOT be set — see r2.py docstring for rationale.
    assert "ContentEncoding" not in call_kwargs


def test_land_uses_correct_content_type_for_jsonl_suffix() -> None:
    client, mock_s3 = _make_client()
    client.land("fda", b"data", "jsonl", extraction_date=date(2024, 6, 1))
    assert mock_s3.put_object.call_args.kwargs["ContentType"] == "application/x-ndjson"


def test_land_uses_correct_content_type_for_html_suffix() -> None:
    client, mock_s3 = _make_client()
    client.land("uscg", b"data", "html", extraction_date=date(2024, 6, 1))
    assert mock_s3.put_object.call_args.kwargs["ContentType"] == "text/html"


def test_land_uses_correct_content_type_for_tsv_suffix() -> None:
    client, mock_s3 = _make_client()
    client.land("nhtsa", b"data", "tsv", extraction_date=date(2024, 6, 1))
    assert mock_s3.put_object.call_args.kwargs["ContentType"] == "text/tab-separated-values"


def test_land_uses_octet_stream_for_unknown_suffix() -> None:
    client, mock_s3 = _make_client()
    client.land("cpsc", b"data", "xml", extraction_date=date(2024, 6, 1))
    assert mock_s3.put_object.call_args.kwargs["ContentType"] == "application/octet-stream"


def test_land_defaults_extraction_date_to_today_utc() -> None:
    client, _ = _make_client()
    fixed_date = date(2025, 1, 15)
    mock_now = MagicMock()
    mock_now.return_value.date.return_value = fixed_date
    with patch("src.landing.r2.datetime") as mock_dt:
        mock_dt.now.return_value.date.return_value = fixed_date
        key = client.land("cpsc", b"data", "json")
    assert "2025-01-15" in key


def test_land_key_contains_uuid_segment() -> None:
    client, _ = _make_client()
    key = client.land("cpsc", b"data", "json", extraction_date=date(2024, 6, 1))
    # key format: cpsc/2024-06-01/<uuid>.json.gz
    parts = key.split("/")
    assert len(parts) == 3
    filename = parts[2]  # <uuid>.json.gz
    assert filename.endswith(".json.gz")
    uuid_part = filename[: -len(".json.gz")]
    import uuid

    uuid.UUID(uuid_part)  # raises ValueError if not a valid UUID


# ---------------------------------------------------------------------------
# land — error handling
# ---------------------------------------------------------------------------


def test_land_raises_transient_error_on_client_error() -> None:
    import botocore.exceptions

    client, mock_s3 = _make_client()
    mock_s3.put_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "500", "Message": "internal"}}, "PutObject"
    )
    with pytest.raises(TransientExtractionError):
        client.land("cpsc", b"data", "json", extraction_date=date(2024, 6, 1))


def test_land_raises_transient_error_on_botocore_error() -> None:
    import botocore.exceptions

    client, mock_s3 = _make_client()
    mock_s3.put_object.side_effect = botocore.exceptions.BotoCoreError()
    with pytest.raises(TransientExtractionError):
        client.land("cpsc", b"data", "json", extraction_date=date(2024, 6, 1))


# ---------------------------------------------------------------------------
# get_raw — happy path
# ---------------------------------------------------------------------------


def test_get_raw_returns_decompressed_bytes() -> None:
    client, mock_s3 = _make_client()
    original = b'{"recall_id": "X-1"}'
    compressed = gzip.compress(original)
    mock_body = MagicMock()
    mock_body.read.return_value = compressed
    mock_s3.get_object.return_value = {"Body": mock_body}

    result = client.get_raw("cpsc/2024-06-01/somefile.json.gz")
    assert result == original


def test_get_raw_calls_get_object_with_correct_bucket_and_key() -> None:
    client, mock_s3 = _make_client(_make_settings(bucket="my-bucket"))
    compressed = gzip.compress(b"data")
    mock_body = MagicMock()
    mock_body.read.return_value = compressed
    mock_s3.get_object.return_value = {"Body": mock_body}

    key = "cpsc/2024-06-01/somefile.json.gz"
    client.get_raw(key)

    mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key=key)


# ---------------------------------------------------------------------------
# get_raw — error handling
# ---------------------------------------------------------------------------


def test_get_raw_raises_transient_error_on_client_error() -> None:
    import botocore.exceptions

    client, mock_s3 = _make_client()
    mock_s3.get_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
    )
    with pytest.raises(TransientExtractionError):
        client.get_raw("cpsc/2024-06-01/missing.json.gz")


def test_get_raw_raises_transient_error_on_botocore_error() -> None:
    import botocore.exceptions

    client, mock_s3 = _make_client()
    mock_s3.get_object.side_effect = botocore.exceptions.BotoCoreError()
    with pytest.raises(TransientExtractionError):
        client.get_raw("cpsc/2024-06-01/missing.json.gz")


# ---------------------------------------------------------------------------
# land_error_response — happy path
# ---------------------------------------------------------------------------


def test_land_error_response_returns_key_in_errors_partition() -> None:
    import gzip
    import json

    client, mock_s3 = _make_client()
    key = client.land_error_response(
        source="cpsc",
        request_url="https://api.example.com/recalls",
        status_code=429,
        response_headers={"retry-after": "60"},
        response_body="Too Many Requests",
    )

    assert key.startswith("cpsc/errors/")
    assert key.endswith("_429.json.gz")

    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["ContentType"] == "application/json"
    assert "ContentEncoding" not in call_kwargs

    payload = json.loads(gzip.decompress(call_kwargs["Body"]))
    assert payload["source"] == "cpsc"
    assert payload["status_code"] == 429
    assert payload["request_url"] == "https://api.example.com/recalls"
    assert payload["response_headers"] == {"retry-after": "60"}
    assert payload["response_body"] == "Too Many Requests"
    assert "captured_at" in payload


def test_land_error_response_raises_transient_error_on_client_error() -> None:
    import botocore.exceptions

    client, mock_s3 = _make_client()
    mock_s3.put_object.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "500", "Message": "internal"}}, "PutObject"
    )
    with pytest.raises(TransientExtractionError):
        client.land_error_response(
            source="cpsc",
            request_url="https://api.example.com/recalls",
            status_code=500,
            response_headers={},
            response_body="error",
        )


def test_land_error_response_raises_transient_error_on_botocore_error() -> None:
    import botocore.exceptions

    client, mock_s3 = _make_client()
    mock_s3.put_object.side_effect = botocore.exceptions.BotoCoreError()
    with pytest.raises(TransientExtractionError):
        client.land_error_response(
            source="cpsc",
            request_url="https://api.example.com/recalls",
            status_code=503,
            response_headers={},
            response_body="error",
        )
