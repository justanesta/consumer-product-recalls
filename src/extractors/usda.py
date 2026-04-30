from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
from pydantic import PrivateAttr, ValidationError

from src.bronze.invariants import (
    check_date_sanity,
    check_null_source_id,
    check_usda_bilingual_pairing,
)
from src.bronze.loader import BronzeLoader
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    ExtractionResult,
    QuarantineRecord,
    RateLimitError,
    RestApiExtractor,
    TransientExtractionError,
)
from src.landing.r2 import R2LandingClient
from src.schemas.usda import UsdaFsisRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_usda_bronze = sa.Table(
    "usda_fsis_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("langcode", sa.Text),
    sa.Column("title", sa.Text),
    sa.Column("recall_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("recall_type", sa.Text),
    sa.Column("recall_classification", sa.Text),
    sa.Column("archive_recall", sa.Boolean),
    sa.Column("has_spanish", sa.Boolean),
    sa.Column("active_notice", sa.Boolean),
    sa.Column("last_modified_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("closed_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("related_to_outbreak", sa.Boolean),
    sa.Column("closed_year", sa.Text),
    sa.Column("year", sa.Text),
    sa.Column("risk_level", sa.Text),
    sa.Column("recall_reason", sa.Text),
    sa.Column("processing", sa.Text),
    sa.Column("states", sa.Text),
    sa.Column("establishment", sa.Text),
    sa.Column("labels", sa.Text),
    sa.Column("qty_recovered", sa.Text),
    sa.Column("summary", sa.Text),
    sa.Column("product_items", sa.Text),
    sa.Column("distro_list", sa.Text),
    sa.Column("media_contact", sa.Text),
    sa.Column("company_media_contact", sa.Text),
    sa.Column("recall_url", sa.Text),
    sa.Column("en_press_release", sa.Text),
    sa.Column("press_release", sa.Text),
)

_usda_rejected = sa.Table(
    "usda_fsis_recalls_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

_source_watermarks = sa.Table(
    "source_watermarks",
    _metadata,
    sa.Column("source", sa.Text, primary_key=True),
    sa.Column("last_cursor", sa.Text),
    sa.Column("last_etag", sa.Text),
    sa.Column("last_successful_extract_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),
)

_extraction_runs = sa.Table(
    "extraction_runs",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source", sa.Text),
    sa.Column("started_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("finished_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("status", sa.Text),
    sa.Column("records_extracted", sa.Integer),
    sa.Column("records_inserted", sa.Integer),
    sa.Column("records_rejected", sa.Integer),
    sa.Column("run_id", sa.Text),
    sa.Column("error_message", sa.Text),
    sa.Column("raw_landing_path", sa.Text),
)

_USDA_SOURCE = "usda"

# Guard ceiling for the incremental path. Full dataset is ~2,001 records (Finding B);
# 5_000 leaves a wide margin for organic growth while still catching a runaway bug
# (e.g., the API starting to return paginated results we don't handle, or some other
# upstream change ballooning the dataset). Not applied on the deep-rescan path.
_MAX_INCREMENTAL_RECORDS = 5_000

# Hash exclusions: en_press_release is 100% empty (Finding C — dead field) and
# press_release is 99.9% empty. If FSIS ever populates these, we don't want their
# transition to drive a full bronze rewrite of every existing record.
_HASH_EXCLUDE = frozenset({"en_press_release", "press_release"})


class UsdaFsisExtractionResult(ExtractionResult):
    """Marker for type-narrowing only; behavior identical to ExtractionResult."""


class UsdaExtractor(RestApiExtractor[UsdaFsisRecord]):
    """
    Extractor for USDA FSIS recall records — incremental path.

    Strategy: full-dump every run. Finding D confirmed both
    `field_last_modified_date_value` and `field_last_modified_date` query parameters
    are silently ignored — there is no working server-side filter. The full ~2,001-
    record dataset is returned in one flat JSON array; idempotency is handled by the
    bronze content-hash loader (ADR 0007).

    ETag optimization (Finding A — cache-control: public, max-age=3100): the extractor
    reads `source_watermarks.last_etag`, sends `If-None-Match` on every request when
    populated, and short-circuits cleanly on 304 (skipping the ~12 MB download and
    skipping the bronze write). A contradiction guard fails the run if a 304 is paired
    with a `last-modified` header that has advanced past the prior recorded value.
    Disable by setting `etag_enabled=False` (or by manually nulling
    source_watermarks.last_etag for the usda row).

    For historical loads / forced re-ingestion use `UsdaDeepRescanLoader`, which never
    sends `If-None-Match` and never updates the watermark — see its docstring.
    """

    source_name: str = _USDA_SOURCE
    settings: Settings
    etag_enabled: bool = True

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")
    # Captured during extract() and applied during load_bronze() in the same txn (ADR 0020).
    _captured_etag: str | None = PrivateAttr(default=None)
    _captured_last_modified: str | None = PrivateAttr(default=None)
    # Set when extract() short-circuits on a 304; downstream lifecycle steps no-op.
    _not_modified: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        self._engine = sa.create_engine(
            self.settings.neon_database_url.get_secret_value(),
            pool_pre_ping=True,
        )
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch all USDA FSIS recall records.

        Returns [] on a 304 Not Modified (and sets _not_modified so downstream
        steps no-op). Raises TransientExtractionError on 5xx / network /
        oversized response. Raises ExtractionError (no retry) on the
        contradiction guard (304 paired with advanced last-modified).
        """
        prior_etag, prior_last_modified = self._read_etag_state()
        records, status_code, etag, last_modified = self._fetch(prior_etag, prior_last_modified)

        if status_code == 304:
            self._not_modified = True
            logger.info(
                "usda.extract.not_modified",
                etag=prior_etag,
                last_modified_header=last_modified,
            )
            self._guard_etag_contradiction(prior_last_modified, last_modified)
            return []

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"USDA incremental query returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. "
                "Possible cause: upstream dataset size change or API shape drift."
            )

        # Stash captured headers for atomic write in load_bronze().
        self._captured_etag = etag
        self._captured_last_modified = last_modified
        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        if self._not_modified:
            # Nothing to land; skip R2 write. Empty path string is a no-op marker
            # consumed by load_bronze() and by quarantine routing (which has no
            # records to route on a 304 path).
            self._current_landing_path = ""
            return ""
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_USDA_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UsdaFsisRecord], list[QuarantineRecord]]:
        valid: list[UsdaFsisRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UsdaFsisRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("field_recall_number")) or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UsdaFsisRecord]
    ) -> tuple[list[UsdaFsisRecord], list[QuarantineRecord]]:
        # Run per-record invariants first (null id, date sanity).
        post_basic: list[UsdaFsisRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = check_null_source_id(record.source_recall_id) or check_date_sanity(
                record.recall_date, "recall_date"
            )
            if failure:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=record.source_recall_id,
                        raw_record=record.model_dump(mode="json"),
                        failure_reason=failure,
                        failure_stage="invariants",
                        raw_landing_path=self._current_landing_path,
                    )
                )
            else:
                post_basic.append(record)

        # Bilingual pairing invariant: Spanish records without an English sibling are
        # quarantined (ADR 0006). The shared invariant function lives in src/bronze/invariants.py
        # and was scaffolded in Phase 2 for exactly this site.
        passing, bilingual_rejects = check_usda_bilingual_pairing(
            post_basic,
            recall_number_fn=lambda r: r.source_recall_id,
            is_spanish_fn=lambda r: r.langcode == "Spanish",
            raw_landing_path=self._current_landing_path,
        )
        quarantined.extend(bilingual_rejects)
        return passing, quarantined

    def load_bronze(
        self,
        records: list[UsdaFsisRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        if self._not_modified:
            # 304 path: no records, no quarantine, but we DO advance
            # last_successful_extract_at so monitoring sees the run as fresh.
            with self._engine.begin() as conn:
                self._touch_freshness(conn)
            return 0

        loader = BronzeLoader(
            bronze_table=_usda_bronze,
            rejected_table=_usda_rejected,
            hash_exclude_fields=_HASH_EXCLUDE,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._update_watermark_state(
                conn,
                records=records,
                etag=self._captured_etag,
                last_modified=self._captured_last_modified,
            )
        return count

    # --- Private helpers ---

    def _fetch(
        self,
        prior_etag: str | None,
        prior_last_modified: str | None,
    ) -> tuple[list[dict[str, Any]], int, str | None, str | None]:
        """
        Single GET to the FSIS recall endpoint.

        Returns (records, status_code, etag, last_modified).
        - 200: records is the full payload list, etag/last_modified from response headers.
        - 304: records is [], headers may be present.
        Raises TransientExtractionError on 5xx and network errors.
        Raises RateLimitError on 429.
        Raises AuthenticationError on 401/403 (unexpected — this API has no auth).
        """
        headers: dict[str, str] = {}
        if self.etag_enabled and prior_etag:
            headers["If-None-Match"] = prior_etag
        if self.etag_enabled and prior_last_modified:
            headers["If-Modified-Since"] = prior_last_modified

        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(self.base_url, headers=headers)
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"USDA network error: {exc}") from exc

        etag = response.headers.get("etag") or response.headers.get("ETag")
        last_modified = response.headers.get("last-modified") or response.headers.get(
            "Last-Modified"
        )

        if response.status_code == 304:
            return [], 304, etag, last_modified
        if response.status_code == 200:
            data = response.json()
            records = data if isinstance(data, list) else []
            return records, 200, etag, last_modified
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            self._capture_error_response(response)
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(
                f"USDA API returned {response.status_code} (unexpected — no auth required)"
            )
        self._capture_error_response(response)
        raise TransientExtractionError(f"USDA API returned {response.status_code}")

    def _guard_etag_contradiction(
        self,
        prior_last_modified: str | None,
        current_last_modified: str | None,
    ) -> None:
        """
        Fail the run if a 304 is paired with a last-modified header that advanced past
        the prior recorded value. That combination indicates the server (or CDN cache
        layer) is returning a stale-positive 304 — the etag matched but the underlying
        dataset has actually changed. Retrying would not help; the watermark needs
        manual repair (null out source_watermarks.last_etag and re-run).
        """
        if not (prior_last_modified and current_last_modified):
            return
        if prior_last_modified == current_last_modified:
            return
        # Headers differ — could be a clock-skew artifact. Compare parsed datetimes
        # to be more tolerant; if parsing fails, treat the inequality as suspicious
        # and raise.
        try:
            prior_dt = _parse_http_date(prior_last_modified)
            current_dt = _parse_http_date(current_last_modified)
        except ValueError:
            raise ExtractionError(
                "USDA contradiction guard: 304 returned with advanced last-modified "
                f"header (prior={prior_last_modified!r}, current={current_last_modified!r}). "
                "Could not parse dates; treating as a stale-positive ETag. "
                "Manually NULL source_watermarks.last_etag for usda and re-run."
            ) from None
        if current_dt > prior_dt:
            raise ExtractionError(
                "USDA contradiction guard: 304 Not Modified returned but last-modified "
                f"header advanced from {prior_last_modified!r} to {current_last_modified!r}. "
                "This is a server-side stale-positive ETag — the cached etag matched but "
                "the underlying dataset has changed. Manually NULL source_watermarks.last_etag "
                "for usda and re-run to force a full payload fetch."
            )

    def _capture_error_response(self, response: httpx.Response) -> None:
        try:
            self._r2_client.land_error_response(
                source=_USDA_SOURCE,
                request_method=response.request.method,
                request_url=str(response.request.url),
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                "usda.error_capture_failed",
                status_code=response.status_code,
            )

    def _read_etag_state(self) -> tuple[str | None, str | None]:
        """Return (prior_etag, prior_last_modified) from source_watermarks."""
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(
                    _source_watermarks.c.last_etag,
                    _source_watermarks.c.last_cursor,
                ).where(_source_watermarks.c.source == _USDA_SOURCE)
            ).fetchone()
        if not row:
            return None, None
        # last_cursor is repurposed for the prior last-modified header value (HTTP-date string).
        # USDA has no usable date watermark per Finding D, so last_cursor is unused as a query
        # parameter — using it here as a cache-validator companion to last_etag is the cleanest
        # repurpose without adding a new column.
        return row[0], row[1]

    def _update_watermark_state(
        self,
        conn: sa.Connection,
        *,
        records: list[UsdaFsisRecord],
        etag: str | None,
        last_modified: str | None,
    ) -> None:
        """Update last_etag, last_cursor (= last_modified header), last_successful_extract_at."""
        values: dict[str, Any] = {
            "updated_at": datetime.now(UTC),
            "last_successful_extract_at": datetime.now(UTC),
        }
        if etag is not None:
            values["last_etag"] = etag
        if last_modified is not None:
            # last_cursor stores the prior response's last-modified header for use as
            # If-Modified-Since on the next run. See _read_etag_state for rationale.
            values["last_cursor"] = last_modified
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _USDA_SOURCE)
            .values(**values)
        )

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump last_successful_extract_at on a 304 path without modifying etag/cursor."""
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _USDA_SOURCE)
            .values(
                last_successful_extract_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )

    def _record_run(
        self,
        run_id: str,
        started_at: datetime,
        status: str,
        result: ExtractionResult | None = None,
        error_message: str | None = None,
    ) -> None:
        row: dict[str, Any] = {
            "source": _USDA_SOURCE,
            "started_at": started_at,
            "finished_at": datetime.now(UTC),
            "status": status,
            "run_id": run_id,
            "error_message": error_message,
        }
        if result is not None:
            row["records_extracted"] = result.records_fetched
            row["records_inserted"] = result.records_loaded
            row["records_rejected"] = (
                result.records_rejected_validate + result.records_rejected_invariants
            )
            row["raw_landing_path"] = result.raw_landing_path
        try:
            with self._engine.begin() as conn:
                conn.execute(_extraction_runs.insert().values(**row))
        except Exception:
            logger.warning("extraction_run.record_failed", run_id=run_id, status=status)


def _parse_http_date(s: str) -> datetime:
    """Parse an RFC 7231 IMF-fixdate header value (e.g. 'Wed, 29 Apr 2026 14:29:36 GMT')."""
    return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=UTC)


class UsdaDeepRescanLoader(UsdaExtractor):
    """
    Historical / deep-rescan loader for USDA FSIS records.

    USDA's API has no working server-side filter (Finding D), so the deep-rescan path
    fetches the same full-dump response shape as the incremental path. The two
    behaviors that differ from `UsdaExtractor`:

    1. **Never sends `If-None-Match`** — even when source_watermarks.last_etag is
       populated. The deep-rescan workflow exists to re-pull the full payload
       unconditionally, so any silent ETag-bug self-corrects within ≤7 days
       (the cron cadence of deep-rescan-usda.yml in Phase 7).
    2. **Never updates source_watermarks** — the incremental extractor owns the
       watermark and ETag exclusively. Deep rescan is purely additive to the bronze
       table.

    Used by the deep-rescan-usda.yml GitHub Actions workflow.
    """

    # Force-disable ETag handling for this subclass regardless of config.
    etag_enabled: bool = False

    def load_bronze(
        self,
        records: list[UsdaFsisRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        # Does NOT touch source_watermarks — the incremental extractor owns it.
        loader = BronzeLoader(
            bronze_table=_usda_bronze,
            rejected_table=_usda_rejected,
            hash_exclude_fields=_HASH_EXCLUDE,
        )
        with self._engine.begin() as conn:
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
