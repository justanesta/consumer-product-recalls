"""USDA FSIS Establishment Listing extractor (Phase 5b.2).

Targets ``GET /fsis/api/establishments/v/1`` — a flat-array endpoint returning
all 7,945 FSIS-regulated establishments in one response. Per Findings A–G in
``documentation/usda/establishment_api_observations.md``:

- **No pagination; no incremental cursor.** The full dataset returns in one
  flat JSON array; idempotency is handled by the bronze content-hash loader
  (ADR 0007). The ETag / Last-Modified conditional-GET path (below) saves the
  download but doesn't replace the bronze hash dedup as the source of truth
  for "did the data change?".
- **ETag conditional-GET, scaffolded but disabled by default.** Finding A
  revision (2026-05-03) confirmed the API emits ``ETag`` and ``Last-Modified``
  under browser fingerprint, mirroring the recall endpoint. This extractor's
  ``_fetch`` / ``_read_etag_state`` / ``_update_watermark_state`` /
  ``_guard_etag_contradiction`` / ``_touch_freshness`` helpers are 1:1 mirrors
  of the corresponding methods on ``UsdaExtractor`` — bug fixes here likely
  apply there and vice versa. Default ``etag_enabled=False`` until viability
  data accumulated in ``extraction_runs.response_*`` (migration 0010) clears
  the gate at ``scripts/sql/_pipeline/etag_viability.sql``.
- **No incremental vs historical split.** The "incremental vs historical load
  paths" architectural standing requirement (implementation_plan.md Phase 5
  preamble) is moot here — there is no cursor to advance and no count guard
  is meaningful at the incremental level. A single ``_MAX_TOTAL_RECORDS``
  guard catches an upstream shape change.
- **No deep-rescan workflow.** Same reason — every run is functionally a
  deep rescan.
- **Akamai Bot Manager** protects the same hostname as the recall API
  (Finding O on the recall side); browser-like headers are reused via
  ``src/extractors/_fsis_headers.browser_headers``.

The motivating downstream use case is enrichment of USDA recall events:
``stg_usda_fsis_recalls.establishment`` joins to ``establishment_name`` (with
``dbas`` array fallback) on normalized name, attaching ``establishment_id``
(stable FSIS FK), address, geolocation, FIPS, and active-MPI status. That
silver join lands in Phase 5b.2 Step 5.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.invariants import check_null_source_id
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
from src.extractors._fsis_headers import browser_headers
from src.landing.r2 import R2LandingClient
from src.schemas.usda_establishment import UsdaFsisEstablishment

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_establishments_bronze = sa.Table(
    "usda_fsis_establishments_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("establishment_name", sa.Text),
    sa.Column("establishment_number", sa.Text),
    sa.Column("address", sa.Text),
    sa.Column("city", sa.Text),
    sa.Column("state", sa.Text),
    sa.Column("zip", sa.Text),
    sa.Column("latest_mpi_active_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("status_regulated_est", sa.Text),
    sa.Column("activities", postgresql.JSONB),
    sa.Column("dbas", postgresql.JSONB),
    sa.Column("phone", sa.Text),
    sa.Column("duns_number", sa.Text),
    sa.Column("county", sa.Text),
    sa.Column("fips_code", sa.Text),
    sa.Column("geolocation", sa.Text),
    sa.Column("grant_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("size", sa.Text),
    sa.Column("district", sa.Text),
    sa.Column("circuit", sa.Text),
)

_establishments_rejected = sa.Table(
    "usda_fsis_establishments_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
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
    sa.Column("change_type", sa.Text),
    sa.Column("response_status_code", sa.Integer),
    sa.Column("response_etag", sa.Text),
    sa.Column("response_last_modified", sa.Text),
    sa.Column("response_body_sha256", sa.Text),
    sa.Column("response_headers", postgresql.JSONB),
)

# Mirror of the source_watermarks declaration in src/extractors/usda.py — keep
# in sync. The conditional-GET path below reads/writes (last_etag, last_cursor)
# for this source's row; last_cursor is repurposed to store the prior response's
# Last-Modified header value (no usable date watermark, same rationale as the
# recall endpoint per Finding D).
_source_watermarks = sa.Table(
    "source_watermarks",
    _metadata,
    sa.Column("source", sa.Text, primary_key=True),
    sa.Column("last_cursor", sa.Text),
    sa.Column("last_etag", sa.Text),
    sa.Column("last_successful_extract_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),
)

_SOURCE = "usda_establishments"

# Sanity guard: current dataset is 7,945 records (Finding B). 20,000 gives
# ~2.5x headroom against organic growth while still catching a runaway
# upstream shape change (e.g., a sibling endpoint silently merging in).
_MAX_TOTAL_RECORDS = 20_000


class UsdaEstablishmentExtractor(RestApiExtractor[UsdaFsisEstablishment]):
    """Full-dump extractor for the FSIS Establishment Listing API.

    ETag conditional-GET pattern is scaffolded as a 1:1 mirror of
    ``UsdaExtractor`` (recall side). When ``etag_enabled=True``, the extractor
    reads ``source_watermarks.last_etag`` / ``last_cursor`` (= prior
    Last-Modified), sends ``If-None-Match`` and ``If-Modified-Since``, and
    short-circuits cleanly on 304 (skipping the ~810 KB download and the
    bronze write). A contradiction guard fails the run if a 304 is paired with
    a ``last-modified`` header that has advanced past the prior recorded
    value. Default is OFF until viability data clears the gate at
    ``scripts/sql/_pipeline/etag_viability.sql``.
    """

    source_name: str = _SOURCE
    settings: Settings
    # Mirrors UsdaExtractor.etag_enabled; default OFF for the same reason —
    # multi-day cross-fingerprint evidence is needed before depending on
    # Akamai-served ETags. Flip to True (or pass explicitly via constructor)
    # once etag_viability.sql gives the green light for this source.
    etag_enabled: bool = False

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")
    # Mirrors UsdaExtractor: captured during extract(), applied during
    # load_bronze() in the same transaction (ADR 0020). Distinct from the
    # base-class _captured_response_* state (migration 0010), which is used
    # for forensic logging on extraction_runs; these drive the watermark
    # write specifically.
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
        """Single GET to the establishments endpoint.

        Returns [] on a 304 Not Modified (and sets _not_modified so downstream
        lifecycle steps no-op). Raises TransientExtractionError on 5xx /
        network / oversized response. Raises ExtractionError (no retry) on
        the contradiction guard (304 paired with advanced last-modified).
        """
        prior_etag, prior_last_modified = self._read_etag_state()
        records, status_code, etag, last_modified = self._fetch(prior_etag, prior_last_modified)

        if status_code == 304:
            self._not_modified = True
            logger.info(
                "usda_establishments.extract.not_modified",
                etag=prior_etag,
                last_modified_header=last_modified,
            )
            self._guard_etag_contradiction(prior_last_modified, last_modified)
            return []

        if len(records) > _MAX_TOTAL_RECORDS:
            raise TransientExtractionError(
                f"USDA establishments query returned {len(records)} records — "
                f"exceeds guard of {_MAX_TOTAL_RECORDS}. "
                "Possible cause: upstream dataset shape change."
            )

        # Stash captured headers for atomic write in load_bronze().
        self._captured_etag = etag
        self._captured_last_modified = last_modified
        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        if self._not_modified:
            # Nothing to land; skip R2 write. Empty path string is a no-op
            # marker consumed by load_bronze() and by quarantine routing
            # (which has no records to route on a 304 path).
            self._current_landing_path = ""
            return ""
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UsdaFsisEstablishment], list[QuarantineRecord]]:
        valid: list[UsdaFsisEstablishment] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UsdaFsisEstablishment.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("establishment_id") or "<unknown>"),
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UsdaFsisEstablishment]
    ) -> tuple[list[UsdaFsisEstablishment], list[QuarantineRecord]]:
        # Only the null-id check applies. No date_sanity: latest_mpi_active_date
        # is administrative, not a publication timestamp; an FSIS dataset
        # re-baseline could legitimately reset it. No bilingual pairing.
        passing: list[UsdaFsisEstablishment] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = check_null_source_id(record.source_recall_id)
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
                passing.append(record)
        return passing, quarantined

    def load_bronze(
        self,
        records: list[UsdaFsisEstablishment],
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
            bronze_table=_establishments_bronze,
            rejected_table=_establishments_rejected,
            # establishment_id is the stable FSIS FK (Finding F) and never
            # has bilingual siblings or other composite components.
            identity_fields=("source_recall_id",),
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._update_watermark_state(
                conn,
                etag=self._captured_etag,
                last_modified=self._captured_last_modified,
            )
        return count

    # --- Private helpers ---

    def _fetch(
        self,
        prior_etag: str | None = None,
        prior_last_modified: str | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None, str | None]:
        """Single GET to the establishments endpoint.

        Mirrors UsdaExtractor._fetch — keep in sync.

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
            with httpx.Client(
                timeout=self.timeout_seconds,
                headers=browser_headers(),
            ) as client:
                response = client.get(self.base_url, headers=headers)
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"USDA establishments network error: {exc}") from exc

        etag = response.headers.get("etag") or response.headers.get("ETag")
        last_modified = response.headers.get("last-modified") or response.headers.get(
            "Last-Modified"
        )

        if response.status_code == 304:
            self._capture_response(response)
            return [], 304, etag, last_modified
        if response.status_code == 200:
            self._capture_response(response)
            data = response.json()
            records = data if isinstance(data, list) else []
            return records, 200, etag, last_modified
        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            self._capture_error_response(response)
            raise RateLimitError(retry_after=retry_after)
        if response.status_code in (401, 403):
            raise AuthenticationError(
                f"USDA establishments API returned {response.status_code} "
                "(unexpected — no auth required)"
            )
        self._capture_error_response(response)
        raise TransientExtractionError(f"USDA establishments API returned {response.status_code}")

    def _capture_error_response(self, response: httpx.Response) -> None:
        try:
            self._r2_client.land_error_response(
                source=_SOURCE,
                request_method=response.request.method,
                request_url=str(response.request.url),
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                "usda_establishments.error_capture_failed",
                status_code=response.status_code,
            )

    def _read_etag_state(self) -> tuple[str | None, str | None]:
        """Return (prior_etag, prior_last_modified) from source_watermarks.

        Mirrors UsdaExtractor._read_etag_state — keep in sync. last_cursor is
        repurposed for the prior last-modified header value (HTTP-date string);
        no usable date watermark exists for either USDA endpoint.
        """
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(
                    _source_watermarks.c.last_etag,
                    _source_watermarks.c.last_cursor,
                ).where(_source_watermarks.c.source == _SOURCE)
            ).fetchone()
        if not row:
            return None, None
        return row[0], row[1]

    def _update_watermark_state(
        self,
        conn: sa.Connection,
        *,
        etag: str | None,
        last_modified: str | None,
    ) -> None:
        """Update last_etag, last_cursor (= last_modified header), last_successful_extract_at.

        Mirrors UsdaExtractor._update_watermark_state — keep in sync.
        """
        values: dict[str, Any] = {
            "updated_at": datetime.now(UTC),
            "last_successful_extract_at": datetime.now(UTC),
        }
        if etag is not None:
            values["last_etag"] = etag
        if last_modified is not None:
            values["last_cursor"] = last_modified
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _SOURCE)
            .values(**values)
        )

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump last_successful_extract_at on a 304 path without modifying etag/cursor.

        Mirrors UsdaExtractor._touch_freshness — keep in sync.
        """
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _SOURCE)
            .values(
                last_successful_extract_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )

    def _guard_etag_contradiction(
        self,
        prior_last_modified: str | None,
        current_last_modified: str | None,
    ) -> None:
        """Fail the run if a 304 is paired with an advanced last-modified header.

        Mirrors UsdaExtractor._guard_etag_contradiction — keep in sync. That
        combination indicates the server (or CDN cache layer) is returning a
        stale-positive 304 — the etag matched but the underlying dataset has
        actually changed. Retrying would not help; the watermark needs manual
        repair (null out source_watermarks.last_etag and re-run).
        """
        if not (prior_last_modified and current_last_modified):
            return
        if prior_last_modified == current_last_modified:
            return
        # Headers differ — could be a clock-skew artifact. Compare parsed
        # datetimes to be more tolerant; if parsing fails, treat the
        # inequality as suspicious and raise.
        try:
            prior_dt = datetime.strptime(prior_last_modified, "%a, %d %b %Y %H:%M:%S GMT").replace(
                tzinfo=UTC
            )
            current_dt = datetime.strptime(
                current_last_modified, "%a, %d %b %Y %H:%M:%S GMT"
            ).replace(tzinfo=UTC)
        except ValueError:
            raise ExtractionError(
                "USDA establishments contradiction guard: 304 returned with advanced "
                f"last-modified header (prior={prior_last_modified!r}, "
                f"current={current_last_modified!r}). Could not parse dates; treating "
                "as a stale-positive ETag. Manually NULL source_watermarks.last_etag "
                "for usda_establishments and re-run."
            ) from None
        if current_dt > prior_dt:
            raise ExtractionError(
                "USDA establishments contradiction guard: 304 Not Modified returned but "
                f"last-modified header advanced from {prior_last_modified!r} to "
                f"{current_last_modified!r}. This is a server-side stale-positive ETag — "
                "the cached etag matched but the underlying dataset has changed. Manually "
                "NULL source_watermarks.last_etag for usda_establishments and re-run to "
                "force a full payload fetch."
            )

    def _record_run(
        self,
        run_id: str,
        started_at: datetime,
        status: str,
        result: ExtractionResult | None = None,
        error_message: str | None = None,
        change_type: str = "routine",
    ) -> None:
        row: dict[str, Any] = {
            "source": _SOURCE,
            "started_at": started_at,
            "finished_at": datetime.now(UTC),
            "status": status,
            "run_id": run_id,
            "error_message": error_message,
            "change_type": change_type,
        }
        if result is not None:
            row["records_extracted"] = result.records_fetched
            row["records_inserted"] = result.records_loaded
            row["records_rejected"] = (
                result.records_rejected_validate + result.records_rejected_invariants
            )
            row["raw_landing_path"] = result.raw_landing_path
        if self._captured_response_status_code is not None:
            row["response_status_code"] = self._captured_response_status_code
            row["response_etag"] = self._captured_response_etag
            row["response_last_modified"] = self._captured_response_last_modified
            row["response_body_sha256"] = self._captured_response_body_sha256
            row["response_headers"] = self._captured_response_headers
        try:
            with self._engine.begin() as conn:
                conn.execute(_extraction_runs.insert().values(**row))
        except Exception as exc:
            # Run-recording is best-effort: the bronze write already committed,
            # so a failure here doesn't lose data. Include the exception type
            # and message so a constraint violation (e.g., missing FK row in
            # source_watermarks for a new source) is diagnosable from logs
            # rather than requiring code-side instrumentation to reproduce.
            logger.warning(
                "extraction_run.record_failed",
                run_id=run_id,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )
