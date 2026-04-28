from __future__ import annotations

import json
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.invariants import check_date_sanity, check_null_source_id
from src.bronze.loader import BronzeLoader
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    QuarantineRecord,
    RateLimitError,
    RestApiExtractor,
    TransientExtractionError,
)
from src.landing.r2 import R2LandingClient
from src.schemas.fda import FdaRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_fda_bronze = sa.Table(
    "fda_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("recall_event_id", sa.BigInteger),
    sa.Column("rid", sa.Integer),
    sa.Column("center_cd", sa.Text),
    sa.Column("product_type_short", sa.Text),
    sa.Column("event_lmd", sa.TIMESTAMP(timezone=True)),
    sa.Column("firm_legal_nam", sa.Text),
    sa.Column("firm_fei_num", sa.BigInteger),
    sa.Column("recall_num", sa.Text),
    sa.Column("phase_txt", sa.Text),
    sa.Column("center_classification_type_txt", sa.Text),
    sa.Column("recall_initiation_dt", sa.TIMESTAMP(timezone=True)),
    sa.Column("center_classification_dt", sa.TIMESTAMP(timezone=True)),
    sa.Column("termination_dt", sa.TIMESTAMP(timezone=True)),
    sa.Column("enforcement_report_dt", sa.TIMESTAMP(timezone=True)),
    sa.Column("determination_dt", sa.TIMESTAMP(timezone=True)),
    sa.Column("initial_firm_notification_txt", sa.Text),
    sa.Column("distribution_area_summary_txt", sa.Text),
    sa.Column("voluntary_type_txt", sa.Text),
    sa.Column("product_description_txt", sa.Text),
    sa.Column("product_short_reason_txt", sa.Text),
    sa.Column("product_distributed_quantity", sa.Text),
)

_fda_rejected = sa.Table(
    "fda_recalls_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", postgresql.JSONB),
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
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),
)

_FDA_SOURCE = "fda"
_DEFAULT_LOOKBACK_DAYS = 1
_PAGE_SIZE = 5_000

# displaycolumns sent to every bulk POST request. Matches the empirically-validated
# column set from bruno/fda/incremental_extraction/post_recalls_eventlmd_range.yml.
# codeinformation is excluded so the 5000-row page limit applies (not 2500).
# productlmd is excluded — not available in bulk POST displaycolumns (finding K0).
_DISPLAY_COLUMNS = (
    "recalleventid,productid,producttypeshort,recallnum,phasetxt,centercd,"
    "centerclassificationtypetxt,firmlegalnam,firmfeinum,recallinitiationdt,"
    "centerclassificationdt,terminationdt,enforcementreportdt,determinationdt,"
    "initialfirmnotificationtxt,distributionareasummarytxt,voluntarytypetxt,"
    "productdescriptiontxt,productshortreasontxt,productdistributedquantity,eventlmd"
)

# Guard ceiling for the incremental path. Daily delta is ~20-300 records; archive
# migration storms may push ~300/day. 5000 allows a wide safety margin while still
# catching a watermark bug that silently returns the full ~134K-record dataset.
# Not applied on the historical load path (FdaDeepRescanLoader).
_MAX_INCREMENTAL_RECORDS = 5_000

_RECALLS_ENDPOINT = "/recalls/"

# FDA's own iRES API documentation (Python sample code) sets this exact User-Agent.
# Sending the default `python-httpx/X.Y.Z` value is suspected to trigger FDA's
# anti-abuse throttle on the very first request — finding N in api_observations.md.
_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# FDA STATUSCODE semantics (finding A / finding K / finding K extension):
_STATUS_SUCCESS = 400  # bulk POST success with records
_STATUS_EMPTY = 412  # bulk POST empty result — no RESULT key present
_STATUS_AUTH_DENIED = 401  # auth failure


class FdaExtractor(RestApiExtractor[FdaRecord]):
    """
    Extractor for FDA iRES enforcement recall records — incremental path only.

    Queries POST /recalls/ using eventlmdfrom = watermark date, paginates until
    len(RESULT) < PAGE_SIZE, then loads to fda_recalls_bronze. The count guard
    (_MAX_INCREMENTAL_RECORDS) aborts if the result set is unexpectedly large —
    catching a watermark bug before it silently loads the full 134K-record dataset.

    For historical loads and deep rescans use FdaDeepRescanLoader, which has no
    count guard and accepts explicit start_date / end_date arguments.
    """

    source_name: str = _FDA_SOURCE
    settings: Settings

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")

    def model_post_init(self, __context: Any) -> None:
        self._engine = sa.create_engine(
            self.settings.neon_database_url.get_secret_value(),
            pool_pre_ping=True,
        )
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch all FDA records with EVENTLMD >= watermark date.

        Raises TransientExtractionError on 5xx or if the response count exceeds the
        guard ceiling. Raises RateLimitError on 429. Raises AuthenticationError on 401.
        Raises ExtractionError on non-retryable FDA STATUSCODE values (402–411, 413–418).
        """
        with self._engine.connect() as conn:
            start_date = self._get_watermark(conn)

        if not isinstance(start_date, date):
            raise TransientExtractionError(
                f"FDA watermark returned unexpected type {type(start_date)!r}; "
                "aborting to avoid unfiltered full-dataset pull"
            )

        filter_str = f"[{{'eventlmdfrom':'{start_date.strftime('%m/%d/%Y')}'}}]"
        records = self._paginate(filter_str, sort="eventlmd", sortorder="desc")

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"FDA incremental query returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. "
                "Possible cause: watermark bug or eventlmdfrom parameter not applied."
            )

        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_FDA_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[FdaRecord], list[QuarantineRecord]]:
        valid: list[FdaRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(FdaRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("PRODUCTID")) or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[FdaRecord]
    ) -> tuple[list[FdaRecord], list[QuarantineRecord]]:
        passing: list[FdaRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = check_null_source_id(record.source_recall_id)
            if not failure and record.recall_initiation_dt is not None:
                failure = check_date_sanity(record.recall_initiation_dt, "recall_initiation_dt")
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
        records: list[FdaRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        loader = BronzeLoader(bronze_table=_fda_bronze, rejected_table=_fda_rejected)
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            if records:
                max_date = max(r.event_lmd for r in records).date()
                self._update_watermark(conn, max_date)
        return count

    # --- Private helpers ---

    def _paginate(
        self,
        filter_str: str,
        sort: str = "eventlmd",
        sortorder: str = "desc",
    ) -> list[dict[str, Any]]:
        """Paginate through all pages of a bulk POST query."""
        all_records: list[dict[str, Any]] = []
        start = 1
        while True:
            page = self._fetch_page(
                filter_str=filter_str,
                start=start,
                sort=sort,
                sortorder=sortorder,
            )
            all_records.extend(page)
            if len(page) < _PAGE_SIZE:
                break
            start += _PAGE_SIZE
        return all_records

    def _fetch_page(
        self,
        filter_str: str,
        start: int = 1,
        sort: str = "eventlmd",
        sortorder: str = "desc",
    ) -> list[dict[str, Any]]:
        """POST a single page to /recalls/ and return the RESULT list."""
        payload = {
            "displaycolumns": _DISPLAY_COLUMNS,
            "filter": filter_str,
            "start": start,
            "rows": _PAGE_SIZE,
            "sort": sort,
            "sortorder": sortorder,
        }
        url = f"{self.base_url}{_RECALLS_ENDPOINT}?signature={int(time.time())}"
        try:
            # follow_redirects=True: FDA iRES returns 302 redirects on the bulk POST
            # endpoint; Bruno's working request sets followRedirects: true (max 5).
            # User-Agent override: the default `python-httpx/...` string is a likely
            # bot-fingerprint signal for FDA's anti-abuse layer — the value below
            # matches FDA's own Python sample code.
            with httpx.Client(
                timeout=self.timeout_seconds,
                follow_redirects=True,
                headers={"User-Agent": _USER_AGENT},
            ) as client:
                response = client.post(
                    url,
                    data={"payLoad": json.dumps(payload)},
                    headers=self._auth_headers(),
                )
        except httpx.TransportError as exc:
            raise TransientExtractionError(f"FDA network error: {exc}") from exc

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            self._capture_error_response(url, response)
            raise RateLimitError(retry_after=retry_after)

        # FDA anti-abuse detection: the iRES server signals throttling by redirecting
        # bulk POST requests (302) to /apology_objects/abuse-detection-apology.html
        # instead of returning a JSON response. Detected by Content-Type=text/html
        # (the API normally returns application/json). Raise ExtractionError so
        # tenacity does NOT retry — retries deepen the throttle and extend the block.
        content_type = response.headers.get("Content-Type", "")
        if "text/html" in content_type:
            self._capture_error_response(url, response)
            raise ExtractionError(
                f"FDA anti-abuse throttle detected (HTTP {response.status_code}, "
                "HTML response in place of JSON). Wait at least 30 minutes before "
                "retrying. Caused by too many rapid requests."
            )

        if response.status_code != 200:
            self._capture_error_response(url, response)
            raise TransientExtractionError(f"FDA HTTP {response.status_code}")

        return self._parse_bulk_post_response(response.json(), url)

    def _parse_bulk_post_response(self, body: dict[str, Any], url: str) -> list[dict[str, Any]]:
        """
        Interpret FDA's STATUSCODE envelope and return the RESULT rows.

        STATUSCODE 400 → success, return RESULT list.
        STATUSCODE 412 → empty window, return [].
        STATUSCODE 401 → auth failure, raise AuthenticationError.
        STATUSCODE 402–418 → payload/parameter error, raise ExtractionError (non-retryable).
        """
        status = body.get("STATUSCODE")
        if status == _STATUS_SUCCESS:
            result = body.get("RESULT", [])
            if not isinstance(result, list):
                raise TransientExtractionError(
                    f"FDA bulk POST: expected RESULT to be a list, got {type(result)!r}"
                )
            return result
        if status == _STATUS_EMPTY:
            return []
        if status == _STATUS_AUTH_DENIED:
            raise AuthenticationError(
                f"FDA iRES authorization denied (STATUSCODE {status}): {body.get('MESSAGE')}"
            )
        raise ExtractionError(
            f"FDA iRES non-retryable error (STATUSCODE {status}): {body.get('MESSAGE')} — "
            f"request URL: {url}"
        )

    def _auth_headers(self) -> dict[str, str]:
        user = self.settings.fda_authorization_user
        key = self.settings.fda_authorization_key
        if user is None or key is None:
            raise AuthenticationError(
                "FDA_AUTHORIZATION_USER and FDA_AUTHORIZATION_KEY must be set in environment"
            )
        return {
            "Authorization-User": user.get_secret_value(),
            "Authorization-Key": key.get_secret_value(),
        }

    def _capture_error_response(self, url: str, response: httpx.Response) -> None:
        # FDA POSTs a form-encoded payLoad= body — capture it so promote_error_to_
        # cassette.py can emit a cassette VCR will match against on replay.
        request_body: str | None = None
        if response.request.content:
            try:
                request_body = response.request.content.decode("utf-8")
            except UnicodeDecodeError:
                request_body = None
        try:
            self._r2_client.land_error_response(
                source=_FDA_SOURCE,
                request_method=response.request.method,
                request_url=url,
                request_body=request_body,
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                "fda.error_capture_failed",
                status_code=response.status_code,
                url=url,
            )

    def _get_watermark(self, conn: sa.Connection) -> date:
        row = conn.execute(
            sa.select(_source_watermarks.c.last_cursor).where(
                _source_watermarks.c.source == _FDA_SOURCE
            )
        ).fetchone()
        if row and row[0]:
            return date.fromisoformat(row[0])
        return datetime.now(UTC).date() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)

    def _update_watermark(self, conn: sa.Connection, new_date: date) -> None:
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _FDA_SOURCE)
            .values(last_cursor=new_date.isoformat(), updated_at=datetime.now(UTC))
        )


class FdaDeepRescanLoader(FdaExtractor):
    """
    Historical / deep-rescan loader for FDA iRES records.

    Accepts explicit start_date and end_date; uses a compound eventlmdfrom + eventlmdto
    filter; paginates without a record-count guard. Sort is recalleventid asc for
    deterministic page boundaries (resumable if a partial run fails).

    Does NOT update source_watermarks — deep rescans are additive to the bronze table;
    the incremental watermark is managed exclusively by FdaExtractor.

    Used by the deep-rescan-fda.yml GitHub Actions workflow (ADR 0023).
    """

    # Date range set by caller before run()
    _start_date: date = PrivateAttr()
    _end_date: date = PrivateAttr()

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self._start_date = datetime.now(UTC).date() - timedelta(days=90)
        self._end_date = datetime.now(UTC).date()

    def set_date_range(self, start_date: date, end_date: date) -> None:
        self._start_date = start_date
        self._end_date = end_date

    def extract(self) -> list[dict[str, Any]]:
        """Fetch all FDA records with EVENTLMD between start_date and end_date (inclusive)."""
        start_str = self._start_date.strftime("%m/%d/%Y")
        end_str = self._end_date.strftime("%m/%d/%Y")
        filter_str = f"[{{'eventlmdfrom':'{start_str}'}},{{'eventlmdto':'{end_str}'}}]"
        logger.info("fda.deep_rescan.extract", start_date=start_str, end_date=end_str)
        return self._paginate(filter_str, sort="recalleventid", sortorder="asc")

    def load_bronze(
        self,
        records: list[FdaRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        loader = BronzeLoader(bronze_table=_fda_bronze, rejected_table=_fda_rejected)
        with self._engine.begin() as conn:
            # Does NOT update source_watermarks — the incremental extractor owns the
            # watermark exclusively.
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
