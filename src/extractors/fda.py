from __future__ import annotations

import json
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
import tenacity
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
    ExtractionResult,
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
    # Phase 6a.5 capture expansion (2026-05-31, migration 0019) — audit §7a SHIP fields.
    sa.Column("code_information", sa.Text),
    sa.Column("firm_city_nam", sa.Text),
    sa.Column("firm_country_nam", sa.Text),
    sa.Column("firm_line1_adr", sa.Text),
    sa.Column("firm_line2_adr", sa.Text),
    sa.Column("firm_postal_cd", sa.Text),
    sa.Column("firm_state_cd", sa.Text),
    sa.Column("firm_state_prvnc_nam", sa.Text),
    sa.Column("firm_surviving_nam", sa.Text),
    sa.Column("firm_surviving_fei", sa.BigInteger),
    sa.Column("posted_internet_dt", sa.TIMESTAMP(timezone=True)),
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

_FDA_SOURCE = "fda"
_DEFAULT_LOOKBACK_DAYS = 1
# 2500 (not 5000): codeinformation in _DISPLAY_COLUMNS caps the API page at 2500
# rows (audit Decision 5). Both the `rows` request param and the pagination
# stop-condition (len(page) < _PAGE_SIZE) key off this, so it must match the API's
# real per-page cap — otherwise the historical seed silently truncates after page 1.
_PAGE_SIZE = 2_500

# displaycolumns sent to every bulk POST request. Matches the empirically-validated
# column set from bruno/fda/incremental_extraction/post_recalls_eventlmd_range.yml.
# The Phase 6a.5 capture expansion (2026-05-31, migration 0019) added the 11 audit
# §7a SHIP fields — codeinformation, the 8 firm-address fields, firmsurviving{nam,fei},
# and postedinternetdt — so the one-time historical seed captures everything silver +
# Phase 6b firm-resolution need (R2 replay can't recover un-requested columns later).
# Including codeinformation drops the API page cap 5000 → 2500 (see _PAGE_SIZE above).
# productlmd stays excluded — lookup-endpoint only, not bulk POST (finding K0).
_DISPLAY_COLUMNS = (
    "recalleventid,productid,producttypeshort,recallnum,phasetxt,centercd,"
    "centerclassificationtypetxt,firmlegalnam,firmfeinum,recallinitiationdt,"
    "centerclassificationdt,terminationdt,enforcementreportdt,determinationdt,"
    "initialfirmnotificationtxt,distributionareasummarytxt,voluntarytypetxt,"
    "productdescriptiontxt,productshortreasontxt,productdistributedquantity,eventlmd,"
    "codeinformation,firmcitynam,firmcountrynam,firmline1adr,firmline2adr,"
    "firmpostalcd,firmstatecd,firmstateprvncnam,firmsurvivingnam,firmsurvivingfei,"
    "postedinternetdt"
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

# Per-page retry for _paginate. Scoped to TransientExtractionError ONLY (5xx /
# transport): RateLimitError must propagate to run()'s outer _TRANSIENT_RETRY
# (it is a sibling, not a subclass — _base.py), and the text/html anti-abuse
# ExtractionError must propagate UNRETRIED (retrying deepens the Akamai throttle —
# the exact failure the historical seed avoids). 3 attempts bounds in-sweep
# amplification (audit C11) without the 5×5 nesting a broader scope would cause.
_PER_PAGE_RETRY = tenacity.Retrying(
    retry=tenacity.retry_if_exception_type(TransientExtractionError),
    wait=tenacity.wait_exponential_jitter(initial=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)


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

    # Inter-page pacing for _paginate. 0.0 = no sleep (the incremental default —
    # a daily delta is 1-2 pages). The historical seed sets this to 5.0 on
    # FdaDeepRescanLoader (Probe 4 floor) to stay under FDA's anti-abuse throttle
    # across ~54 pages.
    inter_page_sleep_seconds: float = 0.0

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
        # RID is excluded from hashing: it is a query-position counter (finding F in
        # api_observations.md), not a property of the recall record. Different query
        # windows return the same record at different positions, producing different RID
        # values and spurious hash changes. RID is still written to the DB row.
        loader = BronzeLoader(
            bronze_table=_fda_bronze,
            rejected_table=_fda_rejected,
            hash_exclude_fields=frozenset({"rid"}),
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            # event_lmd is nullable (migration 0020): un-edited records have null
            # EVENTLMD (Finding H). Advance the watermark only over the non-null
            # values; an all-null batch must leave last_cursor untouched (do NOT
            # "simplify" this back to an unconditional max — null rows are
            # seed-only and the watermark must never regress).
            dates = [r.event_lmd for r in records if r.event_lmd is not None]
            if dates:
                self._update_watermark(conn, max(dates).date())
        return count

    # --- Private helpers ---

    def _paginate(
        self,
        filter_str: str,
        sort: str = "eventlmd",
        sortorder: str = "desc",
    ) -> list[dict[str, Any]]:
        """Paginate through all pages of a bulk POST query.

        Each page fetch is retried independently via ``_PER_PAGE_RETRY`` so a
        transient blip on page N does not restart the whole sweep (audit C11
        amplification). The retry is scoped to ``TransientExtractionError`` ONLY:
        ``RateLimitError`` propagates to the outer ``_TRANSIENT_RETRY`` in
        ``run()``, and the ``text/html`` anti-abuse ``ExtractionError``
        (``_fetch_page``) propagates unretried — retrying it would deepen the
        Akamai throttle, the exact failure the historical seed exists to avoid.

        ``inter_page_sleep_seconds`` (>0 only on the deep-rescan seed) paces
        successive pages to stay under the anti-abuse throttle.
        """
        all_records: list[dict[str, Any]] = []
        start = 1
        while True:
            page = _PER_PAGE_RETRY(
                self._fetch_page,
                filter_str=filter_str,
                start=start,
                sort=sort,
                sortorder=sortorder,
            )
            all_records.extend(page)
            if len(page) < _PAGE_SIZE:
                break
            start += _PAGE_SIZE
            if self.inter_page_sleep_seconds > 0:
                time.sleep(self.inter_page_sleep_seconds)
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

        # Capture only the first page: subsequent pages are pagination follow-ups
        # whose headers don't carry the conditional-GET semantics we're studying.
        if start == 1:
            self._capture_response(response)

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

    def override_watermark_lookback(self, days: int) -> None:
        """Override the source_watermarks cursor to today - N days.

        CLI hook for ``recalls extract fda --lookback-days N``. Encapsulates
        the engine + watermark-table access inside the extractor so the CLI
        does not need to import private symbols (``_source_watermarks``) or
        reach into ``_engine``. Writes are committed immediately; the next
        ``run()`` call will see the overridden cursor.
        """
        override_date = datetime.now(UTC).date() - timedelta(days=days)
        with self._engine.begin() as conn:
            conn.execute(
                sa.update(_source_watermarks)
                .where(_source_watermarks.c.source == _FDA_SOURCE)
                .values(last_cursor=override_date.isoformat())
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
            "source": _FDA_SOURCE,
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


class FdaDeepRescanLoader(FdaExtractor):
    """
    Historical / deep-rescan loader for FDA iRES records.

    Two modes:
    - **Windowed** (set_date_range): compound eventlmdfrom + eventlmdto filter, for a
      targeted re-pull of a date range. Used by deep-rescan-fda.yml (ADR 0023).
    - **Full-corpus** (set_full_corpus): the Phase 6a.5 historical seed. Uses
      filter:"[]" (NO eventlmd window) so the ~197 null-EVENTLMD un-edited records
      (Finding H) are included rather than silently excluded by a >= comparison.
      Requires migration 0020 (nullable core fields) so they land instead of
      quarantining. See project_scope/archive/fda-historical-seed-plan.md.

    Both modes paginate without a record-count guard, sort ``productid`` asc (the UNIQUE
    row key — a total order with NO ties, so offset pagination cannot straddle a page
    boundary and silently drop rows), and pace pages via inter_page_sleep_seconds (5.0s
    default here, the Probe-4 anti-abuse floor).

    Does NOT update source_watermarks — deep rescans are additive to the bronze table;
    the incremental watermark is managed exclusively by FdaExtractor.
    """

    # 5s/page (Probe 4) keeps the ~54-page full-corpus seed under FDA's anti-abuse
    # throttle. Overrides the incremental default of 0.0.
    inter_page_sleep_seconds: float = 5.0

    # Date range set by caller before run() (windowed mode)
    _start_date: date = PrivateAttr()
    _end_date: date = PrivateAttr()
    # Full-corpus mode flag (set by set_full_corpus); when True, extract() ignores
    # the date range and pulls the whole corpus via filter:"[]".
    _full_corpus: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self._start_date = datetime.now(UTC).date() - timedelta(days=90)
        self._end_date = datetime.now(UTC).date()

    def set_date_range(self, start_date: date, end_date: date) -> None:
        self._start_date = start_date
        self._end_date = end_date

    def set_full_corpus(self) -> None:
        """Switch to full-corpus mode: extract() pulls the whole corpus (filter:"[]")."""
        self._full_corpus = True

    def extract(self) -> list[dict[str, Any]]:
        """Fetch FDA records — the full corpus (full-corpus mode) or an EVENTLMD window."""
        if self._full_corpus:
            # filter:"[]" returns the whole dataset including null-EVENTLMD rows that a
            # date window's >= comparison drops (Finding H). Sort on the UNIQUE productid,
            # NOT the non-unique recalleventid: recalleventid groups many products per
            # event, so its tie boundaries reshuffle non-deterministically across page
            # requests and offset pagination silently DROPS distinct products (~245 per
            # seed, confirmed 2026-06-01 via scripts/fda/audit/diagnose_seed_straddle.py).
            # A unique sort key has no ties -> no straddle -> complete; within_batch_dedup
            # stays on as belt-and-suspenders.
            logger.info("fda.deep_rescan.extract", mode="full_corpus", filter="[]")
            return self._paginate("[]", sort="productid", sortorder="asc")
        start_str = self._start_date.strftime("%m/%d/%Y")
        end_str = self._end_date.strftime("%m/%d/%Y")
        filter_str = f"[{{'eventlmdfrom':'{start_str}'}},{{'eventlmdto':'{end_str}'}}]"
        logger.info("fda.deep_rescan.extract", start_date=start_str, end_date=end_str)
        # Sort on the unique productid (see the full-corpus branch above): the windowed
        # re-pull has the same tie-boundary straddle risk whenever a window exceeds one page.
        return self._paginate(filter_str, sort="productid", sortorder="asc")

    def load_bronze(
        self,
        records: list[FdaRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        # within_batch_dedup=True: recalleventid's tie-boundary is non-deterministic
        # across requests, so the same PRODUCTID can straddle two adjacent pages.
        # 'rid' (the only differing field) is hash-excluded, so straddle copies are
        # byte-identical → collapse to one (it does NOT raise — that path fires only on
        # same identity with *different* content, which a single filter:"[]" snapshot
        # should never produce; if it does, the loud abort is correct). Distinct
        # PRODUCTIDs sharing a recalleventid tie are distinct identities and are kept.
        loader = BronzeLoader(
            bronze_table=_fda_bronze,
            rejected_table=_fda_rejected,
            hash_exclude_fields=frozenset({"rid"}),
            within_batch_dedup=True,
        )
        with self._engine.begin() as conn:
            # Does NOT update source_watermarks — the incremental extractor owns the
            # watermark exclusively.
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
