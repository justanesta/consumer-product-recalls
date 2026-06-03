"""FDA Tier-3 press-release enrichment extractor (capture-expansion (b) PR, Part C).

Captures press-release URLs per recall event from the lookup endpoint
``GET /search/pressreleaseurls/{eventid}`` into ``fda_press_releases_bronze``.
Those 4 columns (``recalleventid, pressreleasetype, pressreleaseissuedt,
pressreleaseurl``) are lookup-endpoint-only — the bulk POST 406s them (Finding K0).

Architecture (``project_scope/silver-field-capture-expansion-plan.md`` Part C):

- **First per-record REST fan-out in the repo** — one GET per recall event, work-list
  sourced from ``fda_recalls_bronze`` (the analog of ``UscgManufacturerDetailExtractor``,
  but REST/JSON not HTML). Shares the FDA iRES transport (auth / signature / anti-abuse
  throttle / STATUSCODE / watermark) with ``FdaExtractor`` via ``FdaIresExtractor``.

- **Empty-result reality.** Most events have NO press release; an empty RESULT is a
  successful no-op (the event contributes no rows). The incremental work-list is a
  *date cursor* — events whose recall changed since the watermark (``event_lmd``) —
  NOT "events lacking a row" (which would re-GET the empty majority every run, since
  empty events never produce a row). A press release added to an old, otherwise-unchanged
  event is caught by the deep-rescan full sweep — the documented USCG-style detail-drift
  limitation.

- **Identity** ``(source_recall_id, press_release_url)``: ``source_recall_id`` is
  RECALLEVENTID (the event), joinable to ``fda_recalls_bronze.recall_event_id`` and to
  silver ``recall_event`` via ``md5('FDA' || '|' || source_recall_id)``.

- **Lifecycle**: ``extract`` (work-list → per-event GET → flatten rows) → ``land_raw``
  (one JSON blob, like ``FdaExtractor``) → ``validate`` → ``check_invariants`` →
  ``load_bronze`` (incremental advances the watermark to the max ``event_lmd`` of the
  events processed; deep-rescan does not, like ``FdaDeepRescanLoader``).
"""

from __future__ import annotations

import json
import time
from datetime import datetime  # noqa: TC003 — Pydantic evaluates the PrivateAttr annotation
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
import tenacity
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME
from src.bronze.invariants import (
    PER_RECORD_INVARIANTS_BY_SOURCE_NAME,
    run_per_record_invariants,
)
from src.bronze.loader import BronzeLoader
from src.config.db import make_engine
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    QuarantineRecord,
    RateLimitError,
    TransientExtractionError,
)
from src.extractors._fda_base import (
    IRES_USER_AGENT,
    STATUS_AUTH_DENIED,
    STATUS_EMPTY,
    STATUS_SUCCESS,
    FdaIresExtractor,
)
from src.landing.r2 import R2LandingClient
from src.schemas.fda import FdaPressReleaseRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_press_releases_bronze = sa.Table(
    "fda_press_releases_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),  # = RECALLEVENTID
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("press_release_url", sa.Text),
    sa.Column("press_release_type", sa.Text),
    sa.Column("press_release_issued_dt", sa.TIMESTAMP(timezone=True)),
)

_press_releases_rejected = sa.Table(
    "fda_press_releases_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", postgresql.JSONB),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

# The FDA recalls bronze table — read-only here, for the event work-list.
_fda_recalls_bronze = sa.Table(
    "fda_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("recall_event_id", sa.BigInteger),
    sa.Column("event_lmd", sa.TIMESTAMP(timezone=True)),
)

_SOURCE = "fda_press_releases"
_PR_ENDPOINT = "/search/pressreleaseurls/"  # + {eventid}

# Incremental guard: a daily delta is ~50-180 changed events. 5000 catches a watermark
# reset that would otherwise re-sweep the whole ~25K-event corpus on the incremental
# path. Deep-rescan uses _MAX_SWEEP_EVENTS (no daily-delta assumption).
_MAX_INCREMENTAL_EVENTS = 5_000
# Deep-rescan guard: the distinct-event corpus is ~25K; 100K is ~4× headroom. Hit
# before then = a work-list blow-up (join pathology / upstream explosion); abort.
_MAX_SWEEP_EVENTS = 100_000

# Per-event retry, scoped to TransientExtractionError ONLY (mirrors fda.py
# _PER_PAGE_RETRY): RateLimitError propagates to run()'s outer retry, and the
# text/html anti-abuse ExtractionError propagates UNRETRIED (retrying deepens the
# Akamai throttle). 3 attempts bounds in-sweep amplification.
_PER_EVENT_RETRY = tenacity.Retrying(
    retry=tenacity.retry_if_exception_type(TransientExtractionError),
    wait=tenacity.wait_exponential_jitter(initial=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)


def _rows_from_result(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise FDA's RESULT into a list of dict rows (array or columnar shape).

    Mirrors the ``rowsFromResult`` helper in
    ``bruno/fda/lookup/get_press_release_urls.yml``. Returns ``[]`` for every empty
    shape (``RESULT`` null / ``[]`` / empty columnar) — the empty-event case.
    """
    result = body.get("RESULT")
    if isinstance(result, list):
        return [r for r in result if isinstance(r, dict)]
    if isinstance(result, dict):
        cols = result.get("COLUMNS")
        data = result.get("DATA")
        if isinstance(cols, list) and isinstance(data, list):
            return [dict(zip(cols, row, strict=False)) for row in data if isinstance(row, list)]
    return []


def _interpret_pr_response(
    body: dict[str, Any], *, event_id: int, url: str
) -> list[dict[str, Any]]:
    """Interpret the FDA STATUSCODE envelope for a press-release response.

    STATUSCODE 400 → success, return RESULT rows (possibly ``[]`` — empty event).
    STATUSCODE 412 → empty, return ``[]``. 401 → AuthenticationError.
    Anything else → ExtractionError (non-retryable).
    """
    status = body.get("STATUSCODE")
    if status == STATUS_SUCCESS:
        return _rows_from_result(body)
    if status == STATUS_EMPTY:
        return []
    if status == STATUS_AUTH_DENIED:
        raise AuthenticationError(
            f"FDA iRES authorization denied (STATUSCODE {status}): {body.get('MESSAGE')}"
        )
    raise ExtractionError(
        f"FDA press-release non-retryable error (STATUSCODE {status}): "
        f"{body.get('MESSAGE')} — event {event_id}, url {url}"
    )


class FdaPressReleaseExtractor(FdaIresExtractor[FdaPressReleaseRecord]):
    """Per-event press-release extractor — incremental path (event_lmd watermark cursor).

    Fetches one ``GET /search/pressreleaseurls/{eventid}`` per event whose recall changed
    since the watermark. For the ~25K-event historical seed use
    ``FdaPressReleaseDeepRescanLoader`` (full sweep, chunkable via ``work_list_limit`` +
    ``resume_after_event_id``). Auth / error-capture / watermark are inherited from
    ``FdaIresExtractor``.
    """

    source_name: str = _SOURCE

    # Cap on the work-list (CLI --limit): a small N for cheap dev validation
    # (a few events exercise fetch → R2 → bronze end-to-end without the full sweep).
    work_list_limit: int | None = None
    # Deep-rescan chunking cursor (CLI --resume-after-event-id): skip events with
    # recall_event_id <= this. None = from the start. Incremental leaves it None.
    resume_after_event_id: int | None = None
    # Polite inter-event pacing (Akamai). The seed paces here; dev/incremental deltas
    # are small. Tune up for the full sweep if the throttle bites.
    inter_event_sleep_seconds: float = 1.0

    # Max event_lmd of the events processed this run — the watermark-advance target,
    # stashed by extract() and consumed by load_bronze() (incremental only).
    _processed_max_event_lmd: datetime | None = PrivateAttr(default=None)

    def model_post_init(self, __context: Any) -> None:
        self._engine = make_engine(self.settings.neon_database_url.get_secret_value())
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """Fetch press releases for the incremental work-list (events changed since watermark)."""
        with self._engine.connect() as conn:
            work = self._build_work_list(conn, incremental=True)

        if len(work) > _MAX_INCREMENTAL_EVENTS:
            raise TransientExtractionError(
                f"FDA press-release incremental work-list returned {len(work)} events — "
                f"exceeds guard of {_MAX_INCREMENTAL_EVENTS}. Possible cause: watermark "
                "bug (cursor reset) re-sweeping the whole corpus on the incremental path."
            )
        work = self._apply_limit(work)
        return self._fetch_all(work)

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        content = json.dumps(raw_records, default=str).encode("utf-8")
        path = self._r2_client.land(source=_SOURCE, content=content, suffix="json")
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[FdaPressReleaseRecord], list[QuarantineRecord]]:
        valid: list[FdaPressReleaseRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(FdaPressReleaseRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        source_recall_id=str(record.get("RECALLEVENTID") or "") or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[FdaPressReleaseRecord]
    ) -> tuple[list[FdaPressReleaseRecord], list[QuarantineRecord]]:
        passing: list[FdaPressReleaseRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = run_per_record_invariants(
                record, PER_RECORD_INVARIANTS_BY_SOURCE_NAME[_SOURCE]
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
                passing.append(record)
        return passing, quarantined

    def load_bronze(
        self,
        records: list[FdaPressReleaseRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_SOURCE],
            bronze_table=_press_releases_bronze,
            rejected_table=_press_releases_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            # Advance the watermark to the max event_lmd of the events CHECKED (not the
            # PR rows loaded — empty events still advance the cursor). Skip when --limit
            # is set: a partial check must not move the cursor past unchecked events.
            if self.work_list_limit is None and self._processed_max_event_lmd is not None:
                self._update_watermark(conn, self._processed_max_event_lmd.date())
        return count

    # --- Work-list ---

    def _build_work_list(self, conn: sa.Connection, *, incremental: bool) -> list[dict[str, Any]]:
        """Distinct recall events (+ max event_lmd) to fetch press releases for.

        Incremental: events whose recall changed since the watermark
        (``HAVING max(event_lmd) >= cursor``). Deep-rescan (``incremental=False``): the
        full sweep, optionally resumed past ``resume_after_event_id``. Ordered by
        ``recall_event_id`` so a chunked seed (``--limit`` + ``--resume-after-event-id``)
        is contiguous and deterministic.
        """
        fb = _fda_recalls_bronze
        stmt = (
            sa.select(
                fb.c.recall_event_id.label("recall_event_id"),
                sa.func.max(fb.c.event_lmd).label("event_lmd"),
            )
            .where(fb.c.recall_event_id.is_not(None))
            .group_by(fb.c.recall_event_id)
            .order_by(fb.c.recall_event_id)
        )
        if self.resume_after_event_id is not None:
            stmt = stmt.where(fb.c.recall_event_id > self.resume_after_event_id)
        if incremental:
            watermark = self._get_watermark(conn)
            stmt = stmt.having(sa.func.max(fb.c.event_lmd) >= watermark)
        rows = conn.execute(stmt).all()
        return [
            {"recall_event_id": int(row.recall_event_id), "event_lmd": row.event_lmd}
            for row in rows
        ]

    def _apply_limit(self, work: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if self.work_list_limit is None:
            return work
        full_size = len(work)
        work = work[: self.work_list_limit]
        logger.info(
            "fda_press_releases.work_list_limited",
            limit=self.work_list_limit,
            fetching=len(work),
            work_list_size=full_size,
        )
        return work

    # --- Fetch ---

    def _fetch_all(self, work: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Fetch every work-list event's press releases; flatten to one row list.

        Tracks the max ``event_lmd`` of the events processed (the watermark-advance
        target) regardless of whether they had press releases.
        """
        rows: list[dict[str, Any]] = []
        max_lmd: datetime | None = None
        with httpx.Client(
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers={"User-Agent": IRES_USER_AGENT},
        ) as client:
            for idx, item in enumerate(work):
                if idx > 0 and self.inter_event_sleep_seconds > 0:
                    time.sleep(self.inter_event_sleep_seconds)
                event_id = item["recall_event_id"]
                event_rows = _PER_EVENT_RETRY(
                    self._fetch_event, client=client, event_id=event_id, capture=(idx == 0)
                )
                rows.extend(event_rows)
                lmd = item["event_lmd"]
                if lmd is not None and (max_lmd is None or lmd > max_lmd):
                    max_lmd = lmd
        self._processed_max_event_lmd = max_lmd
        return rows

    def _fetch_event(
        self, client: httpx.Client, event_id: int, capture: bool = False
    ) -> list[dict[str, Any]]:
        """GET one event's press releases and return its RESULT rows (possibly empty)."""
        url = f"{self.base_url}{_PR_ENDPOINT}{event_id}?signature={int(time.time())}"
        try:
            response = client.get(url, headers=self._auth_headers())
        except httpx.TransportError as exc:
            raise TransientExtractionError(
                f"FDA press-release network error (event {event_id}): {exc}"
            ) from exc

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 60))
            self._capture_error_response(url, response)
            raise RateLimitError(retry_after=retry_after)

        # Anti-abuse: iRES redirects throttled requests (302) to an HTML apology page.
        # Raise ExtractionError so tenacity does NOT retry — retries deepen the throttle.
        if "text/html" in response.headers.get("Content-Type", ""):
            self._capture_error_response(url, response)
            raise ExtractionError(
                f"FDA anti-abuse throttle detected (HTTP {response.status_code}, HTML in "
                f"place of JSON; event {event_id}). Wait >=30 min before retrying."
            )

        if response.status_code != 200:
            self._capture_error_response(url, response)
            raise TransientExtractionError(
                f"FDA press-release HTTP {response.status_code} (event {event_id})"
            )

        if capture:
            self._capture_response(response)
        return _interpret_pr_response(response.json(), event_id=event_id, url=url)


class FdaPressReleaseDeepRescanLoader(FdaPressReleaseExtractor):
    """Historical / deep-rescan loader — the full ~25K-event sweep.

    Differences from the incremental path:
      - ``extract`` walks ALL distinct events (no watermark filter, no incremental
        count guard — the sweep guard ``_MAX_SWEEP_EVENTS`` catches a work-list blow-up).
      - chunkable: ``work_list_limit`` + ``resume_after_event_id`` march through the
        corpus in ``recall_event_id`` order across runs (content-hash dedup makes a
        re-run of an overlapping chunk idempotent) — the operator-controlled way to fit
        the long seed under the GitHub Actions 6h job limit / resume after a throttle.
      - ``load_bronze`` does NOT advance the watermark (the incremental path owns it).
    """

    def extract(self) -> list[dict[str, Any]]:
        with self._engine.connect() as conn:
            work = self._build_work_list(conn, incremental=False)

        if len(work) > _MAX_SWEEP_EVENTS:
            raise TransientExtractionError(
                f"FDA press-release deep-rescan work-list returned {len(work)} events — "
                f"exceeds guard of {_MAX_SWEEP_EVENTS}. Possible cause: work-list join "
                "blow-up or upstream record-count explosion."
            )
        work = self._apply_limit(work)
        logger.info(
            "fda_press_releases.deep_rescan.extract",
            events=len(work),
            resume_after_event_id=self.resume_after_event_id,
        )
        return self._fetch_all(work)

    def load_bronze(
        self,
        records: list[FdaPressReleaseRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Same loader config; does NOT advance the watermark (incremental owns it)."""
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_SOURCE],
            bronze_table=_press_releases_bronze,
            rejected_table=_press_releases_rejected,
        )
        with self._engine.begin() as conn:
            return loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
