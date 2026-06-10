from __future__ import annotations

import abc
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
import structlog.contextvars
import tenacity
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from sqlalchemy.exc import OperationalError

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME
from src.bronze.manifest import build_presence_manifest_rows
from src.extractors._tables import extraction_run_identities, extraction_runs

if TYPE_CHECKING:
    import httpx
    from sqlalchemy import Connection

logger = structlog.get_logger()


# --- Exception hierarchy ---


class ExtractionError(RuntimeError):
    """Base for all extractor errors."""


class TransientExtractionError(ExtractionError):
    """Transient failure (network, 5xx). The retry policy will retry these."""


class AuthenticationError(ExtractionError):
    """401/403 — fail fast, do not retry."""


class RateLimitError(ExtractionError):
    """429 — retry after delay. Concrete extractors should set retry_after from the header."""

    def __init__(self, retry_after: float = 60.0) -> None:
        super().__init__(f"Rate limited; retry after {retry_after:.0f}s")
        self.retry_after = retry_after


class ExtractionAbortedError(ExtractionError):
    """Raised when the batch rejection rate exceeds the configured threshold."""

    def __init__(self, source: str, rate: float, threshold: float) -> None:
        super().__init__(f"{source}: rejection rate {rate:.1%} exceeds threshold {threshold:.1%}")
        self.source = source
        self.rate = rate
        self.threshold = threshold


# --- Value objects ---


@dataclass(frozen=True)
class QuarantineRecord:
    """
    A record that failed validation or invariant checks.
    Passed to load_bronze() for T1 quarantine into the source _rejected table.
    """

    source_recall_id: str | None
    raw_record: dict[str, Any]
    failure_reason: str
    failure_stage: str  # "validate" | "invariants"
    raw_landing_path: str


@dataclass
class ExtractionResult:
    source: str
    run_id: str
    records_fetched: int
    records_landed: int
    records_valid: int
    records_rejected_validate: int
    records_rejected_invariants: int
    records_loaded: int
    raw_landing_path: str
    rejection_rate: float = field(init=False)

    def __post_init__(self) -> None:
        total = self.records_fetched
        rejected = self.records_rejected_validate + self.records_rejected_invariants
        self.rejection_rate = rejected / total if total > 0 else 0.0


# --- Module-level retry policies (per ADR 0013) ---
# Per-source calibration is noted in ADR 0013 as a future concern; these are the v1 defaults.


def _is_disconnect(exc: BaseException) -> bool:
    """True iff ``exc`` is a SQLAlchemy ``OperationalError`` caused by a dropped
    connection (Neon reaps idle connections and can cold-start), as opposed to a
    query-level ``OperationalError`` such as the bind-parameter overflow guarded
    against in ``bronze/loader.py`` — those are deterministic and must NOT be retried.

    SQLAlchemy flags a recognized disconnect with ``connection_invalidated``; we also
    string-match the libpq signatures observed in the USCG-detail seed
    (``logs/seed_uscg_detail_chunk.log``) as a fallback. See
    ``documentation/audit/deep_rescan_reliability_audit.md`` (Problem 2).
    """
    if not isinstance(exc, OperationalError):
        return False
    if getattr(exc, "connection_invalidated", False):
        return True
    text = str(getattr(exc, "orig", None) or exc)
    return any(
        signature in text
        for signature in (
            "server closed the connection unexpectedly",
            "SSL connection has been closed unexpectedly",
            "connection already closed",
            "terminating connection due to",
        )
    )


_TRANSIENT_RETRY = tenacity.Retrying(
    # Transient HTTP/transport errors, rate limits, AND Neon connection drops.
    # NullPool (src/config/db.py) covers idle/between-op drops; this covers a drop
    # *during* a load transaction — the run's records are still in memory and the
    # load is idempotent under the content-hash contract, so a retry reconverges.
    # A non-disconnect OperationalError (e.g. the param-count overflow in
    # bronze/loader.py) is deterministic and deliberately NOT retried.
    retry=(
        tenacity.retry_if_exception_type((TransientExtractionError, RateLimitError))
        | tenacity.retry_if_exception(_is_disconnect)
    ),
    wait=tenacity.wait_exponential_jitter(initial=1, max=60),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

# R2 writes get a tighter profile: shorter max wait, fewer attempts.
_R2_RETRY = tenacity.Retrying(
    retry=tenacity.retry_if_exception_type(TransientExtractionError),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)


# --- Abstract base class ---


class Extractor[T: BaseModel](abc.ABC, BaseModel):
    """
    Abstract base class for all recall data extractors.

    Borrows the Pull ABC + BaseModel double-inheritance pattern from NYC DCP's dcpy:
    the class IS its own validated config object (Pydantic) AND enforces a strict
    interface contract (ABC). See ADR 0012.

    Concrete extractors must inherit from one of the three operation-type subclasses
    (RestApiExtractor, FlatFileExtractor, HtmlScrapingExtractor) and implement all
    five abstract lifecycle methods.

    Lifecycle (ADR 0013):
        extract → land_raw → validate → check_invariants → load_bronze
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_name: str
    # 5% default per ADR 0013; per-source tuning expected once real rejection rates
    # are observed in production.
    rejection_threshold: float = Field(default=0.05, ge=0.0, le=1.0)

    # ADR 0026 / C16: whether THIS extractor/loader writes the per-run presence manifest. True only
    # on extractors whose run enumerates the FULL source corpus (a complete presence snapshot):
    # UsdaExtractor (daily full-dump) and NhtsaDeepRescanLoader (both archives). The daily
    # NhtsaExtractor pulls POST_2010 only, so it inherits False. Gated together with the contract's
    # default_track_presence in _maybe_write_presence_manifest.
    writes_presence_manifest: bool = False

    # Response-capture state for the extraction_runs forensic columns (migrations
    # 0010 + 0011). The operation-type subclasses populate these via their
    # _capture_response(); the shared _record_run() template below reads them.
    # Declared here (not per-subclass) so the template type-checks and the three
    # op-type subclasses don't each re-declare the same five attrs. FlatFileExtractor
    # adds one more (_captured_response_inner_content_sha256) for the wrapper/inner
    # split, surfaced into the row via the _augment_response_row hook.
    _captured_response_status_code: int | None = PrivateAttr(default=None)
    _captured_response_etag: str | None = PrivateAttr(default=None)
    _captured_response_last_modified: str | None = PrivateAttr(default=None)
    _captured_response_body_sha256: str | None = PrivateAttr(default=None)
    _captured_response_headers: dict[str, str] | None = PrivateAttr(default=None)

    # Presence-manifest support (ADR 0026): run() stashes the invariant-passing records
    # here so _record_run can write the per-run presence manifest (extraction_run_identities)
    # in the same transaction as the extraction_runs row. None until validation runs (so a
    # run that fails before validation writes no manifest); reset at the top of every run().
    _passing_records: list[Any] | None = PrivateAttr(default=None)

    # --- Abstract lifecycle methods ---

    @abc.abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """Fetch raw records from the source. Retried on transient failures."""

    @abc.abstractmethod
    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """
        Persist raw payload to R2 before any validation.
        Returns the R2 object path (used as raw_landing_path in quarantine records).
        """

    @abc.abstractmethod
    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[T], list[QuarantineRecord]]:
        """
        Parse raw records with the source Pydantic schema.
        Returns (valid_records, quarantine_records).
        Must never raise on bad data — failed records go into quarantine_records.
        """

    @abc.abstractmethod
    def check_invariants(self, records: list[T]) -> tuple[list[T], list[QuarantineRecord]]:
        """
        Apply cross-record and semantic business rules (per ADR 0013 starter list).
        Returns (passing_records, quarantine_records).
        Must never raise on violations — failing records go into quarantine_records.
        """

    @abc.abstractmethod
    def load_bronze(
        self,
        records: list[T],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """
        Content-hash and conditionally insert records into the source bronze table.
        Writes quarantined records to the source _rejected table.
        Returns count of rows actually inserted (dedup excluded).
        """

    def parse_landed_payload(self, raw_bytes: bytes) -> list[dict[str, Any]]:
        """Inverse of :meth:`land_raw`: rebuild the ``raw_records`` list from a landed payload.

        Used by the re-ingest path (``src/bronze/reingest.py``) to replay a payload already in
        R2 through ``validate_records`` → ``check_invariants`` → bronze load, without contacting
        the source (ADR 0014 / ADR 0028 Mechanism B). Default raises: only the JSON REST sources
        round-trip cleanly (``RestApiExtractor`` overrides with ``json.loads`` — their ``land_raw``
        writes ``json.dumps(raw_records)``). Flat-file (NHTSA) and scraped (USCG) payloads are not
        JSON arrays of records and are cheaply re-fetchable via ``recalls deep-rescan`` instead, so
        they keep this raising default.
        """
        raise NotImplementedError(
            f"re-ingest (R2 replay) is not supported for {self.source_name!r}; "
            "use 'recalls deep-rescan' (the source is cheaply re-fetchable)."
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
        """Write a row to ``extraction_runs`` (shared template for all extractors).

        Best-effort: a failure here is logged with its exception type + message, not
        raised — the bronze write has already committed, so run-recording loss doesn't
        lose data, and a constraint violation stays diagnosable from logs. No-op when the
        extractor has no DB engine (abstract / mocked-test instantiation), preserving the
        old stub's behavior.

        Concrete extractors customize two axes via hooks instead of overriding this whole
        method: :meth:`_augment_run_row` (top-level columns, e.g. USCG's
        ``was_short_circuited``) and :meth:`_augment_response_row` (response-capture
        columns when a response was captured, e.g. NHTSA's
        ``response_inner_content_sha256``).

        ``change_type`` is one of routine / schema_rebaseline / hash_helper_rebaseline /
        historical_seed (per ADR 0027 + ADR 0028). Default is routine; the CLI's
        --change-type flag is validated before reaching this point.
        """
        engine = getattr(self, "_engine", None)
        if engine is None:
            return
        row: dict[str, Any] = {
            "source": self.source_name,
            "started_at": started_at,
            "finished_at": datetime.now(UTC),
            "status": status,
            "run_id": run_id,
            "error_message": error_message,
            "change_type": change_type,
        }
        self._augment_run_row(row)
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
            self._augment_response_row(row)
        try:
            with engine.begin() as conn:
                conn.execute(extraction_runs.insert().values(**row))
                # Presence manifest in the SAME txn as its FK parent (ADR 0026).
                self._maybe_write_presence_manifest(conn, run_id=run_id, status=status)
        except Exception as exc:
            logger.warning(
                "extraction_run.record_failed",
                run_id=run_id,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    def _maybe_write_presence_manifest(self, conn: Connection, *, run_id: str, status: str) -> None:
        """Write the ADR 0026 per-run presence manifest, in the caller's transaction.

        Called from :meth:`_record_run` inside the same ``engine.begin()`` block as the
        ``extraction_runs`` insert, so the manifest's ``run_id`` FK target already exists
        (the parent row was inserted immediately above and is visible within the txn).
        This is *why* the manifest is written here and not in ``load_bronze``:
        ``extraction_runs`` is recorded AFTER ``load_bronze`` (best-effort), so a manifest
        written in the bronze transaction would violate the FK — the run row would not yet
        exist. (ADR 0026's sketch predates the after-bronze run-recording design; this is
        the Q1 resolution against the current code.)

        Gated to:
          * ``status == "success"`` — aborted / failed runs do not assert presence.
          * the source's ``DedupContract.default_track_presence`` ({usda, nhtsa}; ADR 0026 /
            C16); every other source no-ops here.
          * ``self.writes_presence_manifest`` — True only on extractors whose run enumerates the
            FULL corpus (UsdaExtractor's daily full-dump; NhtsaDeepRescanLoader's both-archives
            pull). NHTSA's daily POST_2010 extract is partial, so it does NOT write the manifest.
          * ``_passing_records`` stashed by :meth:`run` (None on a run that failed before
            validation; ``[]`` on a 304 — both write no manifest).

        The manifest's recall key is ``contract.presence_recall_id_field`` (campno for NHTSA, whose
        bronze ``source_recall_id`` is the regen-unstable RECORD_ID), defaulting to
        ``source_recall_id``.
        Quarantined records are excluded by construction — ``run`` stashes the
        invariant-passing records only (ADR 0026 Q2).
        """
        if status != "success" or self._passing_records is None:
            return
        contract = DEDUP_CONTRACT_BY_SOURCE_NAME.get(self.source_name)
        if contract is None or not contract.default_track_presence:
            return
        # Only a run that enumerates the FULL corpus is a complete presence snapshot (C16). USDA's
        # daily full-dump qualifies; NHTSA only on the deep-rescan (both archives), not the daily
        # POST_2010 extract — so this is gated per extractor/loader, not just per source.
        if not self.writes_presence_manifest:
            return
        langcode_field = "langcode" if "langcode" in contract.identity_fields else None
        rows = build_presence_manifest_rows(
            self._passing_records,
            run_id=run_id,
            source=self.source_name,
            langcode_field=langcode_field,
            recall_id_field=contract.presence_recall_id_field or "source_recall_id",
        )
        if rows:
            conn.execute(extraction_run_identities.insert(), rows)

    def _augment_run_row(self, row: dict[str, Any]) -> None:
        """Hook: add source-specific top-level columns to the ``extraction_runs`` row.

        Default no-op. Overridden by sources with extra columns (USCG /
        USCG-manufacturer add ``was_short_circuited``).
        """

    def _augment_response_row(self, row: dict[str, Any]) -> None:
        """Hook: add source-specific response-capture columns to the row.

        Default no-op. Called only when a response was captured (i.e.
        ``_captured_response_status_code is not None``). Overridden by NHTSA to add
        ``response_inner_content_sha256``.
        """

    # --- Template orchestration ---

    def run(self, change_type: str = "routine") -> ExtractionResult:
        """
        Execute the full 5-step extraction lifecycle.

        Steps 1, 2, and 5 are retried on transient failures.
        Steps 3 and 4 are pure/deterministic — retrying would fail identically.
        Raises ExtractionAbortedError if batch rejection rate exceeds threshold.

        `change_type` flows through to extraction_runs so downstream history
        models (Phase 6 recall_event_history) can filter parser-driven re-version
        waves out of edit detection. See ADR 0027 + ADR 0028.
        """
        run_id = str(uuid.uuid4())
        started_at = datetime.now(UTC)
        # Reset per-run presence state; set once invariants have run (see _record_run).
        self._passing_records = None
        structlog.contextvars.bind_contextvars(source=self.source_name, run_id=run_id)
        log = logger.bind(source=self.source_name, run_id=run_id)

        result: ExtractionResult | None = None
        try:
            log.info("extraction.started")

            log.info("extraction.extract.started")
            raw_records: list[dict[str, Any]] = _TRANSIENT_RETRY(self.extract)
            log.info("extraction.extract.completed", count=len(raw_records))

            log.info("extraction.land_raw.started")
            raw_landing_path: str = _R2_RETRY(self.land_raw, raw_records)
            log.info("extraction.land_raw.completed", path=raw_landing_path)

            log.info("extraction.validate.started")
            valid_records, schema_rejects = self.validate_records(raw_records)
            log.info(
                "extraction.validate.completed",
                valid=len(valid_records),
                rejected=len(schema_rejects),
            )

            log.info("extraction.check_invariants.started")
            passing_records, invariant_rejects = self.check_invariants(valid_records)
            # Stash for the presence manifest (ADR 0026) — these are the records present
            # in this run's response (quarantined rejects excluded per ADR 0026 Q2).
            self._passing_records = passing_records
            log.info(
                "extraction.check_invariants.completed",
                passing=len(passing_records),
                rejected=len(invariant_rejects),
            )

            all_quarantined = schema_rejects + invariant_rejects

            log.info("extraction.load_bronze.started")
            rows_inserted: int = _TRANSIENT_RETRY(
                self.load_bronze, passing_records, all_quarantined, raw_landing_path
            )
            log.info("extraction.load_bronze.completed", rows_inserted=rows_inserted)

            result = ExtractionResult(
                source=self.source_name,
                run_id=run_id,
                records_fetched=len(raw_records),
                records_landed=len(raw_records),
                records_valid=len(valid_records),
                records_rejected_validate=len(schema_rejects),
                records_rejected_invariants=len(invariant_rejects),
                records_loaded=rows_inserted,
                raw_landing_path=raw_landing_path,
            )

            if result.rejection_rate > self.rejection_threshold:
                log.warning(
                    "extraction.rejection_threshold_exceeded",
                    rate=result.rejection_rate,
                    threshold=self.rejection_threshold,
                )
                self._record_run(run_id, started_at, "aborted", result, change_type=change_type)
                raise ExtractionAbortedError(
                    self.source_name, result.rejection_rate, self.rejection_threshold
                )

            log.info("extraction.completed", **vars(result))
            self._record_run(run_id, started_at, "success", result, change_type=change_type)
            return result

        except ExtractionAbortedError:
            raise
        except Exception as exc:
            self._record_run(
                run_id, started_at, "failed", error_message=str(exc), change_type=change_type
            )
            raise
        finally:
            structlog.contextvars.unbind_contextvars("source", "run_id")


# --- Operation-type subclasses ---


class RestApiExtractor[T: BaseModel](Extractor[T]):
    """
    Base for extractors that pull from JSON REST APIs (CPSC, FDA, USDA).
    Adds HTTP-specific config shared across all REST sources.
    """

    base_url: str
    timeout_seconds: float = 30.0
    rate_limit_rps: float | None = None  # None = no rate limiting enforced

    # The five _captured_response_* PrivateAttrs are inherited from Extractor.
    # _capture_response() (populated once per run; paginated sources call it only on the
    # first page so the headers carry conditional-GET semantics) supports the
    # ETag-viability study at scripts/sql/_pipeline/etag_viability.sql.
    def _capture_response(self, response: httpx.Response, body: bytes | None = None) -> None:
        """Stash response metadata for persistence to extraction_runs.

        ``body`` defaults to ``response.content``; pass explicitly when the
        body has already been consumed (streaming, decoded JSON, etc.) to
        avoid double-reading. Call exactly once per run, on the first/primary
        response — multiple invocations within the same run overwrite earlier
        captures.
        """
        body_bytes = body if body is not None else response.content
        self._captured_response_status_code = response.status_code
        self._captured_response_etag = response.headers.get("etag")
        self._captured_response_last_modified = response.headers.get("last-modified")
        # 304 Not Modified responses carry no body. Writing sha256(b"") would
        # be technically correct but surfaces as a phantom false-304 in
        # etag_viability.sql when compared against the prior 200's real body
        # hash (observed 2026-05-10 on usda_establishments). Persist NULL so
        # downstream analysis can distinguish "no body to hash" from "empty
        # body's hash."
        self._captured_response_body_sha256 = (
            None if response.status_code == 304 else hashlib.sha256(body_bytes).hexdigest()
        )
        self._captured_response_headers = dict(response.headers)

    def parse_landed_payload(self, raw_bytes: bytes) -> list[dict[str, Any]]:
        """REST JSON sources land ``json.dumps(raw_records)``, so the inverse is ``json.loads``.

        Returns the same ``list[dict]`` shape ``validate_records`` consumes (see each concrete
        REST extractor's ``land_raw`` — ``content = json.dumps(raw_records, default=str)``).
        """
        decoded: Any = json.loads(raw_bytes)
        if not isinstance(decoded, list):
            raise ValueError(
                f"{self.source_name}: landed payload is not a JSON array of records "
                f"(got {type(decoded).__name__})."
            )
        return decoded


# NOTE: ``HtmlScrapingExtractor`` lives in ``src/extractors/_html_scraping.py``
# as of Phase 5d Step 2 (first webscraped source: USCG). Previously declared
# here as a stub; promoted to its own module alongside ``_flat_file.py`` once
# concrete usage surfaced. Import from ``src.extractors._html_scraping``.
