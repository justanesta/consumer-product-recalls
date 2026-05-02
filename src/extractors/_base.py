from __future__ import annotations

import abc
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog
import structlog.contextvars
import tenacity
from pydantic import BaseModel, ConfigDict, Field

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

_TRANSIENT_RETRY = tenacity.Retrying(
    retry=tenacity.retry_if_exception_type((TransientExtractionError, RateLimitError)),
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

    def _record_run(
        self,
        run_id: str,
        started_at: datetime,
        status: str,
        result: ExtractionResult | None = None,
        error_message: str | None = None,
        change_type: str = "routine",
    ) -> None:
        """Write a row to extraction_runs. Override in concrete extractors that have a DB engine.

        `change_type` is one of routine / schema_rebaseline / hash_helper_rebaseline /
        historical_seed (per ADR 0027 + ADR 0028). Default is routine; the CLI's
        --change-type flag is validated before reaching this point.
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


class FlatFileExtractor[T: BaseModel](Extractor[T]):
    """
    Base for extractors that download and parse flat files (NHTSA ZIP + TSV).
    Longer default timeout to accommodate large file downloads.
    """

    file_url: str
    timeout_seconds: float = 120.0


class HtmlScrapingExtractor[T: BaseModel](Extractor[T]):
    """
    Base for extractors that scrape HTML pages (USCG).
    scrape_delay_seconds enforces polite-scraper behavior between page requests.
    """

    start_url: str
    timeout_seconds: float = 30.0
    scrape_delay_seconds: float = 1.0
