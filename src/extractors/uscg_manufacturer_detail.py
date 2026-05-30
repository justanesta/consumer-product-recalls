"""USCG manufacturer detail-page enrichment extractor (Phase 5d Step 7, Path B).

Captures the per-manufacturer **detail page** at
``https://uscgboating.org/content/manufacturers-identification-detail.php?id=N``
into ``uscg_manufacturer_details_bronze`` — the ~20 fields the detail page
exposes beyond the 5 listing fields, including the source-native succession
lineage (``Past Company 1-3 (OOB year)``, ``In Business``, ``Out of Business``,
``Date Modified``, ``Parent MIC``, ``DBA``) and the full untruncated address.

Architecture (``project_scope/phase-5d-uscg-manufacturers-detail.md``):

- **Separate source/table** from the listing-only ``uscg_manufacturers``
  (this is bronze-capture only; the SCD-2 silver dim + the time-sensitive
  recall→manufacturer join are Phase 6 / ADR 0035, NOT built here).
- **Work-list sourced from bronze.** Unlike the listing extractor (which walks
  ~651 paginated pages), this fetches one detail page per MIC. The work-list of
  ``(mic, uscg_directory_id, detail_url)`` tuples comes from
  ``uscg_manufacturers_bronze``:
    - **Incremental (Tier-1):** only MICs whose latest listing row is newer than
      their latest detail row (or have no detail row yet) — a listing-delta
      cursor using existing ``extraction_timestamp``s, no new watermark column.
      A reassignment (company/address change) bumps the listing row and re-queues
      the MIC; detail-only drift (a ``Date Modified`` bump with no listing change)
      is caught by the deep-rescan full sweep.
    - **Deep-rescan (Tier-2):** the full ~16.3k-row sweep
      (``UscgManufacturerDetailDeepRescanLoader`` overrides ``_work_list``).

- **Parser** (``_parse_details_page``): the validated probe parser
  (``scripts/uscg/probe_mic_reassignment_rate.py``) promoted to production. The
  detail page lays each row out as 5 cells
  ``[label][value][&nbsp; spacer][label][value]``; the value is the label cell's
  **immediate** next-sibling ``<td>`` (do NOT skip an empty value cell to the
  spacer — that bled the next label in). **Production drift fence:** unlike the
  exploratory probe (which records unknown labels and continues), an unknown
  bolded label RAISES ``TransientExtractionError`` (mirrors the recalls details
  parser, ``src/extractors/uscg.py``).

- ``_parse_listing_page`` raises ``NotImplementedError`` — this extractor never
  walks listing pages (inverse of the listing extractor, whose
  ``_parse_details_page`` is the guarded one).

Identity + hash: single-column ``source_recall_id`` (= MIC). ``detail_url`` and
``uscg_directory_id`` are hash-excluded (URL-scheme-rewrite defense + page-offset
instability), same as the listing extractor. ``date_modified`` IS in the hash
(the Path B change signal).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
import structlog
from bs4 import BeautifulSoup, Tag
from pydantic import PrivateAttr, ValidationError
from sqlalchemy.dialects import postgresql

from src.bronze.invariants import check_null_source_id
from src.bronze.loader import BronzeLoader
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    ExtractionResult,
    QuarantineRecord,
    TransientExtractionError,
)
from src.extractors._html_scraping import HtmlScrapingExtractor
from src.landing.r2 import R2LandingClient
from src.schemas.uscg_manufacturer_detail import UscgManufacturerDetailRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_details_bronze = sa.Table(
    "uscg_manufacturer_details_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("company_name", sa.Text),
    sa.Column("dba", sa.Text),
    sa.Column("parent_company", sa.Text),
    sa.Column("parent_mic", sa.Text),
    sa.Column("past_company_1", sa.Text),
    sa.Column("past_company_2", sa.Text),
    sa.Column("past_company_3", sa.Text),
    sa.Column("address", sa.Text),
    sa.Column("city", sa.Text),
    sa.Column("state", sa.Text),
    sa.Column("zip", sa.Text),
    sa.Column("country", sa.Text),
    sa.Column("phone", sa.Text),
    sa.Column("fax", sa.Text),
    sa.Column("status", sa.Text),
    sa.Column("company_official", sa.Text),
    sa.Column("type", sa.Text),
    sa.Column("additional_address", sa.Text),
    sa.Column("in_business", sa.TIMESTAMP(timezone=True)),
    sa.Column("out_of_business", sa.TIMESTAMP(timezone=True)),
    sa.Column("date_modified", sa.TIMESTAMP(timezone=True)),
    sa.Column("uscg_directory_id", sa.Integer),
    sa.Column("detail_url", sa.Text),
)

_details_rejected = sa.Table(
    "uscg_manufacturer_details_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

# The listing bronze table — read-only here, for the work-list query.
_manufacturers_bronze = sa.Table(
    "uscg_manufacturers_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("uscg_directory_id", sa.Integer),
    sa.Column("detail_url", sa.Text),
)

_source_watermarks = sa.Table(
    "source_watermarks",
    _metadata,
    sa.Column("source", sa.Text, primary_key=True),
    sa.Column("last_cursor", sa.Text),
    sa.Column("last_etag", sa.Text),
    sa.Column("last_successful_extract_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("last_records_count", sa.Integer),
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
    sa.Column("response_inner_content_sha256", sa.Text),
    sa.Column("was_short_circuited", sa.Boolean),
)

_SOURCE = "uscg_manufacturer_details"
_BASE_URL = "https://uscgboating.org/content"
_DETAIL_URL = f"{_BASE_URL}/manufacturers-identification-detail.php"

# Safeguard for the work-list size. Corpus is ~16,263 (Finding D); 30,000 is
# ~2× headroom. Hit before then = a pathological work-list (join blow-up or
# upstream record-count explosion); abort. Mirrors the listing extractor's
# _MAX_PAGES = 2000 (~3× the ~651-page corpus).
_MAX_DETAIL_ROWS = 30_000

# Normalized detail-page label → parser/schema key. Promoted from the validated
# probe ``_LABEL_MAP`` (scripts/uscg/probe_mic_reassignment_rate.py). An observed
# bolded label NOT in this map signals HTML drift and raises in
# ``_parse_details_page`` — the production drift fence (mirrors uscg.py recalls).
_DETAIL_LABEL_MAP: dict[str, str] = {
    "mic": "mic",  # validation_alias → source_recall_id
    "company": "company",  # validation_alias → company_name
    "dba": "dba",
    "parent company": "parent_company",
    "parent mic": "parent_mic",
    "past company 1": "past_company_1",
    "past company 2": "past_company_2",
    "past company 3": "past_company_3",
    "address": "address",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "country": "country",
    "phone": "phone",
    "fax": "fax",
    "status": "status",
    "company official": "company_official",
    "in business": "in_business",
    "out of business": "out_of_business",
    "date modified": "date_modified",
    "type": "type",
    "additional address": "additional_address",
}


def _normalize_label(text: str) -> str:
    """Normalize a detail-page ``<strong>`` label for ``_DETAIL_LABEL_MAP`` lookup.

    Strip whitespace, strip a trailing colon, lowercase — same rule as the
    recalls details parser (``uscg.py:_normalize_label``). Inconsistent colon
    usage on the source page is the reason for the trailing-colon strip.
    """
    return text.strip().rstrip(":").strip().lower()


def _value_for_label(strong: Tag) -> str | None:
    """Return the value paired with a ``<strong>LABEL</strong>`` cell, or None.

    5-cell row layout ``[left-label][left-value][&nbsp; spacer][right-label]
    [right-value]``: the value is the label cell's **immediate** next-sibling
    ``<td>``. An empty / ``&nbsp;``-only value cell yields ``None`` (verbatim
    empties are normalized to NULL at parse time, matching the listing parser's
    ``... or None``). Critically we do NOT skip empty cells — the spacer sits
    AFTER the value, so skipping would bleed the next label in (the empty-cell
    bug that produced ``parent_company == "Parent MIC:"``).
    """
    label_cell = strong.find_parent(["td", "th"])
    if isinstance(label_cell, Tag):
        value_cell = label_cell.find_next_sibling("td")
        if isinstance(value_cell, Tag):
            # `&nbsp;` decodes to U+00A0 which str.strip() removes → None.
            return value_cell.get_text(" ", strip=True) or None
    return None


class UscgManufacturerDetailExtractor(HtmlScrapingExtractor[UscgManufacturerDetailRecord]):
    """Extractor for the USCG manufacturer detail pages — incremental (Tier-1) path.

    Fetches one detail page per MIC for the listing-delta work-list (MICs whose
    listing row changed since their last detail fetch, or never fetched). For a
    full ~16.3k-row historical sweep use ``UscgManufacturerDetailDeepRescanLoader``.
    """

    source_name: str = _SOURCE
    start_url: str = _DETAIL_URL  # required by HtmlScrapingExtractor; the per-row
    # detail URLs come from the bronze work-list, not pagination of start_url.
    settings: Settings
    # Optional cap on the work-list (CLI ``--limit``). None = full work-list.
    # Two uses: (1) cheap dev validation — a handful of detail pages exercises
    # fetch → R2 → bronze → dbt end-to-end without the full ~4.5h sweep; (2)
    # chunked/resumable seeding — repeated capped runs march through the corpus
    # in uscg_directory_id order because the incremental cursor + content-hash
    # dedup skip already-loaded MICs each pass. Inherited by the deep-rescan loader.
    work_list_limit: int | None = None

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
        """Fetch the work-list's detail pages; return one dict per manufacturer.

        Work-list comes from ``_work_list`` (Tier-1 listing-delta for the
        incremental path; full sweep for deep-rescan). An empty work-list (no
        listing changes since the last detail run) is a legitimate no-op —
        ``extract`` returns ``[]`` and ``land_raw`` handles the empty archive.
        """
        with self._engine.connect() as conn:
            work = self._work_list(conn)

        if len(work) > _MAX_DETAIL_ROWS:
            raise TransientExtractionError(
                f"USCG manufacturer-detail work-list returned {len(work)} rows — exceeds "
                f"guard of {_MAX_DETAIL_ROWS}. Possible cause: work-list join blow-up or "
                "upstream record-count explosion."
            )

        # ``--limit`` (CLI): cap AFTER the blow-up guard (which must see the true
        # work-list size). Slicing is safe when the limit >= work-list size.
        if self.work_list_limit is not None:
            full_size = len(work)
            work = work[: self.work_list_limit]
            logger.info(
                "uscg_manufacturer_details.work_list_limited",
                limit=self.work_list_limit,
                fetching=len(work),
                work_list_size=full_size,
            )

        records: list[dict[str, Any]] = []
        for idx, item in enumerate(work):
            detail_url = item["detail_url"]
            response, body = self._fetch_page(detail_url)
            if idx == 0:
                # Capture the first detail response as the run's forensic anchor.
                self._capture_response_metadata(response, body)
            self._archive_page(detail_url, response, body)

            parsed = self._parse_details_page(body, detail_url)
            # Lineage added from the work-list (not parsed from the page).
            parsed["uscg_directory_id"] = item["uscg_directory_id"]
            parsed["detail_url"] = detail_url
            records.append(parsed)

        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """Upload archived detail pages as NDJSON to R2. Caches landing path.

        On a no-op incremental run (empty work-list → no archived pages),
        skip the R2 write and return an empty sentinel path; ``load_bronze``
        then runs with zero records (a clean no-op that still touches freshness).
        """
        if not self._archived_pages:
            self._current_landing_path = ""
            return ""
        path = super().land_raw(raw_records)
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UscgManufacturerDetailRecord], list[QuarantineRecord]]:
        """Instantiate ``UscgManufacturerDetailRecord`` per row; quarantine ValidationErrors.

        ``extra='forbid', strict=True`` (ADR 0014) catches schema drift not
        surfaced by the parser drift fence (e.g. an unexpected key).
        """
        valid: list[UscgManufacturerDetailRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UscgManufacturerDetailRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        # Parser key "mic" → source_recall_id via validation_alias;
                        # on a malformed row it may be absent/non-string.
                        source_recall_id=str(record.get("mic") or "") or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UscgManufacturerDetailRecord]
    ) -> tuple[list[UscgManufacturerDetailRecord], list[QuarantineRecord]]:
        """Apply the null-id invariant. No date_sanity invariant (dates are nullable)."""
        passing: list[UscgManufacturerDetailRecord] = []
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
        records: list[UscgManufacturerDetailRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Write valid records to bronze; route quarantine to rejected table.

        Identity: ``source_recall_id`` (= MIC). Hash excludes ``detail_url`` +
        ``uscg_directory_id`` (same as the listing extractor); ``date_modified``
        is intentionally IN the hash (the Path B change signal). Touches
        freshness so monitoring sees the run; deep-rescan overrides to skip it.
        """
        loader = BronzeLoader(
            bronze_table=_details_bronze,
            rejected_table=_details_rejected,
            identity_fields=("source_recall_id",),
            hash_exclude_fields=frozenset({"detail_url", "uscg_directory_id"}),
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._touch_freshness(conn)
        return count

    # --- Work-list ---

    def _work_list(self, conn: sa.Connection) -> list[dict[str, Any]]:
        """Tier-1 incremental work-list: MICs needing a detail (re)fetch.

        A MIC needs a detail fetch when it has no detail row yet, or its latest
        listing row is newer than its latest detail row (a listing-side change —
        e.g. a reassignment — re-queues it). Uses existing ``extraction_timestamp``s
        as the cursor; no new ``source_watermarks`` column required.
        """
        return self._build_work_list(conn, incremental=True)

    def _build_work_list(self, conn: sa.Connection, *, incremental: bool) -> list[dict[str, Any]]:
        """Shared work-list builder. ``incremental=False`` is the full sweep."""
        mb = _manufacturers_bronze
        db = _details_bronze

        # Exactly one row per MIC — its latest listing version. Uses
        # row_number() rather than max(ts)+self-join because a single listing
        # run stamps every row with the same batch timestamp (BronzeLoader uses
        # one now() per load), so max(ts)+join would return ALL of a MIC's rows
        # from the latest run. If that run wrote a MIC twice with different
        # uscg_directory_ids (page-offset instability when rows shift mid-walk —
        # see the _sources.yml content-hash note), we still pick ONE deterministic
        # row instead of fanning out into duplicate detail fetches. The id-desc
        # tiebreak resolves same-timestamp duplicates.
        ranked = sa.select(
            mb.c.source_recall_id.label("mic"),
            mb.c.uscg_directory_id.label("uscg_directory_id"),
            mb.c.detail_url.label("detail_url"),
            mb.c.extraction_timestamp.label("listing_ts"),
            sa.func.row_number()
            .over(
                partition_by=mb.c.source_recall_id,
                order_by=[mb.c.extraction_timestamp.desc(), mb.c.id.desc()],
            )
            .label("rn"),
        ).subquery()
        listing_latest = (
            sa.select(
                ranked.c.mic,
                ranked.c.uscg_directory_id,
                ranked.c.detail_url,
                ranked.c.listing_ts,
            )
            .where(ranked.c.rn == 1)
            .subquery()
        )

        stmt = sa.select(
            listing_latest.c.mic,
            listing_latest.c.uscg_directory_id,
            listing_latest.c.detail_url,
        )

        if incremental:
            detail_max = (
                sa.select(
                    db.c.source_recall_id.label("mic"),
                    sa.func.max(db.c.extraction_timestamp).label("ts"),
                )
                .group_by(db.c.source_recall_id)
                .subquery()
            )
            stmt = stmt.select_from(
                listing_latest.outerjoin(detail_max, listing_latest.c.mic == detail_max.c.mic)
            ).where(
                sa.or_(
                    detail_max.c.ts.is_(None),
                    detail_max.c.ts < listing_latest.c.listing_ts,
                )
            )
        else:
            stmt = stmt.select_from(listing_latest)

        # No .distinct() needed — listing_latest is already one row per MIC (rn==1).
        stmt = stmt.order_by(listing_latest.c.uscg_directory_id)
        rows = conn.execute(stmt).all()
        return [
            {
                "mic": row.mic,
                "uscg_directory_id": row.uscg_directory_id,
                "detail_url": row.detail_url,
            }
            for row in rows
        ]

    # --- Parsers ---

    def _parse_details_page(self, body: bytes, page_url: str) -> dict[str, Any]:
        """Parse a manufacturer detail page → label→value dict.

        Walks all ``<strong>`` / ``<b>`` elements; each one's text is normalized
        and looked up in ``_DETAIL_LABEL_MAP``. The value is the label cell's
        immediate next-sibling ``<td>`` (``_value_for_label``).

        Drift fence: an observed bolded label NOT in ``_DETAIL_LABEL_MAP`` raises
        ``TransientExtractionError`` — the production behavior that differs from
        the exploratory probe (which records unknown labels and continues).
        The ``<h2>COMPANY</h2>`` page title is an ``<h2>``, not picked up here.
        """
        soup = BeautifulSoup(body, "lxml")
        result: dict[str, Any] = {}
        for strong in soup.find_all(["strong", "b"]):
            if not isinstance(strong, Tag):  # pragma: no cover — bs4 typing defense
                continue
            label_raw = strong.get_text(strip=True)
            if not label_raw:
                continue
            normalized = _normalize_label(label_raw)
            if normalized not in _DETAIL_LABEL_MAP:
                raise TransientExtractionError(
                    f"USCG manufacturer detail page {page_url}: unknown label "
                    f"{label_raw!r} (normalized to {normalized!r}). Phase 5d Step 7 "
                    "drift fence — extend _DETAIL_LABEL_MAP + the schema and "
                    "re-baseline before resuming."
                )
            result[_DETAIL_LABEL_MAP[normalized]] = _value_for_label(strong)
        return result

    def _parse_listing_page(self, body: bytes, page_url: str) -> list[dict[str, Any]]:
        """Detail extractor never walks listing pages — guard against misuse.

        The work-list comes from ``uscg_manufacturers_bronze`` (see
        ``_build_work_list``); ``extract`` only fetches detail pages. The raise
        is defense-in-depth against a future refactor wiring this into a listing
        walk (inverse of the listing extractor's guarded ``_parse_details_page``).
        """
        raise NotImplementedError(
            "UscgManufacturerDetailExtractor fetches detail pages from a bronze "
            "work-list; it never parses listing pages. If you see this raised, a "
            "refactor wired the wrong walk — restore detail-only behavior."
        )

    # --- Private helpers ---

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump ``last_successful_extract_at`` for the detail source.

        Deep-rescan does not touch this (a rebaseline shouldn't reset freshness
        alerts) — matches the NHTSA/USDA/USCG convention.
        """
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _SOURCE)
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
        change_type: str = "routine",
    ) -> None:
        """Persist a row to ``extraction_runs`` with USCG forensic columns.

        Mirrors ``UscgManufacturerExtractor._record_run`` minus the
        short-circuit column — this source has no ``Records Found`` precheck, so
        ``was_short_circuited`` stays NULL (per migration 0014 semantics: NULL
        for sources that don't implement a short-circuit). The detail page emits
        no ``Last-Modified`` / ``ETag`` (Finding E) — those columns persist NULL.
        """
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
            logger.warning(
                "extraction_run.record_failed",
                run_id=run_id,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )


class UscgManufacturerDetailDeepRescanLoader(UscgManufacturerDetailExtractor):
    """Historical / deep-rescan loader for the USCG manufacturer detail pages.

    Tier-2 full sweep — fetches every MIC's detail page (vs the incremental
    Tier-1 listing-delta), catching detail-only drift (a ``Date Modified`` bump
    with no listing change) that the listing-delta cursor misses. Behavioral
    differences vs the incremental path:
      - ``_work_list`` returns the full corpus (no listing-vs-detail delta filter).
      - ``load_bronze`` does NOT call ``_touch_freshness`` (a rebaseline run is
        invisible to freshness monitoring — same convention as the listing
        deep-rescan loader).
    """

    def _work_list(self, conn: sa.Connection) -> list[dict[str, Any]]:
        """Deep-rescan fetches every MIC's latest listing row — full sweep."""
        return self._build_work_list(conn, incremental=False)

    def load_bronze(
        self,
        records: list[UscgManufacturerDetailRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Same loader config as the incremental path; skip the freshness touch."""
        loader = BronzeLoader(
            bronze_table=_details_bronze,
            rejected_table=_details_rejected,
            identity_fields=("source_recall_id",),
            hash_exclude_fields=frozenset({"detail_url", "uscg_directory_id"}),
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
        return count
