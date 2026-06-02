"""USCG boating-recall webscraper (Phase 5d Step 2).

Architecture per ``documentation/uscg/scraping_observations.md``:

- **Incremental path** (``UscgScrapingExtractor``): fetches all 71
  listing pages (``?pageNum_allRecalls=0..70``) then a details page
  per recall (~1,763 details pages on first run). ADR 0007 content-hash
  dedup absorbs no-op weeks; steady-state cost is ~72s/run for ~71
  listing fetches plus ~handful of new details (USCG ships ~0-10 new
  recalls/week per Finding J's bursty profile).

- **Deep-rescan path** (``UscgDeepRescanLoader``): structurally
  identical to the incremental path — same fetches, same parsing.
  Differs only in (a) not touching ``last_successful_extract_at``
  (matches NHTSA/USDA deep-rescan convention so freshness alerts
  don't reset on a rebaseline run) and (b) the operator using a
  non-routine ``--change-type`` (``schema_rebaseline`` for a parser
  change, ``historical_seed`` for the initial corpus load). Kept as
  a separate class for symmetry with NHTSA/FDA/USDA precedent — there
  is no structural difference vs the incremental path (no second URL
  to load, no second corpus to merge); see class docstring.

Identity (per Phase 5d Step 1 Finding C, calibrated to Step 1.5 corpus
probe): ``source_recall_id`` alone — USCG's "Number" column with
year-prefix encoding (e.g. ``26MF0158`` / ``25CG0017``). Two-sample
sanity confirms uniqueness within the 25-row probe windows; the
corpus-wide check landed in Step 1.5. Single-column identity, unlike
NHTSA's 11-tuple.

Hash excludes: ``details_url`` (per Plan-agent critique — defense
against future URL-scheme rewrites; cosmetic field, not data).
``last_date`` is **not** excluded — Finding D confirmed two consecutive
fetches are byte-stable so the field is a real lifecycle date, not a
server-render timestamp. Revisit at Step 3 if cross-day observations
show otherwise.

Parser strategy:
- Listing page: BeautifulSoup with the lxml backend finds the second
  ``<table>`` (the first is pagination links), validates headers
  against ``expected_columns`` (drift fence), then iterates rows.
  The pagination-boundary signal (Finding L) is an empty ``id``
  parameter in the row anchor's href — those rows are skipped, the
  empty result list signals end-of-pagination to the walk loop.

- Details page: each ``<strong>LABEL</strong>`` element has a
  following ``<span class="defaultFont">VALUE</span>`` sibling.
  Labels (with inconsistent trailing colons; see Finding B) get
  normalized via the ``_DETAILS_LABEL_MAP`` lookup. Unknown labels
  raise ``TransientExtractionError`` — schema-drift fence per
  Phase 5d Step 2 requirement.

Forensic capture: ``_capture_response_metadata`` called once on the
**page-0 listing** response. USCG's response (Finding K) has no
``Last-Modified`` and no ``ETag`` — those columns persist as NULL,
which is the correct semantics. ``response_inner_content_sha256`` is
left NULL (HTML has no wrapper/inner distinction).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
import structlog
from bs4 import BeautifulSoup, Tag
from pydantic import PrivateAttr, ValidationError

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME
from src.bronze.invariants import (
    PER_RECORD_INVARIANTS_BY_SOURCE_NAME,
    run_per_record_invariants,
)
from src.bronze.loader import BronzeLoader
from src.config.db import make_engine
from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    QuarantineRecord,
    TransientExtractionError,
)
from src.extractors._html_scraping import HtmlScrapingExtractor
from src.extractors._tables import source_watermarks as _source_watermarks
from src.landing.r2 import R2LandingClient
from src.schemas.uscg import UscgRecallRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_uscg_bronze = sa.Table(
    "uscg_recalls_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("company_name", sa.Text),
    sa.Column("opened_on", sa.TIMESTAMP(timezone=True)),
    sa.Column("mic", sa.Text),
    sa.Column("model_name", sa.Text),
    sa.Column("problem_1", sa.Text),
    sa.Column("details_url", sa.Text),
    sa.Column("company_official", sa.Text),
    sa.Column("model_year", sa.Text),
    sa.Column("problem_2", sa.Text),
    sa.Column("hin", sa.Text),
    sa.Column("case_open_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("disposition", sa.Text),
    sa.Column("case_close_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("units", sa.Text),
    sa.Column("campaign_open_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("boat_type", sa.Text),
    sa.Column("campaign_close_date", sa.TIMESTAMP(timezone=True)),
    sa.Column("severity", sa.Text),
    sa.Column("last_date", sa.TIMESTAMP(timezone=True)),
)

_uscg_rejected = sa.Table(
    "uscg_recalls_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

# ``_source_watermarks`` (imported above, with last_records_count for the Finding J
# short-circuit total) and ``extraction_runs`` are the shared cross-source operational
# tables — see src/extractors/_tables.py.

_USCG_SOURCE = "uscg"
_USCG_BASE_URL = "https://uscgboating.org/content"
_USCG_LISTING_URL = f"{_USCG_BASE_URL}/recalls.php"

# Default safeguard for the page walk: USCG's current corpus is 1,763 records
# across 71 pages (Finding A). 200 pages = ~5,000 records, 3× current.
# Hit before then = pathological pagination / drift; abort. Exposed as the
# default for ``UscgScrapingExtractor.max_pages``; the constructor field is
# the override surface for tests (cassette suite walks 2 real pages + an
# injected empty boundary). Production never overrides this.
_MAX_PAGES = 200

# Sanity guard for the record count. The corpus has been stable around
# ~1,800; 10,000 catches a runaway upstream change without firing on
# multi-year organic growth. Not applied to the deep-rescan path.
_MAX_INCREMENTAL_RECORDS = 10_000

# Default expected listing-page table headers (Finding A). The YAML
# config overrides via ``expected_columns`` — this default applies only
# when YAML doesn't specify and provides a safe-by-default drift fence.
# Note: USCG's HTML has a leading space inside ``<strong> Company Name</strong>``
# (column 3) — strip whitespace when comparing.
_EXPECTED_LISTING_COLUMNS = (
    "Number",
    "MIC",
    "Company Name",
    "Model Name",
    "Problem 1",
    "Opened On",
)

# Regex for the ``Records Found: NNNN`` cell rendered below the listing
# table on every paginated USCG listing page (Finding J). Used by Step 6's
# short-circuit precheck — Gate 1 compares this against the prior run's
# value persisted to ``source_watermarks.last_records_count``. The leading
# space in the literal cell text (``"<td> Records Found: 01763</td>"``) is
# tolerated by ``\s*``; the count is zero-padded but ``int()`` strips that.
_RECORDS_FOUND_RE = re.compile(r"Records Found:\s*(\d+)")

# Map from normalized details-page label text → dict key emitted by the
# parser. Normalization rule: strip whitespace, strip trailing ":",
# lowercase. Inconsistent colon usage on the source page (Finding B) is
# the reason for the trailing-colon strip.
#
# The keys here must match either:
# (a) a UscgRecallRecord field name (e.g. ``"company_official"``), or
# (b) a UscgRecallRecord validation_alias (e.g. ``"number"`` → field
#     ``source_recall_id``).
#
# An observed label NOT in this map signals schema drift and raises
# ``TransientExtractionError`` in ``_parse_details_page``.
_DETAILS_LABEL_MAP: dict[str, str] = {
    "number": "number",  # validation_alias → source_recall_id
    "mic": "mic",
    "company": "company_name",  # details says "Company"; listing says "Company Name"; one canonical
    "company official": "company_official",
    "model name": "model_name",
    "model year": "model_year",
    "problem 1": "problem_1",
    "problem 2": "problem_2",
    "hin": "hin",
    "case open date": "case_open_date",
    "disposition": "disposition",
    "case close date": "case_close_date",
    "units": "units",
    "campaign open date": "campaign_open_date",
    "boat type": "boat_type",
    "campaign close date": "campaign_close_date",
    "severity": "severity",
    "last date": "last_date",
}


def _normalize_label(text: str) -> str:
    """Normalize HTML label text for the ``_DETAILS_LABEL_MAP`` lookup.

    Strip surrounding whitespace, strip trailing colon, lowercase.
    Inconsistent colon usage on the source page (Finding B) — some
    labels end with ``:``, some don't.
    """
    return text.strip().rstrip(":").strip().lower()


# Note: a ``_check_year_prefix_consistency`` invariant existed here from
# Phase 5d Step 2 → removed in Step 3 (2026-05-17). The hypothesis it
# encoded — that ``source_recall_id[:2]`` always matches
# ``opened_on.year % 100`` — was empirically falsified for 218/1763 ≈
# 12.4% of the corpus across at least three distinct mechanisms
# documented in Finding G (replaced) of
# ``documentation/uscg/scraping_observations.md``:
#   * fiscal-year prefixes on Oct-Dec openings (FY runs Oct 1 → Sep 30)
#   * prefix = opened_on_year − 1 (likely number-on-filing,
#     opened_on advances later when investigation begins)
#   * multi-year offsets for re-issued / amended recalls
#   * the Unix-epoch sentinel pattern (Finding O — listing renders
#     "1970-01-01" for the no-date case)
# No single invariant captures these; removed rather than relaxed.
# Evidence: ``scripts/sql/uscg/bronze/diagnose_rejections.sql`` Q5/Q6.


class UscgScrapingExtractor(HtmlScrapingExtractor[UscgRecallRecord]):
    """Extractor for USCG boating recalls — incremental path.

    Strategy: walk all 71 listing pages, fetch every recall's details
    page, merge listing + details into one row per recall. Bronze
    content-hash dedup (ADR 0007) absorbs no-op weeks.

    For historical seeding / forced re-ingestion use
    ``UscgDeepRescanLoader`` (symmetric — same fetches, different
    watermark semantics).
    """

    source_name: str = _USCG_SOURCE
    start_url: str = _USCG_LISTING_URL
    settings: Settings
    # Production safety guard for the page walk. Default at the module
    # constant ``_MAX_PAGES``; cassette tests construct with ``max_pages=N``
    # to bound the walk against a recorded subset of the corpus. Hitting
    # ``max_pages`` without an empty-row boundary raises
    # ``TransientExtractionError`` per the original guard semantics.
    max_pages: int = _MAX_PAGES

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")

    # Phase 5d Step 6 — Finding J short-circuit state. ``_records_found_total``
    # is populated as a side effect of ``_parse_listing_page`` (regex match
    # on the page text). ``_was_short_circuited`` is the flag persisted to
    # ``extraction_runs.was_short_circuited`` by ``_record_run``.
    _records_found_total: int | None = PrivateAttr(default=None)
    _was_short_circuited: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        self._engine = make_engine(self.settings.neon_database_url.get_secret_value())
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """Walk listing pages, fetch details for each recall, return merged dicts.

        Stop condition (Finding L): a page with zero real rows (every
        ``<a href="recalls-details.php?id=...">`` having empty ``id``)
        signals end-of-pagination. Drift guard: abort if page-count
        exceeds ``self.max_pages`` (default ``_MAX_PAGES``).

        Returns:
            One dict per recall, with merged listing + details fields.
            Keys match ``UscgRecallRecord`` field names + validation
            aliases — ``"number"`` flows through validation_alias to
            ``source_recall_id``.
        """
        records: list[dict[str, Any]] = []
        page_idx = 0
        while page_idx < self.max_pages:
            page_url = f"{self.start_url}?pageNum_allRecalls={page_idx}"
            response, body = self._fetch_page(page_url)
            if page_idx == 0:
                # Capture page-0 listing as the run's forensic anchor
                # (Phase 5d convention; see _record_run docstring).
                self._capture_response_metadata(response, body)
            self._archive_page(page_url, response, body)

            listing_rows = self._parse_listing_page(body, page_url)
            if not listing_rows:
                # End-of-pagination signal (Finding L).
                break

            # Phase 5d Step 6 — Finding J short-circuit. After page 0 is
            # fetched, parsed (which sets ``_records_found_total`` as a side
            # effect), and archived (so the run still has a forensic anchor
            # in R2), check the two-gate precheck. If both gates pass, return
            # ``[]`` so the parent ``Extractor.run()`` template no-ops through
            # validate / invariants / load_bronze; ``_touch_freshness`` still
            # runs in load_bronze; ``_record_run`` persists the short-circuit
            # flag. Mirrors USDA's 304-Not-Modified short-circuit pattern at
            # ``src/extractors/usda.py:195-227``.
            if page_idx == 0 and self._should_short_circuit(listing_rows):
                self._was_short_circuited = True
                return []

            for listing_row in listing_rows:
                details_url = listing_row["details_url"]
                detail_response, detail_body = self._fetch_page(details_url)
                self._archive_page(details_url, detail_response, detail_body)
                details_row = self._parse_details_page(detail_body, details_url)
                # Merge: details fields overwrite listing for the
                # overlapping keys (number, mic, company_name,
                # model_name, problem_1) since the details page has
                # the un-truncated values. Listing-only fields
                # (opened_on, details_url) carry through.
                merged = {**listing_row, **details_row}
                records.append(merged)

            page_idx += 1
        else:
            # Loop exited without break — page count exceeded guard.
            raise TransientExtractionError(
                f"USCG listing walk exceeded {self.max_pages} pages without "
                "end-of-pagination signal. Possible cause: pagination "
                "logic drift, or upstream record-count explosion."
            )

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"USCG incremental walk returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. Possible "
                "cause: upstream pagination/duplication drift, or organic "
                "growth that warrants raising the ceiling."
            )

        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """Upload archived pages as single NDJSON to R2. Caches landing path."""
        path = super().land_raw(raw_records)
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UscgRecallRecord], list[QuarantineRecord]]:
        """Instantiate ``UscgRecallRecord`` per row; route ValidationErrors to quarantine.

        Pydantic's ``extra='forbid', strict=True`` catches schema drift
        not surfaced by the upstream parser (e.g., an unexpected key
        from a future HTML change that slipped through ``_parse_*``).
        """
        valid: list[UscgRecallRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UscgRecallRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        # The dict key ``"number"`` flows to source_recall_id
                        # via validation_alias; on malformed rows it may be
                        # absent or non-string. Fall back to None.
                        source_recall_id=str(record.get("number") or "") or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UscgRecallRecord]
    ) -> tuple[list[UscgRecallRecord], list[QuarantineRecord]]:
        """Apply null-id and date-sanity invariants.

        A year-prefix-consistency invariant existed here in Step 2 and
        was removed in Step 3 — see the module-level comment above the
        class and Finding G (replaced) in scraping_observations.md.
        """
        passing: list[UscgRecallRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in records:
            failure = run_per_record_invariants(
                record, PER_RECORD_INVARIANTS_BY_SOURCE_NAME[_USCG_SOURCE]
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
        records: list[UscgRecallRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Write valid records to bronze; route quarantine to rejected table.

        Identity: single-column ``source_recall_id`` per Finding C.
        Hash excludes: ``details_url`` (Plan-agent critique — defense
        against future URL-scheme rewrites).
        """
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_USCG_SOURCE],
            bronze_table=_uscg_bronze,
            rejected_table=_uscg_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._touch_freshness(conn)
            self._update_records_count(conn)
        return count

    # --- Parsers ---

    def _parse_listing_page(self, body: bytes, page_url: str) -> list[dict[str, Any]]:
        """Parse a USCG listing page; return one dict per real recall row.

        Drift fence: validates table headers against
        ``self.expected_columns`` (case-insensitive, whitespace-stripped).
        Mismatch raises ``TransientExtractionError`` so the run aborts
        before writing partial bronze.

        Pagination boundary (Finding L): rows with empty ``id`` query
        parameter on the anchor are filtered out — those are the
        placeholder rows USCG returns on out-of-range page numbers.
        Empty return list = end-of-pagination signal for the caller's
        walk loop.

        Side effect (Phase 5d Step 6 — Finding J): if the page contains
        the ``Records Found: NNNN`` cell, ``self._records_found_total``
        is updated. The cell is rendered on every paginated listing page
        so any successful parse populates the value. Used by the page-0
        short-circuit precheck in ``extract()``.
        """
        # Side-effect: capture Records Found total before structural parsing.
        # Done early so that even if the structural parse later raises (drift
        # fence), the total is still available for diagnostic logging.
        match = _RECORDS_FOUND_RE.search(body.decode("utf-8", errors="replace"))
        if match:
            self._records_found_total = int(match.group(1))

        soup = BeautifulSoup(body, "lxml")
        # The recalls table is the one whose header row contains
        # ``<strong>Number</strong>``. There are multiple tables on the
        # page (header nav, pagination); find by structural signal.
        table = None
        for candidate in soup.find_all("table"):
            if not isinstance(candidate, Tag):  # pragma: no cover — bs4 typing defense
                continue
            strongs = candidate.find_all("strong")
            if any(isinstance(s, Tag) and s.get_text(strip=True) == "Number" for s in strongs):
                table = candidate
                break
        if table is None or not isinstance(table, Tag):
            raise TransientExtractionError(
                f"USCG listing page {page_url}: no table with 'Number' header found. "
                "Possible cause: HTML structure drift (Finding A reference)."
            )

        # Header row is the first <tr> in the table. Cells contain
        # <strong>HEADER</strong> per Finding A.
        rows = table.find_all("tr")
        if not rows:
            raise TransientExtractionError(
                f"USCG listing page {page_url}: recalls table has no rows."
            )
        header_row = rows[0]
        if not isinstance(header_row, Tag):  # pragma: no cover — bs4 typing defense
            raise TransientExtractionError(
                f"USCG listing page {page_url}: header row is not a Tag instance."
            )
        observed_headers = tuple(
            strong.get_text(strip=True)
            for td in header_row.find_all("td")
            if isinstance(td, Tag)
            for strong in td.find_all("strong")
            if isinstance(strong, Tag)
        )
        expected = tuple(self.expected_columns or _EXPECTED_LISTING_COLUMNS)
        if observed_headers != expected:
            raise TransientExtractionError(
                f"USCG listing-page schema drift at {page_url}: "
                f"expected headers {expected}, got {observed_headers}. "
                "Phase 5d Step 2 drift fence — re-baseline the parser + "
                "expected_columns config before resuming extracts."
            )

        # Data rows: <tr class="defaultFont">.
        parsed: list[dict[str, Any]] = []
        for row in rows[1:]:
            if not isinstance(row, Tag):  # pragma: no cover — bs4 typing defense
                continue
            cls = row.get("class") or []
            if "defaultFont" not in cls:
                continue
            cells = row.find_all("td")
            if len(cells) != len(expected):
                # A malformed row (wrong cell count) is structural drift —
                # cleanest to abort the run rather than emit partial data.
                raise TransientExtractionError(
                    f"USCG listing page {page_url}: data row has "
                    f"{len(cells)} cells; expected {len(expected)}."
                )
            anchor = cells[0].find("a") if isinstance(cells[0], Tag) else None
            if not isinstance(anchor, Tag):
                continue
            href = str(anchor.get("href") or "")
            number = anchor.get_text(strip=True)
            # Pagination-boundary signal (Finding L): empty ``id``
            # parameter in href means USCG returned a placeholder row.
            if not number or href.endswith("id=") or href == "":
                continue
            # Absolutize the details URL — listing href is relative
            # ("recalls-details.php?id=..."); we want the full URL on
            # bronze rows + in subsequent fetches.
            details_url = href if href.startswith("http") else f"{_USCG_BASE_URL}/{href}"
            parsed.append(
                {
                    "number": number,
                    "mic": cells[1].get_text(strip=True) or None
                    if isinstance(cells[1], Tag)
                    else None,
                    "company_name": (
                        cells[2].get_text(strip=True) if isinstance(cells[2], Tag) else ""
                    ),
                    "model_name": cells[3].get_text(strip=True) or None
                    if isinstance(cells[3], Tag)
                    else None,
                    "problem_1": cells[4].get_text(strip=True) or None
                    if isinstance(cells[4], Tag)
                    else None,
                    "opened_on": (
                        cells[5].get_text(strip=True) if isinstance(cells[5], Tag) else ""
                    ),
                    "details_url": details_url,
                }
            )
        return parsed

    def _parse_details_page(self, body: bytes, page_url: str) -> dict[str, Any]:
        """Parse a USCG details page; return label→value dict.

        Strategy: walk all ``<strong>`` elements; each one's label gets
        normalized via ``_normalize_label`` and looked up in
        ``_DETAILS_LABEL_MAP``. The value comes from the next sibling
        ``<td>`` containing a ``<span class="defaultFont">``.

        Drift fence: an observed label NOT in the map raises
        ``TransientExtractionError`` per Phase 5d Step 2 requirement
        (Schema drift on HTML structure changes raises ValidationError).
        """
        soup = BeautifulSoup(body, "lxml")
        result: dict[str, Any] = {}
        observed_labels: set[str] = set()

        for strong in soup.find_all("strong"):
            if not isinstance(strong, Tag):  # pragma: no cover — bs4 typing defense
                continue
            label_raw = strong.get_text(strip=True)
            if not label_raw:
                continue
            normalized = _normalize_label(label_raw)
            observed_labels.add(normalized)
            if normalized not in _DETAILS_LABEL_MAP:
                raise TransientExtractionError(
                    f"USCG details page {page_url}: unknown label {label_raw!r} "
                    f"(normalized to {normalized!r}). Phase 5d Step 2 drift fence — "
                    "extend _DETAILS_LABEL_MAP and re-baseline before resuming."
                )
            schema_key = _DETAILS_LABEL_MAP[normalized]

            # Walk forward in the DOM from the strong tag to find the
            # value span. The structure is
            # <td><span class="defaultFont"><strong>LABEL</strong></span></td>
            # <td><span class="defaultFont">VALUE</span></td>
            # so we ascend to the strong's parent <td>, take the next
            # sibling <td>, and find a <span class="defaultFont"> inside.
            label_td = strong.find_parent("td")
            if not isinstance(label_td, Tag):  # pragma: no cover — bs4 typing defense
                continue
            value_td = label_td.find_next_sibling("td")
            while isinstance(value_td, Tag) and not value_td.find_all("span"):
                # Skip spacer cells (<td>&nbsp;</td>).
                value_td = value_td.find_next_sibling("td")
            if not isinstance(value_td, Tag):
                result[schema_key] = None
                continue
            value_span = value_td.find("span", class_="defaultFont")
            if not isinstance(value_span, Tag):
                result[schema_key] = None
                continue
            text = value_span.get_text(strip=True)
            result[schema_key] = text or None

        return result

    # --- Private helpers ---

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump ``last_successful_extract_at`` so monitoring sees the run as fresh.

        USCG has no usable HTTP-level watermark (Finding K — no
        Last-Modified, no ETag); the watermark row exists only for
        freshness tracking. Deep-rescan does not touch this field —
        the incremental extractor owns it exclusively.
        """
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _USCG_SOURCE)
            .values(
                last_successful_extract_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )

    def _read_last_records_count(self, conn: sa.Connection) -> int | None:
        """Read ``source_watermarks.last_records_count`` for USCG.

        Returns ``None`` on the first ever run (column NULL by default after
        migration 0014) — the caller falls through to the full walk and
        ``_update_records_count`` populates the column for next time.
        """
        result = conn.execute(
            sa.select(_source_watermarks.c.last_records_count).where(
                _source_watermarks.c.source == _USCG_SOURCE
            )
        )
        row = result.first()
        return row[0] if row is not None else None

    def _update_records_count(self, conn: sa.Connection) -> None:
        """Persist the latest observed ``Records Found: NNNN`` to the watermark.

        Idempotent on short-circuit (the new value equals the existing one).
        Only writes when ``_records_found_total`` is populated — protects
        against pathological cases where the listing-page parser succeeded
        but the regex failed (unlikely, but the alternative is silently
        clobbering the watermark with NULL).

        Deep-rescan overrides ``load_bronze`` to skip this call — same
        convention as ``_touch_freshness`` (deep-rescan is invisible to
        monitoring; advancing the count would let the next incremental
        run short-circuit prematurely if the deep-rescan landed mid-cycle).
        """
        if self._records_found_total is None:
            return
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _USCG_SOURCE)
            .values(
                last_records_count=self._records_found_total,
                updated_at=datetime.now(UTC),
            )
        )

    def _should_short_circuit(self, page_0_rows: list[dict[str, Any]]) -> bool:
        """Two-gate Finding J precheck — return True to short-circuit the walk.

        Gate 1 (count): the parsed ``Records Found: NNNN`` total equals the
        prior run's value persisted in ``source_watermarks.last_records_count``.
        Gate 2 (membership): every recall ID on page 0 already exists in
        ``uscg_recalls_bronze``.

        Both gates must pass. Either failing falls through to the full walk.

        Defensive returns ``False``:
          - First ever run (``last_records_count`` IS NULL) — no baseline.
          - ``_records_found_total`` IS None — parser regex failed.
          - ``page_0_rows`` is empty — defensive; ``extract()`` already
            breaks on empty rows before this is called, so this branch
            is unreachable in practice. Documented for explicitness.

        Misses by design: a USCG details-only edit (e.g.
        ``Disposition: Open → Closed`` without touching the listing row)
        is invisible to both gates. The Phase 7 weekly safety-net
        ``deep-rescan uscg --change-type=schema_rebaseline`` cron catches
        those.
        """
        if not page_0_rows or self._records_found_total is None:
            return False
        with self._engine.connect() as conn:
            prior_count = self._read_last_records_count(conn)
            if prior_count is None or self._records_found_total != prior_count:
                return False
            page_0_ids = [r["number"] for r in page_0_rows]
            result = conn.execute(
                sa.select(sa.func.count(sa.distinct(_uscg_bronze.c.source_recall_id)))
                .select_from(_uscg_bronze)
                .where(_uscg_bronze.c.source_recall_id.in_(page_0_ids))
            )
            distinct_ids_in_bronze = result.scalar_one()
        return distinct_ids_in_bronze == len(page_0_ids)

    def _augment_run_row(self, row: dict[str, Any]) -> None:
        """Add USCG's short-circuit flag to the extraction_runs row.

        TRUE on the page-0 precheck short-circuit path, FALSE on full walks (Finding J).
        Other sources leave this column NULL.
        """
        row["was_short_circuited"] = self._was_short_circuited


class UscgDeepRescanLoader(UscgScrapingExtractor):
    """Historical / deep-rescan loader for USCG.

    **Symmetry-only** — structurally identical to ``UscgScrapingExtractor``.
    Unlike ``NhtsaDeepRescanLoader`` which adds the PRE_2010 URL,
    USCG has no second corpus to merge in: the listing already shows
    all 1,763 records. The deep-rescan exists purely so the CLI
    ``recalls deep-rescan uscg`` works uniformly across sources and
    operators have a documented path for ``--change-type=schema_rebaseline``
    or ``--change-type=historical_seed`` runs.

    Behavioral differences vs the incremental path:
      - ``load_bronze`` does NOT call ``_touch_freshness`` (matches the
        NHTSA/FDA/USDA convention so freshness alerts don't reset on a
        rebaseline run).
      - ``load_bronze`` does NOT call ``_update_records_count`` (same
        convention — deep-rescan is invisible to monitoring, and advancing
        the count mid-cycle could let the next incremental short-circuit
        prematurely).
      - ``_should_short_circuit`` is overridden to always return ``False``:
        deep-rescan exists specifically to force a full walk for
        rebaseline / historical-seed scenarios, so the short-circuit is
        semantically wrong here.
    """

    def _should_short_circuit(self, page_0_rows: list[dict[str, Any]]) -> bool:
        """Deep-rescan never short-circuits — always do the full walk."""
        return False

    def load_bronze(
        self,
        records: list[UscgRecallRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Same loader config as the incremental path; skip freshness + count touch."""
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_USCG_SOURCE],
            bronze_table=_uscg_bronze,
            rejected_table=_uscg_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
        return count
