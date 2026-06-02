"""USCG manufacturer-directory webscraper (Phase 5d Step 7).

Architecture per ``documentation/uscg/manufacturer_scraping_observations.md``:

- **Incremental path** (``UscgManufacturerExtractor``): fetches all ~651
  listing pages of the manufacturer directory at
  ``https://uscgboating.org/content/manufacturers-identification.php``. No
  per-manufacturer detail walk — Finding C established that the detail page
  exists at ``manufacturers-identification-detail.php?id=N`` but the listing
  carries the 5 fields we need (MIC, Company, Address, City, State). Address
  is truncated at ~30 chars (Finding F.1, source DB VARCHAR constraint) —
  documented limitation, full address recovery deferred to a future per-row
  enrichment pass.

  Reuses USCG-recalls' Finding J short-circuit pattern: the directory page
  exposes the same ``Records Found: NNNN`` footer (Finding D, value 16263),
  generic ``source_watermarks.last_records_count`` (migration 0014) is the
  second consumer, no new schema needed.

- **Deep-rescan path** (``UscgManufacturerDeepRescanLoader``): structurally
  identical — same fetches, same parsing. Differs only in (a) not touching
  ``last_successful_extract_at`` (matches NHTSA/USDA/USCG-recalls deep-rescan
  convention so freshness alerts don't reset on a rebaseline run), (b) not
  advancing ``source_watermarks.last_records_count`` (rebaseline shouldn't
  shift the short-circuit baseline), and (c) ``_should_short_circuit``
  always returns False (deep-rescan exists specifically to force a full walk).

Identity (Finding B [MIC natural key] + Finding J [cross-source join
validation against recalls.mic]): single-column ``source_recall_id`` derived
from the MIC column (regulatory 3-char alphanumeric per USCG-2013-0133-0005).
The detail-page URL ``id=`` query parameter is captured separately as
``uscg_directory_id`` (lineage only; hash-excluded since it's page-offset-
deterministic and would churn the content_hash on every re-crawl when
records are added/removed earlier in the alphabetical ordering).

Hash excludes: ``detail_url`` (defense against URL-scheme rewrites — mirrors
``UscgScrapingExtractor``'s ``details_url`` exclusion); ``uscg_directory_id``
(page-offset-deterministic instability).

Parser strategy:
- Listing page: BeautifulSoup with the lxml backend finds the manufacturers
  table by its header cells (the table itself has no id/class attribute —
  Finding A). Headers are validated against ``expected_columns`` (drift fence).
  Each data row carries ``class="defaultFont"``; ``id`` query parameter from
  the MIC cell's anchor href is extracted as ``uscg_directory_id``. End-of-
  pagination signal is an empty parse-result list (the URL gracefully returns
  no data rows past the corpus end).

- Details page: ``_parse_details_page`` raises ``NotImplementedError``. The
  listing-only walk in ``extract()`` never invokes it; the raise is
  defense-in-depth against accidental invocation by future refactors. If
  detail-page extraction ever becomes desirable, this is where the parser
  goes.

Forensic capture: ``_capture_response_metadata`` called once on the page-0
listing response. The directory page (Finding E) emits no ``Last-Modified``
and no ``ETag`` (stronger ``Cache-Control: no-store, no-cache, must-revalidate``
than recalls); those columns persist as NULL by design.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse

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
from src.schemas.uscg_manufacturer import UscgManufacturerRecord

logger = structlog.get_logger()

# --- Module-level SQLAlchemy table metadata ---
_metadata = sa.MetaData()

_manufacturers_bronze = sa.Table(
    "uscg_manufacturers_bronze",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("content_hash", sa.Text),
    sa.Column("extraction_timestamp", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
    sa.Column("company_name", sa.Text),
    sa.Column("address", sa.Text),
    sa.Column("city", sa.Text),
    sa.Column("state", sa.Text),
    sa.Column("uscg_directory_id", sa.Integer),
    sa.Column("detail_url", sa.Text),
)

_manufacturers_rejected = sa.Table(
    "uscg_manufacturers_rejected",
    _metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("source_recall_id", sa.Text),
    sa.Column("raw_record", sa.JSON),
    sa.Column("failure_reason", sa.Text),
    sa.Column("failure_stage", sa.Text),
    sa.Column("rejected_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("raw_landing_path", sa.Text),
)

# ``_source_watermarks`` (imported above) and ``extraction_runs`` are the shared
# cross-source operational tables — see src/extractors/_tables.py.

_SOURCE = "uscg_manufacturers"
_BASE_URL = "https://uscgboating.org/content"
_LISTING_URL = f"{_BASE_URL}/manufacturers-identification.php"

# Default safeguard for the page walk: current corpus is ~16,263 records across
# ~651 pages (Finding D). 2000 pages = ~3× current; hit before then = pathological
# pagination / drift; abort. The 3× margin matches the recalls precedent
# (uscg.py: 200 pages vs ~71-page current corpus). Production never overrides
# this; cassette tests construct with ``max_pages=N`` to bound walks against
# recorded subsets.
_MAX_PAGES = 2000

# Sanity guard for the record count. Corpus has been stable around ~16,263;
# 30,000 catches a runaway upstream change without firing on multi-year organic
# growth. Not applied to the deep-rescan path.
_MAX_INCREMENTAL_RECORDS = 30_000

# Default expected listing-page table headers (Finding A). The YAML config
# overrides via ``expected_columns`` — this default applies only when YAML
# doesn't specify and provides a safe-by-default drift fence. Unlike recalls
# (which has " Company Name" with a leading space), the manufacturer table
# headers have NO leading/trailing whitespace inside the <strong> tags.
_EXPECTED_LISTING_COLUMNS = (
    "MIC",
    "Company",
    "Address",
    "City",
    "State",
)

# Regex for the ``Records Found: NNNN`` cell rendered below the listing table
# on every paginated manufacturer-directory page (Finding D, value 16263).
# Identical regex to recalls; the watermark column is shared via migration 0014.
_RECORDS_FOUND_RE = re.compile(r"Records Found:\s*(\d+)")


class UscgManufacturerExtractor(HtmlScrapingExtractor[UscgManufacturerRecord]):
    """Extractor for USCG boat-manufacturer directory — incremental path.

    Strategy: walk all ~651 listing pages of the manufacturer directory; do
    NOT fetch per-manufacturer detail pages. Bronze content-hash dedup
    (ADR 0007) absorbs no-op runs; the Finding K ``Records Found`` short-
    circuit (shared infrastructure with USCG recalls' Finding J; migration 0014)
    skips the walk when the directory hasn't changed since the last run.

    For historical seeding / forced re-ingestion use
    ``UscgManufacturerDeepRescanLoader`` (symmetric — same fetches, different
    watermark semantics).
    """

    source_name: str = _SOURCE
    start_url: str = _LISTING_URL
    settings: Settings
    max_pages: int = _MAX_PAGES

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    _current_landing_path: str = PrivateAttr(default="")

    # Finding K short-circuit state (analog of USCG-recalls' Finding J).
    # Populated as a side effect of ``_parse_listing_page``; consumed by
    # ``_should_short_circuit``.
    _records_found_total: int | None = PrivateAttr(default=None)
    _was_short_circuited: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        self._engine = sa.create_engine(
            self.settings.neon_database_url.get_secret_value(),
            pool_pre_ping=True,
        )
        self._r2_client = R2LandingClient(self.settings)

    # --- Lifecycle methods ---

    def extract(self) -> list[dict[str, Any]]:
        """Walk listing pages; return one dict per manufacturer row.

        Listing-only — no per-manufacturer detail-page fetches (Path A per
        ``manufacturer_scraping_observations.md`` § "Step 2 architectural
        decisions").

        Stop condition: a page with zero data rows signals end-of-pagination.
        Drift guard: abort if page-count exceeds ``self.max_pages`` (default
        ``_MAX_PAGES`` = 2000, ~3× current corpus).

        Returns:
            One dict per manufacturer row, with keys matching
            ``UscgManufacturerRecord`` field names + validation aliases —
            ``"mic"`` flows through ``validation_alias`` to ``source_recall_id``,
            ``"company"`` to ``company_name``.
        """
        records: list[dict[str, Any]] = []
        page_idx = 0
        while page_idx < self.max_pages:
            page_url = f"{self.start_url}?pageNum_manufacturers={page_idx}"
            response, body = self._fetch_page(page_url)
            if page_idx == 0:
                # Capture page-0 listing as the run's forensic anchor.
                self._capture_response_metadata(response, body)
            self._archive_page(page_url, response, body)

            listing_rows = self._parse_listing_page(body, page_url)
            if not listing_rows:
                # End-of-pagination signal.
                break

            # Finding K short-circuit (page-0 precheck; analog of USCG-recalls'
            # Finding J). Mirrors ``UscgScrapingExtractor.extract`` at the
            # same lifecycle point.
            if page_idx == 0 and self._should_short_circuit(listing_rows):
                self._was_short_circuited = True
                return []

            records.extend(listing_rows)
            page_idx += 1
        else:
            # Loop exited without break — page count exceeded guard.
            raise TransientExtractionError(
                f"USCG manufacturer-directory walk exceeded {self.max_pages} pages "
                "without end-of-pagination signal. Possible cause: pagination "
                "logic drift, or upstream record-count explosion."
            )

        if len(records) > _MAX_INCREMENTAL_RECORDS:
            raise TransientExtractionError(
                f"USCG manufacturer-directory walk returned {len(records)} records — "
                f"exceeds guard of {_MAX_INCREMENTAL_RECORDS}. Possible cause: "
                "upstream pagination/duplication drift, or organic growth that "
                "warrants raising the ceiling."
            )

        return records

    def land_raw(self, raw_records: list[dict[str, Any]]) -> str:
        """Upload archived pages as single NDJSON to R2. Caches landing path."""
        path = super().land_raw(raw_records)
        self._current_landing_path = path
        return path

    def validate_records(
        self, raw_records: list[dict[str, Any]]
    ) -> tuple[list[UscgManufacturerRecord], list[QuarantineRecord]]:
        """Instantiate ``UscgManufacturerRecord`` per row; route ValidationErrors to quarantine.

        Pydantic ``extra='forbid', strict=True`` (ADR 0014) catches schema
        drift not surfaced by the upstream parser (e.g., an unexpected key
        from a future HTML change that slipped through ``_parse_listing_page``).
        """
        valid: list[UscgManufacturerRecord] = []
        quarantined: list[QuarantineRecord] = []
        for record in raw_records:
            try:
                valid.append(UscgManufacturerRecord.model_validate(record))
            except ValidationError as exc:
                quarantined.append(
                    QuarantineRecord(
                        # Parser-emitted key ``"mic"`` flows to source_recall_id
                        # via validation_alias; on malformed rows may be absent
                        # or non-string. Fall back to None.
                        source_recall_id=str(record.get("mic") or "") or None,
                        raw_record=record,
                        failure_reason=str(exc),
                        failure_stage="validate_records",
                        raw_landing_path=self._current_landing_path,
                    )
                )
        return valid, quarantined

    def check_invariants(
        self, records: list[UscgManufacturerRecord]
    ) -> tuple[list[UscgManufacturerRecord], list[QuarantineRecord]]:
        """Apply the null-id invariant.

        No date_sanity invariant — the listing exposes no dates.
        """
        passing: list[UscgManufacturerRecord] = []
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
        records: list[UscgManufacturerRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Write valid records to bronze; route quarantine to rejected table.

        Identity: single-column ``source_recall_id`` (= MIC).
        Hash excludes: ``detail_url`` (URL-scheme rewrite defense) and
        ``uscg_directory_id`` (page-offset-deterministic instability — see
        Finding B / module docstring).
        """
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_SOURCE],
            bronze_table=_manufacturers_bronze,
            rejected_table=_manufacturers_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
            self._touch_freshness(conn)
            self._update_records_count(conn)
        return count

    # --- Parsers ---

    def _parse_listing_page(self, body: bytes, page_url: str) -> list[dict[str, Any]]:
        """Parse a manufacturer-directory listing page; return one dict per row.

        Drift fence: validates table headers against ``self.expected_columns``
        (case-sensitive, whitespace-stripped). Mismatch raises
        ``TransientExtractionError``.

        Empty return list = end-of-pagination signal for the caller's walk
        loop (the URL gracefully returns no data rows past the corpus end).

        Side effect (Finding D): if the page contains the ``Records Found: NNNN``
        cell, ``self._records_found_total`` is updated. The cell renders on
        every paginated listing page so any successful parse populates the
        value. Used by the page-0 short-circuit precheck in ``extract()``.
        """
        # Side-effect: capture Records Found total before structural parsing.
        # Done early so even if structural parse later raises (drift fence),
        # the total is still available for diagnostic logging.
        match = _RECORDS_FOUND_RE.search(body.decode("utf-8", errors="replace"))
        if match:
            self._records_found_total = int(match.group(1))

        soup = BeautifulSoup(body, "lxml")
        # The manufacturers table has no id/class (Finding A); disambiguate
        # from header nav / search-form / footer tables by structural signal —
        # the only table whose first <tr> has a DIRECT <td><strong>MIC</strong></td>
        # as its first cell. Critical: use ``recursive=False`` for the td
        # search so we don't pick up <strong>MIC</strong> from descendant
        # tables. The live USCG page nests the manufacturers table inside
        # a content-wrapper table, and the wrapper's first <tr> contains
        # the manufacturers-table descendants — without recursive=False
        # the finder selects the wrapper instead of the inner listing table.
        # The drift-fence's full-header check below still validates all 5
        # column names; this finder just ensures we land on the right table.
        table = None
        for candidate in soup.find_all("table"):
            if not isinstance(candidate, Tag):  # pragma: no cover — bs4 typing defense
                continue
            first_tr = candidate.find("tr")
            if not isinstance(first_tr, Tag):
                continue
            direct_tds = first_tr.find_all("td", recursive=False)
            if not direct_tds:
                continue
            first_td = direct_tds[0]
            if not isinstance(first_td, Tag):
                continue
            first_strong = first_td.find("strong")
            if isinstance(first_strong, Tag) and first_strong.get_text(strip=True) == "MIC":
                table = candidate
                break
        if table is None or not isinstance(table, Tag):
            raise TransientExtractionError(
                f"USCG manufacturer-directory page {page_url}: no table with 'MIC' "
                "header found. Possible cause: HTML structure drift (Finding A reference)."
            )

        # Header row is the first <tr> in the table (bare, no class). Cells
        # contain <strong>HEADER</strong> per Finding A. Use recursive=False
        # on the td search to scope to direct cells of the header row (a
        # nested table inside the header row, if one ever appears, would
        # otherwise leak its <td>s into the observed-headers tuple).
        rows = table.find_all("tr", recursive=False)
        if not rows:
            raise TransientExtractionError(
                f"USCG manufacturer-directory page {page_url}: table has no rows."
            )
        header_row = rows[0]
        if not isinstance(header_row, Tag):  # pragma: no cover — bs4 typing defense
            raise TransientExtractionError(
                f"USCG manufacturer-directory page {page_url}: header row is not a Tag."
            )
        observed_headers = tuple(
            strong.get_text(strip=True)
            for td in header_row.find_all("td", recursive=False)
            if isinstance(td, Tag)
            for strong in td.find_all("strong")
            if isinstance(strong, Tag)
        )
        expected = tuple(self.expected_columns or _EXPECTED_LISTING_COLUMNS)
        if observed_headers != expected:
            raise TransientExtractionError(
                f"USCG manufacturer-directory listing-page schema drift at {page_url}: "
                f"expected headers {expected}, got {observed_headers}. "
                "Phase 5d Step 7 drift fence — re-baseline the parser + "
                "expected_columns config before resuming extracts."
            )

        # Data rows: <tr class="defaultFont">. recursive=False on the cell
        # search scopes to direct <td> children of the data row.
        parsed: list[dict[str, Any]] = []
        for row in rows[1:]:
            if not isinstance(row, Tag):  # pragma: no cover — bs4 typing defense
                continue
            cls = row.get("class") or []
            if "defaultFont" not in cls:
                continue
            cells = row.find_all("td", recursive=False)
            if len(cells) != len(expected):
                # A malformed row (wrong cell count) is structural drift —
                # cleanest to abort the run rather than emit partial data.
                raise TransientExtractionError(
                    f"USCG manufacturer-directory page {page_url}: data row has "
                    f"{len(cells)} cells; expected {len(expected)}."
                )

            # MIC cell wraps the value in an <a> (Finding B). Anchor text is
            # the MIC; href ``id=N`` query parameter is the USCG internal
            # sequential row PK (uscg_directory_id).
            mic_cell = cells[0]
            if not isinstance(mic_cell, Tag):
                continue
            anchor = mic_cell.find("a")
            if not isinstance(anchor, Tag):
                # No anchor in MIC cell — drift or placeholder row. Log and
                # skip; downstream row-count guards catch systematic loss.
                logger.warning(
                    "uscg_manufacturer.parse.missing_anchor",
                    page_url=page_url,
                )
                continue
            href = str(anchor.get("href") or "")
            mic = anchor.get_text(strip=True)
            # Defensive: detect placeholder rows analogous to USCG-recalls'
            # Finding L (out-of-range pagination renders rows with empty
            # ``id=`` query parameter). The manufacturer directory has not
            # been observed to emit these in Step 1 probes, but symmetry with
            # recalls protects against a future backend alignment.
            if not mic or href.endswith("id=") or href == "":
                if not mic:
                    logger.warning(
                        "uscg_manufacturer.parse.empty_mic",
                        page_url=page_url,
                        href=href,
                    )
                continue

            # Parse the ``id`` query parameter from the href as uscg_directory_id.
            uscg_directory_id: int | None = None
            try:
                qs = parse_qs(urlparse(href).query)
                id_values = qs.get("id", [])
                if id_values:
                    uscg_directory_id = int(id_values[0])
            except (ValueError, TypeError):
                # Non-integer id — leave as None and let downstream investigate.
                uscg_directory_id = None

            # Absolutize the detail URL — listing href is relative
            # ("manufacturers-identification-detail.php?id=..."); we want the
            # full URL on bronze rows.
            detail_url = href if href.startswith("http") else f"{_BASE_URL}/{href}"

            parsed.append(
                {
                    "mic": mic,
                    "company": (
                        cells[1].get_text(strip=True) or None if isinstance(cells[1], Tag) else None
                    ),
                    "address": (
                        cells[2].get_text(strip=True) or None if isinstance(cells[2], Tag) else None
                    ),
                    "city": (
                        cells[3].get_text(strip=True) or None if isinstance(cells[3], Tag) else None
                    ),
                    "state": (
                        cells[4].get_text(strip=True) or None if isinstance(cells[4], Tag) else None
                    ),
                    "uscg_directory_id": uscg_directory_id,
                    "detail_url": detail_url,
                }
            )
        return parsed

    def _parse_details_page(self, body: bytes, page_url: str) -> dict[str, Any]:
        """Manufacturer directory uses listing-only extraction (Finding C decision).

        ``extract()`` never invokes this method — the walk loop stays on
        listing pages only. The raise is defense-in-depth against accidental
        invocation by future refactors. If detail-page extraction ever becomes
        desirable, replace this with a parser keyed on the per-manufacturer
        detail-page structure (open question for Step 1 follow-up — does
        ``manufacturers-identification-detail.php?id=N`` return useful content
        when fetched directly vs in iframe context?).
        """
        raise NotImplementedError(
            "USCG manufacturer directory uses listing-only extraction "
            "(see manufacturer_scraping_observations.md Finding C). The walk "
            "loop in extract() never invokes _parse_details_page. If you "
            "see this raised, a future refactor accidentally enabled the "
            "details walk — restore listing-only behavior or implement this "
            "parser against the actual detail-page HTML shape."
        )

    # --- Private helpers ---

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump ``last_successful_extract_at`` so monitoring sees the run as fresh.

        Deep-rescan does not touch this field (matches NHTSA/USDA/USCG-recalls
        convention so freshness alerts don't reset on a rebaseline run).
        """
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _SOURCE)
            .values(
                last_successful_extract_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )

    def _read_last_records_count(self, conn: sa.Connection) -> int | None:
        """Read ``source_watermarks.last_records_count`` for uscg_manufacturers.

        Returns ``None`` on the first ever run (column NULL by default after
        migration 0014) — caller falls through to the full walk;
        ``_update_records_count`` populates for next time.
        """
        result = conn.execute(
            sa.select(_source_watermarks.c.last_records_count).where(
                _source_watermarks.c.source == _SOURCE
            )
        )
        row = result.first()
        return row[0] if row is not None else None

    def _update_records_count(self, conn: sa.Connection) -> None:
        """Persist the latest observed ``Records Found: NNNN`` to the watermark.

        Idempotent on short-circuit (new value equals existing). Only writes
        when ``_records_found_total`` is populated — protects against
        pathological cases where the listing-page parser succeeded but the
        regex failed.

        Deep-rescan overrides ``load_bronze`` to skip this call (same
        convention as ``_touch_freshness``).
        """
        if self._records_found_total is None:
            return
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == _SOURCE)
            .values(
                last_records_count=self._records_found_total,
                updated_at=datetime.now(UTC),
            )
        )

    def _should_short_circuit(self, page_0_rows: list[dict[str, Any]]) -> bool:
        """Two-gate Finding K precheck (analog of USCG-recalls' Finding J) —
        return True to short-circuit the walk.

        Gate 1 (count): parsed ``Records Found: NNNN`` equals prior run's
        ``source_watermarks.last_records_count``.
        Gate 2 (membership): every MIC on page 0 already exists in
        ``uscg_manufacturers_bronze``.

        Both gates must pass. Either failing falls through to the full walk.

        Defensive returns False:
          - First ever run (``last_records_count`` IS NULL) — no baseline.
          - ``_records_found_total`` IS None — parser regex failed.
          - ``page_0_rows`` is empty — defensive; ``extract()`` already breaks
            on empty rows before this is called.

        Misses by design: a per-manufacturer field edit (e.g., address change
        without adding/removing a row) is invisible to both gates. Weekly
        safety-net ``deep-rescan uscg_manufacturers --change-type=schema_rebaseline``
        catches those, same pattern as recalls.
        """
        if not page_0_rows or self._records_found_total is None:
            return False
        with self._engine.connect() as conn:
            prior_count = self._read_last_records_count(conn)
            if prior_count is None or self._records_found_total != prior_count:
                return False
            page_0_ids = [r["mic"] for r in page_0_rows]
            result = conn.execute(
                sa.select(sa.func.count(sa.distinct(_manufacturers_bronze.c.source_recall_id)))
                .select_from(_manufacturers_bronze)
                .where(_manufacturers_bronze.c.source_recall_id.in_(page_0_ids))
            )
            distinct_ids_in_bronze = result.scalar_one()
        return distinct_ids_in_bronze == len(page_0_ids)

    def _augment_run_row(self, row: dict[str, Any]) -> None:
        """Add the USCG manufacturer short-circuit flag to the extraction_runs row.

        TRUE on the page-0 precheck short-circuit path, FALSE on full walks. Other
        sources leave this column NULL.
        """
        row["was_short_circuited"] = self._was_short_circuited


class UscgManufacturerDeepRescanLoader(UscgManufacturerExtractor):
    """Historical / deep-rescan loader for USCG manufacturer directory.

    **Symmetry-only** — structurally identical to ``UscgManufacturerExtractor``.
    Unlike NHTSA's deep-rescan (which adds a PRE_2010 URL), manufacturers has
    no second corpus to merge in: the listing already shows all ~16,263 records.
    Exists so the CLI ``recalls deep-rescan uscg_manufacturers`` works uniformly
    across sources and operators have a documented path for
    ``--change-type=schema_rebaseline`` or ``--change-type=historical_seed`` runs.

    Behavioral differences vs the incremental path:
      - ``load_bronze`` does NOT call ``_touch_freshness`` (freshness alerts
        don't reset on a rebaseline run).
      - ``load_bronze`` does NOT call ``_update_records_count`` (deep-rescan
        is invisible to monitoring; advancing the count could let the next
        incremental short-circuit prematurely).
      - ``_should_short_circuit`` is overridden to always return False:
        deep-rescan exists specifically to force a full walk.
    """

    def _should_short_circuit(self, page_0_rows: list[dict[str, Any]]) -> bool:
        """Deep-rescan never short-circuits — always do the full walk."""
        return False

    def load_bronze(
        self,
        records: list[UscgManufacturerRecord],
        quarantined: list[QuarantineRecord],
        raw_landing_path: str,
    ) -> int:
        """Same loader config as the incremental path; skip freshness + count touch."""
        loader = BronzeLoader.from_contract(
            DEDUP_CONTRACT_BY_SOURCE_NAME[_SOURCE],
            bronze_table=_manufacturers_bronze,
            rejected_table=_manufacturers_rejected,
        )
        with self._engine.begin() as conn:
            count = loader.load(conn, records, quarantined, raw_landing_path)  # type: ignore[arg-type]
        return count
