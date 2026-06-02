"""Shared base for the two FSIS conditional-GET extractors.

USDA recalls (``UsdaExtractor``) and USDA establishments (``UsdaEstablishmentExtractor``)
both hit ``www.fsis.usda.gov`` behind Akamai Bot Manager, full-dump every run with no
working server-side filter (Finding D), and use ETag / ``If-Modified-Since`` conditional
GET with a stale-positive-304 contradiction guard. Their ``_fetch`` / ETag-state /
watermark / freshness helpers were byte-for-byte mirrors (the in-code "keep in sync"
note was already drifting — the establishment copy had inlined the HTTP-date parse that
the recall copy delegated to ``_parse_http_date``). This base holds the single copy;
concrete subclasses provide only what differs: ``source_name``, ``base_url`` (+loader
config) and the lifecycle methods ``extract`` / ``land_raw`` / ``validate_records`` /
``check_invariants`` / ``load_bronze``.

Source-specific text (log events, error messages, the ``source_watermarks`` row) is keyed
off ``self.source_name`` so the one implementation serves both sources accurately.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import sqlalchemy as sa
import structlog
from pydantic import BaseModel, PrivateAttr

from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import (
    AuthenticationError,
    ExtractionError,
    RateLimitError,
    RestApiExtractor,
    TransientExtractionError,
)
from src.extractors._fsis_headers import browser_headers
from src.extractors._tables import source_watermarks as _source_watermarks
from src.landing.r2 import R2LandingClient

logger = structlog.get_logger()


def _parse_http_date(s: str) -> datetime:
    """Parse an RFC 7231 IMF-fixdate header value (e.g. 'Wed, 29 Apr 2026 14:29:36 GMT')."""
    return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=UTC)


class FsisConditionalGetExtractor[T: BaseModel](RestApiExtractor[T]):
    """REST extractor base for FSIS endpoints that support ETag/Last-Modified conditional
    GET. Holds the shared engine/R2 wiring and the ETag conditional-GET machinery."""

    settings: Settings
    # Production default ON since 2026-05-09: etag_viability.sql cleared the green-light
    # gate for both FSIS endpoints (false_304_count=0; over-fetches from upstream ETag
    # re-stamping are absorbed by ADR 0007 content-hash dedup). See Finding P /
    # establishment Finding A revision. UsdaDeepRescanLoader overrides this to False.
    etag_enabled: bool = True

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    # Stored by land_raw() so validate_records()/check_invariants() can reference it
    # when building QuarantineRecords.
    _current_landing_path: str = PrivateAttr(default="")
    # Captured during extract(), applied during load_bronze() in the same txn (ADR 0020).
    # Distinct from the base-class _captured_response_* forensic state (migration 0010):
    # these drive the watermark/cache-validator write specifically.
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

    # --- Conditional-GET machinery (shared by both FSIS sources) ---

    def _fetch(
        self,
        prior_etag: str | None = None,
        prior_last_modified: str | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None, str | None]:
        """Single GET to the FSIS endpoint.

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
            raise TransientExtractionError(f"{self.source_name} network error: {exc}") from exc

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
                f"{self.source_name} API returned {response.status_code} "
                "(unexpected — no auth required)"
            )
        self._capture_error_response(response)
        raise TransientExtractionError(f"{self.source_name} API returned {response.status_code}")

    def _guard_etag_contradiction(
        self,
        prior_last_modified: str | None,
        current_last_modified: str | None,
    ) -> None:
        """Fail the run if a 304 is paired with a last-modified header that advanced past
        the prior recorded value. That combination indicates the server (or CDN cache
        layer) is returning a stale-positive 304 — the etag matched but the underlying
        dataset has actually changed. Retrying would not help; the watermark needs manual
        repair (null out source_watermarks.last_etag and re-run).
        """
        if not (prior_last_modified and current_last_modified):
            return
        if prior_last_modified == current_last_modified:
            return
        # Headers differ — could be a clock-skew artifact. Compare parsed datetimes to be
        # more tolerant; if parsing fails, treat the inequality as suspicious and raise.
        try:
            prior_dt = _parse_http_date(prior_last_modified)
            current_dt = _parse_http_date(current_last_modified)
        except ValueError:
            raise ExtractionError(
                f"{self.source_name} contradiction guard: 304 returned with advanced "
                f"last-modified header (prior={prior_last_modified!r}, "
                f"current={current_last_modified!r}). Could not parse dates; treating as a "
                f"stale-positive ETag. Manually NULL source_watermarks.last_etag for "
                f"{self.source_name} and re-run."
            ) from None
        if current_dt > prior_dt:
            raise ExtractionError(
                f"{self.source_name} contradiction guard: 304 Not Modified returned but "
                f"last-modified header advanced from {prior_last_modified!r} to "
                f"{current_last_modified!r}. This is a server-side stale-positive ETag — the "
                f"cached etag matched but the underlying dataset has changed. Manually NULL "
                f"source_watermarks.last_etag for {self.source_name} and re-run to force a "
                f"full payload fetch."
            )

    def _capture_error_response(self, response: httpx.Response) -> None:
        try:
            self._r2_client.land_error_response(
                source=self.source_name,
                request_method=response.request.method,
                request_url=str(response.request.url),
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                f"{self.source_name}.error_capture_failed",
                status_code=response.status_code,
            )

    def _read_etag_state(self) -> tuple[str | None, str | None]:
        """Return (prior_etag, prior_last_modified) from source_watermarks.

        last_cursor is repurposed for the prior last-modified header value (HTTP-date
        string): no usable date watermark exists for either FSIS endpoint (Finding D),
        so last_cursor is free to serve as the If-Modified-Since cache validator.
        """
        with self._engine.connect() as conn:
            row = conn.execute(
                sa.select(
                    _source_watermarks.c.last_etag,
                    _source_watermarks.c.last_cursor,
                ).where(_source_watermarks.c.source == self.source_name)
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
            .where(_source_watermarks.c.source == self.source_name)
            .values(**values)
        )

    def _touch_freshness(self, conn: sa.Connection) -> None:
        """Bump last_successful_extract_at on a 304 path without modifying etag/cursor."""
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == self.source_name)
            .values(
                last_successful_extract_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
