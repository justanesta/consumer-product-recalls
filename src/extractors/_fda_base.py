"""Shared transport base for the FDA iRES extractors (bulk recalls + press releases).

``FdaExtractor`` (bulk ``POST /recalls/``) and ``FdaPressReleaseExtractor`` (per-event
``GET /search/pressreleaseurls/{id}``) both hit ``www.accessdata.fda.gov/rest/iresapi``
behind FDA's Akamai anti-abuse layer and share the iRES transport: the Mozilla UA
(finding N), Authorization-User/Key headers, the ``STATUSCODE`` envelope (400 success /
412 empty / 401 auth), the ``text/html`` anti-abuse throttle signal, R2 error capture, and
a date-string ``source_watermarks.last_cursor``. This base holds the single copy; concrete
subclasses provide ``source_name``, ``base_url`` (+ loader config), the request shape (bulk
POST vs per-event GET), and the five lifecycle methods.

Source-specific text (the ``source_watermarks`` row, the error-capture log event) is keyed
off ``self.source_name`` so the one implementation serves both accurately. Mirrors
``_fsis_base.py`` (the FSIS conditional-GET family base).

``model_post_init`` (engine + R2 wiring) is intentionally left to each subclass — it is two
lines, and keeping it local keeps ``R2LandingClient`` referenced in each extractor's own
module (so the existing per-module test patches stay valid). The shared *logic* — not the
wiring — is what lives here.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import httpx  # noqa: TC002 — runtime import kept; httpx.Response is a method annotation
import sqlalchemy as sa
import structlog
from pydantic import BaseModel, PrivateAttr

from src.config.settings import (
    Settings,  # noqa: TC001 — Pydantic evaluates field annotations at runtime
)
from src.extractors._base import AuthenticationError, RestApiExtractor
from src.extractors._tables import source_watermarks as _source_watermarks
from src.landing.r2 import (
    R2LandingClient,  # noqa: TC001 — Pydantic evaluates the PrivateAttr annotation at runtime
)

logger = structlog.get_logger()

# FDA's own iRES Python sample sets this exact UA; the default python-httpx UA is
# suspected to trip the anti-abuse throttle on the first request (finding N).
IRES_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# FDA STATUSCODE semantics (finding A / K): 400 = success, 412 = empty, 401 = auth.
STATUS_SUCCESS = 400
STATUS_EMPTY = 412
STATUS_AUTH_DENIED = 401

DEFAULT_LOOKBACK_DAYS = 1


class FdaIresExtractor[T: BaseModel](RestApiExtractor[T]):
    """Transport base for the FDA iRES extractors — shared auth headers, R2 error
    capture, and the date-cursor watermark helpers. Subclasses wire engine/R2 in their
    own ``model_post_init`` and implement the request shape + lifecycle methods."""

    settings: Settings

    _engine: sa.Engine = PrivateAttr()
    _r2_client: R2LandingClient = PrivateAttr()
    # Stored by land_raw() so validate_records()/check_invariants() can reference it.
    _current_landing_path: str = PrivateAttr(default="")

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
        """Land an error response to R2 so promote_error_to_cassette.py can emit a cassette.

        Captures the request body when present (the bulk POST's form-encoded ``payLoad=``);
        a GET (empty body) lands ``request_body=None``. Best-effort — a failure here is
        logged under ``<source_name>.error_capture_failed``, not raised.
        """
        request_body: str | None = None
        if response.request.content:
            try:
                request_body = response.request.content.decode("utf-8")
            except UnicodeDecodeError:
                request_body = None
        try:
            self._r2_client.land_error_response(
                source=self.source_name,
                request_method=response.request.method,
                request_url=url,
                request_body=request_body,
                status_code=response.status_code,
                response_headers=dict(response.headers),
                response_body=response.text,
            )
        except Exception:
            logger.warning(
                f"{self.source_name}.error_capture_failed",
                status_code=response.status_code,
                url=url,
            )

    def _get_watermark(self, conn: sa.Connection) -> date:
        """Read the date cursor from ``source_watermarks.last_cursor`` for this source."""
        row = conn.execute(
            sa.select(_source_watermarks.c.last_cursor).where(
                _source_watermarks.c.source == self.source_name
            )
        ).fetchone()
        if row and row[0]:
            return date.fromisoformat(row[0])
        return datetime.now(UTC).date() - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    def _update_watermark(self, conn: sa.Connection, new_date: date) -> None:
        conn.execute(
            sa.update(_source_watermarks)
            .where(_source_watermarks.c.source == self.source_name)
            .values(last_cursor=new_date.isoformat(), updated_at=datetime.now(UTC))
        )
