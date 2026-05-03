"""Capture HTTP response metadata on extraction_runs

Revision ID: 0010
Revises: 0009
Create Date: 2026-05-03

Adds five nullable columns to extraction_runs to support the ETag viability
study (see scripts/sql/_pipeline/etag_viability/). Every successful HTTP fetch
now persists status code, ETag, Last-Modified, full response headers, and a
SHA-256 of the raw response body — enough to correlate the server's claims
about change against the bronze-hash ground truth, and to fingerprint whether
the response was served from origin or a CDN cache layer.

Columns are universal across sources (CPSC / FDA / USDA / USDA establishments,
plus future NHTSA / USCG) so the same forensic queries work for any source
that wants to exercise this study.

Forward-only by convention. The downgrade body exists for completeness but
should not be relied upon.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_status_code",
            sa.Integer,
            nullable=True,
            comment=(
                "HTTP status code of the (first) response — 200 normally; "
                "304 once etag_enabled flips on."
            ),
        ),
    )
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_etag",
            sa.Text,
            nullable=True,
            comment="ETag header from the response, verbatim including quotes / W/ prefix.",
        ),
    )
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_last_modified",
            sa.Text,
            nullable=True,
            comment="Last-Modified header from the response, RFC 1123 format.",
        ),
    )
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_body_sha256",
            sa.Text,
            nullable=True,
            comment=(
                "SHA-256 of the raw response body bytes, lowercase hex. "
                "Authoritative ground-truth oracle for 'did the data change?' — "
                "covers inserts, updates, and deletes simultaneously."
            ),
        ),
    )
    op.add_column(
        "extraction_runs",
        sa.Column(
            "response_headers",
            postgresql.JSONB,
            nullable=True,
            comment=(
                "Full response headers as JSONB. Promoted ETag / Last-Modified live in "
                "their own columns; X-Cache, Age, Server, Via and any forensic header "
                "are queryable here without another migration."
            ),
        ),
    )


def downgrade() -> None:
    op.drop_column("extraction_runs", "response_headers")
    op.drop_column("extraction_runs", "response_body_sha256")
    op.drop_column("extraction_runs", "response_last_modified")
    op.drop_column("extraction_runs", "response_etag")
    op.drop_column("extraction_runs", "response_status_code")
