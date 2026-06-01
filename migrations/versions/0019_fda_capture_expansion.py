"""add Phase 6a.5 capture-expansion columns to fda_recalls_bronze

Revision ID: 0019
Revises: 0018
Create Date: 2026-05-31

The FDA field audit (documentation/fda/field_audit_2026_w22.md §7a) marked 11
bulk-POST fields as SHIP that the original 22-field capture omitted:
`codeinformation` (lot/serial — landing-page critical), the 8 firm-address fields
(`firmcitynam`, `firmcountrynam`, `firmline1adr`, `firmline2adr`, `firmpostalcd`,
`firmstatecd`, `firmstateprvncnam`), `firmsurvivingnam` / `firmsurvivingfei`
(firm-rename continuity for Phase 6b firm resolution), and `postedinternetdt`.

These are added to the bronze capture BEFORE the Phase 6a.5 historical seed so the
one-time, Akamai-risky ~134k-record FDA pull lands everything silver + Phase 6b
will need — R2 replay (ADR 0028 Mechanism B) cannot recover columns that were
never requested, so capturing later would force a second full re-pull. The plan's
FDA storage estimate (1.5–3 GB) was already revised up for `codeinformation`, so
the seed was implicitly sized for the expanded set.

All columns are nullable: probe-window populations range from 0% (`firm_line2_adr`)
through ~15% (`firm_surviving_*`) and ~84% (`posted_internet_dt`) to ~100% (city /
country). `fda_recalls_bronze` currently holds zero rows on the production `main`
branch, so there is nothing to backfill and no content-hash churn. Silver mapping
and cross-source column-naming for these fields are deferred to the (b)
capture-expansion PR; this migration only lands the raw bronze bytes.
"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "fda_recalls_bronze"

# (column_name, type) — order matches _DISPLAY_COLUMNS / FdaRecord field order.
# `Any` for the type slot: sa.types.TypeEngine is invariant in its parameter, so a
# heterogeneous (Text / BigInteger / TIMESTAMP) tuple won't satisfy TypeEngine[object].
_COLUMNS: tuple[tuple[str, Any], ...] = (
    ("code_information", sa.Text()),
    ("firm_city_nam", sa.Text()),
    ("firm_country_nam", sa.Text()),
    ("firm_line1_adr", sa.Text()),
    ("firm_line2_adr", sa.Text()),
    ("firm_postal_cd", sa.Text()),
    ("firm_state_cd", sa.Text()),
    ("firm_state_prvnc_nam", sa.Text()),
    ("firm_surviving_nam", sa.Text()),
    ("firm_surviving_fei", sa.BigInteger()),
    ("posted_internet_dt", sa.TIMESTAMP(timezone=True)),
)


def upgrade() -> None:
    for name, type_ in _COLUMNS:
        op.add_column(_TABLE, sa.Column(name, type_, nullable=True))


def downgrade() -> None:
    for name, _type in reversed(_COLUMNS):
        op.drop_column(_TABLE, name)
