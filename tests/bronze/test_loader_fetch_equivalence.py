"""Differential proof that the set-based staging fetch == the old chunked IN-query fetch.

This is the dedup-semantics guarantee for the NHTSA O(corpus × chunks) → O(corpus) lookup
change (`BronzeLoader._fetch_existing_hashes_staged`). It runs against a prod-clone Neon branch
(the `test_db_url` fixture forks the default branch, so the real ~241k-row `nhtsa_recalls_bronze`
is present) and asserts the two lookup mechanisms return byte-identical dicts for the same
incoming identities — including every real `bgman`/`endman` timestamptz value/format the
text-canonicalization must handle. Skips cleanly without `NEON_API_KEY` / `NEON_PROJECT_ID`.

Run the conclusive full-corpus diff before merge:  NHTSA_DEDUP_DIFF_FULL=1 pytest -k equivalence
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import sqlalchemy as sa

from src.bronze.dedup_contracts import DEDUP_CONTRACT_BY_SOURCE_NAME
from src.bronze.loader import _PG_PARAM_SAFETY_LIMIT, BronzeLoader
from src.config.db import make_engine
from src.extractors.nhtsa import _NHTSA_SOURCE, _nhtsa_bronze, _nhtsa_rejected

if TYPE_CHECKING:
    from sqlalchemy import Connection

# Distinct real identities to diff. 50k forces the old multi-chunk loop (chunk_size ≈ 5,454 for
# the 11-tuple) and spans a broad slice of real bgman/endman values; NHTSA_DEDUP_DIFF_FULL=1 diffs
# the entire corpus (the conclusive pre-merge proof).
_SAMPLE_SIZE = 50_000


def _old_chunked_fetch(
    loader: BronzeLoader, conn: Connection, identity_keys: list[tuple[str, ...]]
) -> dict[tuple[str, ...], str]:
    """Frozen copy of the pre-change multi-chunk loop — the ``old`` reference to diff against."""
    n_cols = len(loader._identity_fields)
    chunk_size = max(1, _PG_PARAM_SAFETY_LIMIT // n_cols)
    result: dict[tuple[str, ...], str] = {}
    for start in range(0, len(identity_keys), chunk_size):
        chunk = identity_keys[start : start + chunk_size]
        result.update(loader._fetch_existing_hashes_chunk(conn, chunk))
    return result


def test_staged_fetch_matches_chunked_on_real_nhtsa_bronze(test_db_url: str) -> None:
    """The staging fetch returns a dict IDENTICAL to the chunked IN-query fetch on the real
    NHTSA corpus — the dedup-equivalence guarantee. Also checks the routing entrypoint dispatches
    to the staged path for a large batch and yields the same result."""
    loader = BronzeLoader.from_contract(
        DEDUP_CONTRACT_BY_SOURCE_NAME[_NHTSA_SOURCE],
        bronze_table=_nhtsa_bronze,
        rejected_table=_nhtsa_rejected,
    )
    full = os.getenv("NHTSA_DEDUP_DIFF_FULL") == "1"
    n_fields = len(loader._identity_fields)

    engine = make_engine(test_db_url)
    with engine.begin() as conn:
        # The real incoming identity set, text-canonical exactly as load() builds it (so we diff
        # the lookups on identities that actually exist, exercising the canonicalization on real
        # timestamptz/text values rather than synthetic ones).
        text_exprs = [loader._identity_text_expr(c) for c in loader._identity_columns()]
        sample_q = sa.select(*text_exprs).distinct()
        if not full:
            sample_q = sample_q.limit(_SAMPLE_SIZE)
        identity_keys = [tuple(str(v) for v in row) for row in conn.execute(sample_q).fetchall()]
        assert identity_keys, "prod-clone branch should have NHTSA bronze rows"
        # Plus synthetic never-seen identities — both paths must agree they're absent (skipped).
        identity_keys.extend(
            [("ZZZ-absent-1", *([""] * (n_fields - 1))), ("ZZZ-absent-2", *([""] * (n_fields - 1)))]
        )

        old = _old_chunked_fetch(loader, conn, identity_keys)
        new = loader._fetch_existing_hashes_staged(conn, identity_keys)
        routed = loader._fetch_existing_hashes(conn, identity_keys)  # entrypoint → staged

    assert new == old, "staged fetch diverged from the chunked IN-query fetch"
    assert routed == old, "routing entrypoint diverged from the chunked fetch"
