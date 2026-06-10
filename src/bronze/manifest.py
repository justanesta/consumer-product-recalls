"""Per-run presence manifest construction (ADR 0026).

The bronze content store (ADR 0007) is a Type-4 history of *what content* we saw, but
it cannot answer *which records were present in a given run's response* — a retraction
(record absent upstream) produces zero new bronze rows, identical to "content unchanged,
dedup skipped." The presence manifest closes that gap: one row per
``(run_id, source, source_recall_id [, langcode])`` actually returned by a run, so silver
can derive ``is_currently_active`` / ``was_ever_retracted`` (ADR 0026 ``recall_lifecycle``).

This module is the **pure** half — turning a run's validated records into manifest row
dicts, no DB access (mirrors the ``filter_new_records`` / ``_dedup_within_batch`` pure
functions in :mod:`src.bronze.loader`). The write happens in ``Extractor._record_run``
(:mod:`src.extractors._base`), in the same transaction as the ``extraction_runs`` row the
manifest's ``run_id`` references — see that method for why it cannot live in the bronze
transaction.

Grain note: the manifest is RECALL-grain (``recall_id_field`` + optional ``langcode``), not
bronze-identity grain. The recall key is ``source_recall_id`` by default, but a source can override
it via ``DedupContract.presence_recall_id_field`` — NHTSA uses **campno** because its bronze
``source_recall_id`` is the regen-unstable RECORD_ID (excluded from the content hash) and
``recall_event`` joins NHTSA on campno (C16). USDA's bilingual siblings share the recall key and
diverge only on ``langcode`` (Finding F / ADR 0006), so ``langcode`` is carried when it is part of
the source's dedup identity; every other in-scope source leaves it NULL. Track-presence sources are
``{usda, nhtsa}`` (``DedupContract.default_track_presence``); WHICH run writes the manifest is gated
separately by ``Extractor.writes_presence_manifest`` (full-corpus enumerations only).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence


def build_presence_manifest_rows(
    records: Sequence[Any],
    *,
    run_id: str,
    source: str,
    langcode_field: str | None = None,
    recall_id_field: str = "source_recall_id",
) -> list[dict[str, Any]]:
    """Build deduped ``extraction_run_identities`` row dicts for one run's records.

    ``records`` are the run's **passing** records (validated + invariant-checked) — the
    ones present in the response. Quarantined records are excluded by the caller (ADR 0026
    Q2: the manifest tracks bronze-table presence, not rejects).

    Each row is ``{run_id, source, source_recall_id, langcode}``. The recall key is read from
    ``recall_id_field`` (default ``"source_recall_id"``; NHTSA passes ``"campno"`` — see the module
    docstring) and **trimmed to silver's canonical recall identity** — ~5 of USDA's ids carry
    leading/trailing whitespace (Finding R) and `stg_usda_fsis_recalls` trims with `trim()`; the
    manifest is a silver-feeding join key, so it must match that trimmed form or
    `recall_lifecycle`'s join to silver silently misses those ids. ``langcode`` is read from
    ``langcode_field`` when given (USDA), else NULL. Rows are deduped on the trimmed
    ``(recall_key, langcode)`` so whitespace variants and repeated recalls collapse to a single
    presence row. The output column stays named ``source_recall_id`` (the manifest table column),
    whatever record attribute it was sourced from.

    Pure function — no DB access, fully unit-testable in isolation.
    """
    seen: set[tuple[str, str | None]] = set()
    rows: list[dict[str, Any]] = []
    for record in records:
        raw_id = getattr(record, recall_id_field, None)
        if raw_id is None:
            # Defensive: the loader already rejects null-identity records unless a source
            # opts into allow_null_identity, and no presence-tracked source does. Skip
            # rather than emit a NOT NULL violation.
            continue
        source_recall_id = str(raw_id).strip()
        if not source_recall_id:
            continue  # whitespace-only id → nothing to anchor presence on.
        langcode = getattr(record, langcode_field, None) if langcode_field else None
        key = (source_recall_id, langcode)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "run_id": run_id,
                "source": source,
                "source_recall_id": source_recall_id,
                "langcode": langcode,
            }
        )
    return rows
