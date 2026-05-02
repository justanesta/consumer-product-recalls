# 0028 — Backfill and historical re-extraction semantics

- **Status:** Accepted
- **Date:** 2026-05-01
- **Supersedes:** —
- **Superseded by:** —
- **Clarifies:** ADR 0014 (which covers source-driven schema drift recovery but not historical-load semantics); ADR 0023 (FDA archive-migration deep rescan); ADR 0026 (manifest backfill).

## Context

Three prior ADRs touch on backfill in passing:

- **ADR 0014** prescribes the four upstream-driven schema-drift recovery procedures (source adds field, renames field, changes type, adds enum value) and routes them through a planned `scripts/re_ingest.py` CLI in Phase 6. The CLI re-processes raw R2 payloads through the bronze loader.
- **ADR 0023** filed a `deep-rescan-fda.yml` workflow to handle FDA's archive migration touching old records.
- **ADR 0026** asks whether to backfill the snapshot-presence manifest for runs that happened pre-ADR (Q3 in the original draft).

None of these formalize the question: **how do we backfill a date range — for any reason — and what semantics does that produce?**

Backfill is needed in three concrete situations the project has already identified, and likely more as it evolves:

1. **Historical seeding from before the project existed.** CPSC's incremental strategy will not reach 2005–2024 records until the upstream archive migration completes (estimated years away at ~2–3 records/day). A one-time multi-year deep rescan is required before Phase 7 cron go-live to populate that 20-year gap. Documented in `documentation/cpsc/last_publish_date_semantics.md` Section 3.
2. **Recovery from extractor downtime or correctness bugs.** If a daily cron job fails for a week, or if a bug routed valid records to the rejected table, the gap needs to be filled. Re-running the daily extractor with a widened watermark window may suffice, but for sources with weak or absent watermarks (CPSC, USDA — see ADR 0010 revision note), the daily query won't catch records changed during the gap.
3. **Schema-drift recovery.** ADR 0014's `scripts/re_ingest.py` CLI replays raw R2 payloads through a fixed Pydantic schema. This is itself a backfill operation, but its mechanism (replay from R2) is different from situations 1 and 2 (re-fetch from source).

The mechanisms differ, the idempotency stories differ, and the silver-layer implications differ. Without a unifying ADR, each case is solved ad-hoc and inconsistently.

## Decision

Three named backfill mechanisms, each with documented use cases, idempotency guarantees, and silver-layer implications. They are not mutually exclusive — a single backfill operation may use more than one — but each step has clear semantics.

### Mechanism A — Deep rescan (re-fetch from source)

**Definition.** Re-run the source extractor against the live API/file/scrape, ignoring the incremental watermark, with an explicit date range or "everything" window. Persist new bronze rows where content has changed; let content-hash dedup (ADR 0007) make unchanged records into no-ops.

**Implementation.** A separate workflow file `.github/workflows/deep-rescan-<source>.yml` per source, with `workflow_dispatch` for manual triggering and (for CPSC and FDA) a weekly cron schedule per ADR 0010 / ADR 0023. The workflow calls a separate extractor method or class (`<Source>Extractor.deep_rescan(...)`), not `extract()` — the incremental path's count-guard would fire immediately on a full-window query (per ADR 0010's "incremental vs. historical load paths" section).

**Use cases.** Initial historical seeding (CPSC 2005–2024 gap), recovery from extractor downtime, periodic edit-detection on weak-watermark sources (CPSC, FDA archive migration).

**Idempotency.** Bronze inserts are content-hash-conditional. Re-running a deep rescan over the same window produces no new bronze rows if nothing has changed. **Safe to retry.** Safe to overlap with the daily incremental cron.

**Silver implications.** Bronze rows produced by a deep rescan are indistinguishable from rows produced by the daily incremental — same `extraction_runs` table entry, same `content_hash` semantics, same downstream `recall_event_history` derivation. The `extraction_runs.change_type` column (per ADR 0027 / `documentation/operations/re_baseline_playbook.md`) defaults to `'routine'` for both paths; this is correct because a deep rescan is genuinely re-fetching the source-of-truth and any new rows are real edits the incremental missed.

**The exception:** the **one-time CPSC historical seeding deep rescan** (situation 1 above) should be marked `--change-type=historical_seed` to distinguish it from routine deep rescans in `recall_event_history`. The history model treats `historical_seed` rows as `first_seen_at`-style appearances rather than synthesizing edit events out of them. Add `historical_seed` to the allowed values for `extraction_runs.change_type` (originally introduced as `routine`, `schema_rebaseline`, `hash_helper_rebaseline` in ADR 0027). One-time use; the CPSC backfill is the only foreseen instance.

### Mechanism B — R2 replay (re-process raw payloads)

**Definition.** Re-run the bronze loader against raw payloads already landed in R2, without contacting the source. Used when the source's response was correct but our processing of it was wrong (schema bug, normalizer change, hashing-helper update).

**Implementation.** `scripts/re_ingest.py` CLI as planned in ADR 0014. Takes `--source`, optional date range (matched against R2 object keys: `<source>/<extraction_date>/`), and optional `--change-type` flag (`schema_rebaseline` or `hash_helper_rebaseline` per ADR 0027). Iterates R2 objects, deserializes the raw payload, re-runs the schema validation + invariant checks + bronze loader.

**Use cases.** Schema drift recovery (ADR 0014 cases 1–4), Pydantic normalizer changes that flip the canonical hash (ADR 0027's "re-baseline" wave), bug fixes that should retroactively process previously-rejected records (a fix to a too-strict invariant lets old `_rejected` records re-enter bronze).

**Idempotency.** Same content-hash conditional insert as Mechanism A. Re-running R2 replay on the same window with the same code is a no-op. **Safe to retry.** Safe to interleave with daily incremental.

**Silver implications.** R2 replay produces a new wave of bronze rows where the canonical dict changed — even though the source did not change. The `extraction_runs.change_type` column **must** be set to `'schema_rebaseline'` or `'hash_helper_rebaseline'` (not `'routine'`). The `recall_event_history` model in Phase 6 filters out non-routine runs from edit detection so the wave doesn't synthesize false edit events. This is the whole reason `change_type` exists.

### Mechanism C — Manifest backfill (ADR 0026 Q3)

**Definition.** Synthesize `extraction_run_identities` rows for runs that completed before ADR 0026 was implemented, by reading historical R2 payloads and computing the identity tuples that were present in each one.

**Implementation.** A one-shot job (`scripts/backfill_manifest.py`) that, for each historical extraction run, fetches the corresponding R2 payload, parses it for identity tuples, and inserts them into `extraction_run_identities` keyed on the original `run_id`.

**Use cases.** Bridging the lifecycle-tracking gap when ADR 0026 lands. Without the backfill, `first_seen_at` and `was_ever_retracted` for any record present before ADR 0026 implementation will be derivable only from the implementation date forward.

**Idempotency.** Inserts are conditional on `(run_id, source, source_recall_id, langcode)` primary key. Re-running the backfill against the same `run_id` is a no-op. **Safe to retry.**

**Silver implications.** Manifest-backfilled rows are indistinguishable from manifest rows written by current extractors. Silver lifecycle dimensions (`first_seen_at`, `last_seen_at`, etc.) become accurate retroactively to the start of R2 payload retention. R2 retention policy (currently: keep forever) bounds how far back this works.

**Decision on whether to run it:** Yes, run it once when ADR 0026 implementation lands. The historical lifecycle data is portfolio-visible and queryable, and the cost is small (one read per R2 object, one insert per identity tuple). USDA-only initially (matching ADR 0026 scope). The job is rerunnable when ADR 0026 extends to additional sources.

## Consequences

### Positive

- **Three named mechanisms.** Operators reading the runbook know which one to reach for. Schema-drift bug → Mechanism B. CPSC historical gap → Mechanism A. Manifest gap → Mechanism C.
- **Idempotency is universal.** All three mechanisms are safe to retry, safe to interleave with daily extraction, and produce no duplicates. This was already true for A and B via content-hash dedup; C inherits idempotency from its primary key constraint.
- **`change_type` is the unifying audit field.** Routine extraction, deep rescan, R2 replay, historical seed, schema rebaseline — every distinct backfill flavor has a value. The `recall_event_history` model uses it to keep the edit story honest.
- **CPSC historical gap is unblocked.** The 20-year (2005–2024) gap can be loaded via Mechanism A with `--change-type=historical_seed` before Phase 7 cron go-live, without polluting `recall_event_history` with synthesized 25-year-old edits.
- **R2 retention policy is now load-bearing.** R2 is the substrate for both Mechanism B and Mechanism C. The current "keep forever" stance (no formal policy) is fine for v1; if storage cost becomes meaningful, retention pruning becomes a deletion of backfill capability and warrants its own ADR.

### Negative

- **`scripts/re_ingest.py` (Mechanism B) and `scripts/backfill_manifest.py` (Mechanism C) are unimplemented.** Both are filed as Phase 6 deliverables in the implementation plan.
- **Operator discipline required.** Forgetting `--change-type=schema_rebaseline` on a Mechanism B run will silently corrupt the edit history. The CI guard (per ADR 0027) catches this for code changes; operator-driven runs need a different gate. Recommendation: the `recalls extract` CLI requires `--change-type` to be set explicitly when invoked outside the cron context (no default fallback to `'routine'` on `workflow_dispatch`).
- **`extraction_runs.change_type` allowed-values list grows.** From the original three (`routine`, `schema_rebaseline`, `hash_helper_rebaseline`) to four with the addition of `historical_seed`. Future backfill flavors (e.g., `deletion_recovery` if a source's records are accidentally deleted from bronze) would extend this further. The list lives in a single Alembic migration's CHECK constraint; growth is cheap.

### Neutral

- **Mechanism A and B are not exclusive.** A historical-seed deep rescan that surfaces a schema bug requires running Mechanism B against the just-landed R2 payloads to fix the bug retroactively. This is fine and produces sensible results — both mechanisms write through the bronze loader's content-hash conditional insert.

## Alternatives considered

### Alternative 1 — Single unified "backfill" CLI

Combine all three mechanisms behind one `scripts/backfill.py` CLI with mode flags.

- **Pros:** one entry point; simpler operator mental model.
- **Cons:** the three mechanisms have different inputs (live API vs R2 vs computed manifest), different operator concerns (cost, throttling for A; hashing implications for B; one-shot for C), and different testing surfaces. Forcing them into one CLI conflates concerns.
- **Verdict:** rejected. Three named scripts with clear semantics beat one polymorphic one. The naming (`deep-rescan-<source>.yml` for A; `re_ingest.py` for B; `backfill_manifest.py` for C) makes the operator's choice obvious.

### Alternative 2 — Avoid backfill entirely; declare history-before-implementation as out-of-scope

Treat ADR 0026 as starting from its implementation date forward; don't backfill the manifest.

- **Pros:** no extra script; no R2 read traffic.
- **Cons:** loses portfolio-visible historical lifecycle data; permanently asymmetric silver dimensions ("we know `first_seen_at` for records published after X but not before").
- **Verdict:** rejected for ADR 0026's case (manifest backfill is cheap and earns its keep). Accepted in spirit for any future ADR whose backfill cost dominates the value.

### Alternative 3 — Treat the CPSC historical gap as out-of-scope for v1

Ship without 2005–2024 CPSC data. Document it as a gap users should know about.

- **Pros:** no operator action required; ship sooner.
- **Cons:** the project's primary user-visible promise is "all consumer-product recalls." A 20-year gap on the largest source is a substantial defect even for v1.
- **Verdict:** rejected. The deep-rescan workflow exists; pointing it at the historical window is an operator choice, not a code change.

## Implementation outline

| Task | Where filed | Owner |
|---|---|---|
| `scripts/re_ingest.py` (Mechanism B) | Phase 6 deliverable per `project_scope/implementation_plan.md` | Phase 6 |
| `scripts/backfill_manifest.py` (Mechanism C) | Phase 6 deliverable, alongside ADR 0026 implementation | Phase 6 |
| `extraction_runs.change_type` allowed-values list extension to include `historical_seed` | Add to the existing migration introduced in ADR 0027 / Phase 5b.2 Step 4.5 (if not yet shipped) or follow-up Alembic migration | Phase 5b.2 or Phase 6 |
| CPSC historical-seed deep rescan operator runbook | `documentation/operations.md` troubleshooting section | Pre-Phase 7 (operator-driven, one-shot) |
| `recalls extract <source>` CLI requires explicit `--change-type` outside `cron` triggers | `src/cli/main.py` | Phase 7 (with cron turn-on) |
