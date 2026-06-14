# 0041 — NHTSA bronze dedup-lookup: set-based staging-table JOIN (fetch-only restructure)

- **Status:** Accepted
- **Date:** 2026-06-12
- **Clarifies:** ADR 0030 — the chunked existing-hash lookup in 0030's "Implementation-side: text-canonical IN + chunked existing-hash lookup" subsection is now the **small-batch path only**; large batches route to the staging-table JOIN this ADR introduces. 0030's 11-tuple identity, `hash_exclude_fields`, within-batch dedup, `allow_null_identity`, and the `_identity_text_expr` text-canonical comparison are **unchanged**.
- **Related:** ADR 0007 (content-hash dedup — the decision this lookup serves), ADR 0020 (single-transaction pipeline state).

> Built 2026-06-11 on `feature/pre-go-live-validation` as a fetch-only restructure; committed `460bec5` (v0.24.1); verified on the production Neon branch 2026-06-12. Filed here so the chosen restructure — and the typed / anti-join / generated-column alternatives it rejects — are on record with their reasoning.

## Context

### What prompted this ADR

The daily `recalls extract nhtsa` ran **~17 min and worsened as bronze grew** — flagged "Performance — TOP PRIORITY" in `TODO.md`. Profiling (`documentation/audit/deep_rescan_reliability_audit.md` Problem 1) localized the cost to the existing-hash dedup **lookup**, not the insert: a no-op deep-rescan logged `load_bronze ≈ 21.5 min` against ~13 s for download + parse + validate combined.

NHTSA's daily flat-file dump *is* the whole corpus (~241k identities). Per ADR 0030's chunked-lookup note, `_fetch_existing_hashes` split that incoming identity set into `60_000 // 11 ≈ 5,454` keys per chunk → **~45 chunked `IN`-queries**, each a full sequential scan of `nhtsa_recalls_bronze` that recomputes the non-sargable 11-column `_identity_text_expr` per row. No index serves that functional predicate (NHTSA's only indexes are on `source_recall_id` — *excluded* from identity — and `campno` — 1 of 11 columns). Cost ≈ `bronze_rows × chunks` → **O(corpus × chunks)**, growing with bronze on both axes.

The 2026-06-10 inner-SHA short-circuit only skips **no-change** days. NHTSA regenerates real content on most active days, so the ~17-min change-day lookup is the **common** case — this is what falsified the original "defer the staging lookup" rationale in the deep-rescan plan.

### What was already decided

- **ADR 0007** — dedup is `(identity_fields, content_hash)` against the most-recent existing bronze row; the insert/skip *decision* lives in `BronzeLoader.filter_new_records` (`existing.get(item[0]) != item[1]` — insert if the identity is new OR its content changed vs the latest).
- **ADR 0030** — NHTSA's 11-tuple identity, `RECORD_ID` excluded from the hash, within-batch dedup, `allow_null_identity`, the text-canonical `IN` comparison (`_identity_text_expr`), and the chunked existing-hash lookup. The chunking solved the wire-protocol bind-parameter ceiling at 65k+ batches; it did **not** target the per-chunk full-scan cost.
- **The single-oracle guardrail** — the identity canonicalization and the insert/skip decision must have exactly one home. ADR 0030's 2026-06-01 amendment (per-source `DedupContract` SSOT) exists precisely so the incremental, deep-rescan, and recovery paths physically cannot disagree on the dedup oracle.

### The architectural question

How to cut the O(corpus × chunks) lookup to O(corpus) **without moving any part of the dedup decision — or the identity canonicalization — into a second place**, i.e. without trading the ~45× win for the dual-oracle divergence risk ADR 0030 guards against.

## Decision

**Restructure only the existing-hash *fetch* into one set-based staging-table JOIN; leave the dedup *decision* byte-identical in Python.**

`_fetch_existing_hashes` routes by batch size against `chunk_size = _PG_PARAM_SAFETY_LIMIT // n_cols`:

- **`len(identity_keys) <= chunk_size`** (e.g. CPSC's ~10-row daily delta) → the existing single `IN`-query (`_fetch_existing_hashes_chunk`), unchanged — one seq-scan, no temp-table overhead.
- **`len(identity_keys) > chunk_size`** (NHTSA's full dump) → a new `_fetch_existing_hashes_staged`:
  1. `CREATE TEMP TABLE _dedup_ids_<uuid> (c0 text, … cN-1 text) ON COMMIT DROP` in `pg_temp`.
  2. Bulk-load the (already text-canonical) incoming identity tuples as **chunked multi-row `INSERT`s** — one `VALUES` statement per chunk under the bind-param ceiling; no `psycopg2` COPY (keeps the SQLAlchemy-only dependency posture).
  3. `ANALYZE` the temp table.
  4. Run the **same** two-stage query as the chunk path, with the chunk's `tuple_(*exprs).in_(keys)` replaced by a `JOIN` to the temp table on `_identity_text_expr(bronze.col_i) = temp.c_i`: **stage 1** `GROUP BY` text-canonical identity, `max(extraction_timestamp)`; **stage 2** join back to bronze on identity + `max_ts` to recover `content_hash`. Returns the identical `{identity_tuple: latest_content_hash}` dict.

This is a pure **`IN → JOIN`** restructure: the same `_identity_text_expr` text-canonical comparison on both sides, the same two-stage latest-per-identity recovery, the same return dict. `filter_new_records` and `load()` are **untouched** — the change is **correct by construction**, not merely test-correct.

Two load-bearing details:

- **`ANALYZE` is required.** A fresh temp table carries zero statistics → the planner assumes ~1,000 rows and risks a **nested loop** (per-row bronze seq-scans = catastrophic at corpus scale). `ANALYZE` supplies the real rowcount (~322k) so the planner uses correct *input* cardinalities and picks a sort-merge / hash join. (Observed on prod: stage-1 is a merge join feeding a top hash join — never a nested loop.)
- **Two-stage shape preserved for tie-equivalence.** Keeping the `GROUP BY`-then-rejoin form (rather than collapsing to a window function) makes tie behaviour — two rows sharing `max(extraction_timestamp)` — **provably identical** to ADR 0030's chunk path, including its pre-existing dict-overwrite nondeterminism. A deterministic tiebreaker would shift the dedup decision boundary and is deliberately out of scope.

**Privilege.** The TEMP table lives in `pg_temp` and needs only the database `TEMPORARY` privilege, held by `recalls_app` via the `PUBLIC` default (verified by `verify_recalls_app_grants.sql` §0.6). It does **not** need `CREATE` on schema `public` — which `recalls_app` deliberately lacks. It runs on the caller's existing `Connection` inside the one `engine.begin()` (ADR 0020); `ON COMMIT DROP` reaps it at commit and nothing survives a rollback. **No role or privilege expansion.**

**Activation threshold.** `BronzeLoader` is source-agnostic; the size gate keeps CPSC / FDA / USDA / USCG daily deltas on the fast single `IN`-query (a temp table + `ANALYZE` would be net overhead for ~10 rows). Only large batches — NHTSA's daily dump and deep-rescan, both via `load_bronze` — take the staged path, so it benefits the daily incremental and the weekly deep-rescan equally and composes with the inner-SHA short-circuit (short-circuit skips no-change days; this makes change days fast).

## Rejected alternatives

On record because each is a plausible "more elegant" path that will be re-litigated.

### Typed temp table ("single SQL oracle")

Mirror bronze's identity column *types* in the temp table and apply `_identity_text_expr` to both sides, collapsing canonicalization to one SQL expression. **Rejected:** it does not achieve single-oracle for a *fetch-only* change. The Python↔SQL canonicalization coincidence that matters lives in `filter_new_records` (`existing.get(item[0])` — a Python-text dict key vs the SQL-text dict keys this fetch returns), which we deliberately keep. Typing the temp table adds a new comparison path and more complexity for **no net oracle gain**.

### Full anti-join INSERT (decision-in-SQL)

Replace the fetch + Python decision with one server-side `INSERT … SELECT … WHERE NOT EXISTS`. **Rejected:** it relocates the dedup *decision* into SQL that must re-prove A→B→A revert handling, latest-per-identity, tie behaviour, and null-identity semantics — a far larger blast radius next to the project's #1 guardrail, **test-correct rather than structure-correct**, and `content_hash` is computed in Python anyway. This is exactly the dual-oracle relocation ADR 0030's `DedupContract` SSOT was created to prevent.

### Stored generated `identity_hash` column + btree index (true O(delta))

A generated column materializing the text-canonical identity, indexed, would make the lookup O(delta) instead of O(corpus). **Rejected for now:** it institutionalizes a **second** canonicalization oracle in DDL (the generated expression must mirror `_identity_text_expr` forever or dedup silently diverges) and needs a 322k+-row table rewrite + backfill. NHTSA's daily batch *is* the whole corpus (~241k of ~241k), so O(delta) ≈ O(corpus) for it anyway. **Deferred-escalation criteria** — write a follow-up ADR only when all hold for some source: corpus ≫ 1M rows **and** daily delta ≪ corpus **and** the one-pass staged join measurably misses the run budget.

## Consequences

### Positive

- **~17 min → ~1 min** on the real daily run; the existing-hash *fetch* alone goes from ~17 min (~45 full scans) to ~6 s (two scans). Benefits the daily incremental and the weekly deep-rescan (both call `load_bronze`).
- **Smallest possible blast radius near the dedup oracle.** The insert/skip decision and identity canonicalization are byte-identical to ADR 0030 — no new dedup semantics to test, correct by construction. The differential proof (below) confirms it empirically as belt-and-suspenders, not as the safety basis.
- **No role/privilege expansion**, and it runs inside the existing single transaction.

### Negative

- **A second lookup code path** (`_fetch_existing_hashes_chunk` for small batches, `_fetch_existing_hashes_staged` for large), size-gated — slightly more loader surface; reviewers need this ADR for the "why two paths."
- **Still O(corpus) one-pass, not O(delta).** The functional `_identity_text_expr` predicate remains non-sargable, so the staged join does two full bronze seq-scans. Right trade for NHTSA's bounded corpus; revisit per the deferred-escalation criteria when a source outgrows it.
- **The planner badly under-estimates the join cardinality** (`rows=1` vs ~322k actual) because column statistics cannot propagate through the 11 `COALESCE`/`to_char` expressions. The `ANALYZE` on the temp table (correct *input* sizes) is what keeps the plan off a nested loop. A latent sensitivity to document, not a present problem — empirically a 6 s plan.

### Neutral

- ADR 0030's identity (11-tuple), `hash_exclude_fields`, within-batch dedup, `allow_null_identity`, and text-canonical comparison are unchanged; this ADR refines only 0030's chunked-lookup implementation note (now the small-batch path).
- The per-source `DedupContract` SSOT (ADR 0030 amendment) is untouched — the staged path sits **below** the oracle, on the fetch side.

## Empirical evidence (verified on the production Neon branch, 2026-06-12)

| Check | Result |
|---|---|
| Real daily `recalls extract nhtsa` — new code, full walk (`was_short_circuited=f`) | **17 min → 61 s**; fetched 241,798 / inserted 287 / **0.1%** insert rate / `status=success`. Bronze-side cross-check: the latest `extraction_timestamp` appended exactly 287 rows. (`scripts/sql/nhtsa/_pipeline/verify_dedup_refactor_run.sql`) |
| Prior daily runs — old chunked code | 2026-06-10 = 17:27, 2026-06-11 = 16:50 — the baseline this replaces |
| `EXPLAIN (ANALYZE, BUFFERS)` of the staged join over ~322k | fetch **6.2 s**; stage-1 **merge join** (external-merge sorts) feeding a top **hash join**; one bronze seq-scan per stage; **no nested loop** — the `ANALYZE` did its job. (`scripts/sql/nhtsa/bronze/explain_staged_dedup_join.sql`) |
| Full-corpus differential — staged dict **==** old chunked-loop dict over every identity | **PASSED** (21 min, dominated by the old chunked reference it diffs against). 50k-identity sample also passed pre-merge on a prod-clone branch. (`NHTSA_DEDUP_DIFF_FULL=1 pytest -k equivalence`) |
| `recalls_app` database `TEMPORARY` privilege | **PASS** (`verify_recalls_app_grants.sql` §0.6 `can_create_temp=t`) |

## Implementation

**`src/bronze/loader.py`** (generic; only the large-batch route is new):

1. `_fetch_existing_hashes` — routes by `len(identity_keys)` vs `chunk_size = _PG_PARAM_SAFETY_LIMIT // n_cols`.
2. `_fetch_existing_hashes_chunk` — the pre-change single `IN`-query body, unchanged; now the small-batch path.
3. `_fetch_existing_hashes_staged` (new) — the `pg_temp` TEMP table + chunked multi-row `INSERT`s + `ANALYZE` + the two-stage `GROUP BY`-`max(extraction_timestamp)` JOIN.
4. `filter_new_records`, `load()`, `_identity_text_expr`, and `DedupContract` — unchanged.

**Tests** (`tests/bronze/`):

- `test_loader.py` — routing: a large batch dispatches to the staged path; a batch *at* the ceiling stays on the single query.
- `test_loader_fetch_equivalence.py` (new, DB-backed via `tests/conftest.py:test_db_url` on a prod-clone Neon branch) — differential: the staged dict equals a frozen copy of the old chunked loop over the real corpus (50k sample default; full corpus via `NHTSA_DEDUP_DIFF_FULL=1`), including synthetic never-seen identities and every real `bgman`/`endman` timestamptz value/format.

**Verification scripts** (`scripts/sql/`):

- `nhtsa/_pipeline/verify_dedup_refactor_run.sql` — run telemetry from `extraction_runs` (short-circuit flag, duration, insert-rate divergence tripwire, bronze cross-check).
- `nhtsa/bronze/explain_staged_dedup_join.sql` — representative `EXPLAIN (ANALYZE, BUFFERS)` of the staged plan.
- `_pipeline/verify_recalls_app_grants.sql` §0.6 — the `TEMPORARY`-privilege check.

## Related

- **ADR 0007** — content-hash dedup; the insert/skip decision this lookup serves stays in `filter_new_records`.
- **ADR 0030** — NHTSA 11-tuple identity + within-batch dedup + the chunked lookup this ADR refines (large batches → staging JOIN; everything else unchanged).
- **ADR 0020** — single-transaction pipeline state; the staged path runs in the caller's existing `engine.begin()`.
- **Findings** — `documentation/audit/deep_rescan_reliability_audit.md` Problem 1 (the cost this resolves) + Section B (the text-canonical constraint any server-side join must honor).
- **Plan** — `project_scope/deep-rescan-reliability-plan.md` W10/R9 (build tracking).
- **Operations** — `documentation/operations.md` (the `recalls_app` role posture incl. the `TEMPORARY` dependency); migration `0033_recalls_app_role_posture` (the role recreate).
