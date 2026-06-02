# `src/` soundness consolidation — plan

- **Status:** Complete — PR #48 (2026-06-01); archived. The NHTSA data remediation (Task 10) is the
  one open operator step, tracked below.
- **Owns:** the execution of the refactor + the NHTSA data remediation.
- **Points at:** `documentation/audit/src_soundness_audit.md` for *what we found* (this doc does not
  restate the findings — single-home rule). ADR 0030 (amended 2026-06-01) for the dedup-contract
  decision rationale.

## Context

The `src/` soundness audit (see the audit doc) confirmed a reachable NHTSA deep-rescan dedup bug and
~440 lines of duplication accumulated as later sources were added. This plan executed the
"disciplined Maximal" scope: registry-drive the *declarative* config (dedup contracts, invariant
sets) and base-class-consolidate the *behavioral* duplication — making the NHTSA bug class
structurally impossible rather than just patched. Guardrails: no invariant DSL; dedup contracts stay
in typed Python (never YAML); single-source only the dedup *oracle* (mode flags stay per-mode);
leaf-module placement to avoid import cycles.

## Workstreams (done-markers)

| # | Workstream | Status |
|---|---|---|
| 1 | Foundations: `src/bronze/dedup_contracts.py` (oracle SSOT), `BronzeLoader.from_contract`, invariant registry (`PER_RECORD_INVARIANTS_BY_SOURCE_NAME`, `run_per_record_invariants`), shared `src/extractors/_tables.py` | ✅ |
| 2 | Base-class `_record_run` template + `_augment_run_row`/`_augment_response_row` hooks; lift `_captured_response_*` to `Extractor` | ✅ |
| 3 | Migrate all 8 extractors to contract + invariant registry; **NHTSA deep-rescan bug fixed structurally** (both paths resolve one contract) | ✅ |
| 4 | Collapse the two USDA extractors onto `FsisConditionalGetExtractor` (shared ETag/304 + `_parse_http_date`); fixes the already-violated keep-in-sync drift | ✅ |
| 5 | `recovery.py` `RECOVERY_CONFIG_BY_SOURCE_NAME` consumes the contract (3rd config copy eliminated) | ✅ |
| 6 | Dead code removed: `UsdaFsisExtractionResult`, `inner_content_stream`, `src/bronze/retry.py`(+test), orphaned `.pyc` | ✅ |
| 7 | Micro-fixes: 3 USCG schema docstrings, `settings.py` comments, CPSC `_SUB` reuse, `historical_seed_urls` cap, hoisted `import hashlib`, cli `.get()` hardening | ✅ |
| 8 | Tests: `test_dedup_contracts.py` (incl. NHTSA `RECORD_ID`-churn regression guard) + invariant-registry tests; all broken loader-mock tests updated. Gates: ruff/ruff-format/pyright clean, pytest green (~96% cov) | ✅ |
| 9 | Documentation graduation (this doc + audit doc + TODO #56 pointer + master-plan index + ADR 0030 amendment) | ✅ |
| 10 | NHTSA data remediation (assess → truncate → re-seed → verify) | ⏳ pending operator run |

## Key files

- `src/bronze/dedup_contracts.py` *(new)* — `DedupContract` + `DEDUP_CONTRACT_BY_SOURCE_NAME`
- `src/bronze/loader.py` — `BronzeLoader.from_contract`
- `src/bronze/invariants.py` — invariant registry + `run_per_record_invariants`
- `src/extractors/_tables.py` *(new)* — shared `source_watermarks` + `extraction_runs`
- `src/extractors/_fsis_base.py` *(new)* — `FsisConditionalGetExtractor`
- `src/extractors/nhtsa.py` — the dedup bug fix
- `tests/bronze/test_dedup_contracts.py` *(new)* — the regression guard

## NHTSA data remediation (Task 10 — operator runs the SQL/CLI)

The buggy `historical_seed` already ran into the empty `main` `nhtsa_recalls_bronze`, so 100% of its
rows carry `RECORD_ID`-polluted content hashes. Sequence (SQL under
`scripts/sql/nhtsa/bronze/`, run by the operator against `main`; **after** the code fix is on `main`):

0. **Assess** (`assess_deep_rescan_seed_damage.sql`, read-only): confirm the seed run in
   `extraction_runs`; if `status='failed'` the txn rolled back → no remediation needed. Confirm all
   `nhtsa_recalls_bronze` rows share one `raw_landing_path` (corroborates empty-before).
1. **Code fix on `main`** — the dedup contract (already done here; lands at PR merge).
2. **Reset** (`reset_nhtsa_bronze.sql`): `TRUNCATE nhtsa_recalls_bronze` + clear the `nhtsa`
   `source_watermarks` row (100% of rows are buggy-seed output → truncate is the clean reset).
3. **Re-seed**: `recalls deep-rescan nhtsa --change-type=historical_seed` (now uses the 11-tuple
   oracle; byte-dup groups collapse, hashes exclude `RECORD_ID`).
4. **Verify**: re-run the census; run the deep-rescan a **second** time and assert **0 inserts**
   (the live form of the unit-level regression guard); spot-check `content_hash` is
   `RECORD_ID`-independent.

There is no "un-insert" tool (`recover-rejected` only reclaims *rejected* rows), so this is one-off
SQL. A "roll back a run by `raw_landing_path`" utility is possible future-work — not built now.
