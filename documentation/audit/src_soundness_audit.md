# `src/` soundness audit — duplication, dead code, and the NHTSA deep-rescan dedup bug

- **Status:** Active (findings; 2026-06-01)
- **Type:** cross-cutting audit (per `documentation/documentation_model.md` type 6 — describes the
  world; the remediation/refactor work it motivated is owned by
  `project_scope/archive/src-consolidation-plan.md`, which this doc points at rather than prescribing here).
- **Method:** three read-only `codebase-analyzer` passes over `src/` (extractors / bronze+landing /
  schemas+config+cli), then direct verification of every load-bearing claim.

This audit was prompted by three smells surfaced in conversation. Two were red herrings (one
inverted), but the investigation surfaced a **reachable correctness bug** and ~440 lines of
duplication. What we found is recorded below; what we did about it is the plan doc.

---

## The three surfaced smells — verdicts

| Smell (as reported) | Verdict |
|---|---|
| "NHTSA can't be represented by one accessor (incremental 11-tuple vs deep-rescan single-col differ fundamentally)" | **Confirmed and worse — a reachable correctness bug, not a design smell** (see below). |
| "Only 5 of 8 sources call `check_date_sanity`" | **Justified, not drift.** The 3 that skip it (`usda_establishments`, `uscg_manufacturers`, `uscg_manufacturer_details`) expose no recall-publication timestamp — only nullable/administrative dates — so the check has nothing meaningful to assert. Each omission was already code-commented. Preserved as a reviewed choice, now encoded explicitly in the invariant registry. |
| "3 USCG schemas lack `populate_by_name=True`" | **Inverted — the code was right, the docstrings lied.** All three schemas *set* `populate_by_name=True` (added for quarantine-recovery `model_validate` of dumped payloads). Their module docstrings still claimed "No populate_by_name." Stale docstrings only; corrected. |

---

## The NHTSA deep-rescan dedup bug (reachable; data already polluted on `main`)

`NhtsaDeepRescanLoader.load_bronze` constructed `BronzeLoader(identity_fields=("source_recall_id",))`
— it dedup'd on, and hashed, the regen-unstable `RECORD_ID`. The incremental path
(`NhtsaExtractor.load_bronze`) used the 11-tuple identity (ADR 0030) **and** excluded
`source_recall_id` from the content hash. Both write the *same* `nhtsa_recalls_bronze` table, so the
two paths disagreed on both the identity bucket and the hash for the same logical row.

- **Reachability:** the deep-rescan loader runs in the `deep-rescan-nhtsa.yml` weekly cron and the
  `recalls deep-rescan nhtsa --change-type=historical_seed` path. Not dead code. `recovery.py` had
  already flagged the single-column loader as "a latent bug."
- **Manifestation:** on a populated table, every NHTSA file regen (which reassigns `RECORD_ID`)
  re-inserts the whole overlapping corpus as phantom-new rows. On an **empty** table (the actual
  `main` seed — see below) there are no phantom-vs-incremental rows, but every seeded row's
  `content_hash` includes `RECORD_ID`, so the *next* correct incremental run would cascade-duplicate.
- **Incident (recorded, not prescribed):** the buggy `historical_seed` was already executed as the
  first NHTSA load on the new `main` Neon branch (empty beforehand), so 100% of current
  `nhtsa_recalls_bronze` rows are buggy-seed output with `RECORD_ID`-polluted hashes. Remediation
  (assess → truncate → re-seed → verify) is owned by the plan doc, not this findings doc.

---

## Duplication inventory (the "bloat", now removed)

| Item | Scale | Resolution (see plan) |
|---|---|---|
| `_record_run` copy-pasted across extractors | 8 copies, ~330 lines | one base-class template + `_augment_run_row`/`_augment_response_row` hooks |
| `source_watermarks` / `extraction_runs` `Table` objects redeclared per module | 8× each, with drifting column subsets | one shared `src/extractors/_tables.py` (full-column union) |
| `UsdaEstablishmentExtractor` ETag/304 helpers copied from `UsdaExtractor` | 6 methods, ~110 lines, with an *already-violated* keep-in-sync note (establishment had inlined the HTTP-date parse the recall copy delegated) | shared `FsisConditionalGetExtractor` base |
| `BronzeLoader` dedup config restated per source | 3 sites each (incremental / deep-rescan / recovery) — the **root cause** of the NHTSA bug | one `DedupContract` per source (`src/bronze/dedup_contracts.py`), consumed via `BronzeLoader.from_contract` |

Net effect of the refactor: **−662 lines** (`+520 / −1182`) across the first commit; further reduction
from the USDA collapse.

### Dead code removed
- `UsdaFsisExtractionResult` — never instantiated.
- `inner_content_stream()` (`_flat_file.py`) — no production caller.
- `src/bronze/retry.py` (`transient_retry` / `r2_retry` decorators) — tests-only; production uses the
  `_TRANSIENT_RETRY` / `_R2_RETRY` `Retrying` instances in `_base.py`, which are covered in
  `test_base.py`.
- Orphaned `.pyc` for the retired FDA recovery one-off.

---

## Justified divergences — explicitly NOT consolidated

These looked like inconsistencies but are correct; the refactor preserved them deliberately:

- **`check_date_sanity` 5/8 split** — the 3 skippers have no publication timestamp (above).
- **Per-source date validators / per-source retry scopes** — `_PER_PAGE_RETRY` (FDA) deliberately
  retries only `TransientExtractionError` so `RateLimitError` propagates; `_PER_FETCH_RETRY` (HTML
  scrapers) is per-fetch. Distinct policies, not duplication.
- **FDA deep-rescan `within_batch_dedup=True`** — a genuine per-mode need (productid tie-boundary
  straddle collapse); kept as a per-mode override on top of the shared oracle, not flattened into it.
- **USCG/manufacturer deep-rescan "symmetry" loader classes** — intentional, for CLI uniformity.
- **YAML typed-but-unconsumed config fields** — deliberate forward-declared intent (ADR 0012 waves).

---

## Related
- `project_scope/archive/src-consolidation-plan.md` — the work this audit motivated (what we did + the NHTSA
  data remediation).
- ADR 0030 (amended 2026-06-01) — the dedup-contract SSOT that makes the bug class structurally
  impossible.
- `documentation/audit/methodology.md` — how cross-source audits are run in this project.
