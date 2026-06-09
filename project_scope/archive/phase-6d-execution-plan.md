# Phase 6d — Operational Tooling (Execution Plan)

- **Status:** In progress on `feature/archive/phase-6d-operational-tooling` (branch name predates the 6d rename; it is the 6d branch). Re-scoped 2026-06-06 after re-evaluating the original three deliverables against the post-6c codebase.
- **Owning master plan:** `project_scope/archive/phase-6-execution-plan.md` §"Phase 6d — Operational Tooling" (this doc supersedes those three bullets; build from here).
- **Sequencing:** 6d is independent — any time after 6a; no dependency on 6e/6f.

## Context

The original Phase 6d listed three deliverables. Re-evaluated against what shipped in Phases 6a–6c:

1. **`re_ingest` (R2 replay, ADR 0028 Mech B / ADR 0014) — BUILD.** Fills the one unfilled recovery gap: *source response correct, our processing wrong, fix retroactively without re-hitting the source* (Pydantic normalizer fix, new bronze field parsing an already-present key, ADR 0027 hash-helper rebaseline). `recover-rejected` (#45) only un-quarantines invariant false-positives; `deep-rescan` re-hits the live source. Headline payoff: an FDA normalizer fix replays from R2 with **zero Akamai requests**. **Scope decision: JSON REST sources only** (`cpsc, fda, usda, usda_establishments, fda_press_releases`) — their `land_raw` writes `json.dumps(raw_records)`, so the inverse is a clean `json.loads`. NHTSA (flat file) / USCG (HTML) are cheaply re-fetchable → `deep-rescan` already covers them. **Packaging decision: a `recalls re-ingest` CLI command + `src/bronze/reingest.py` core**, mirroring the `recover-rejected` precedent (not the ADR's older `scripts/re_ingest.py` sketch — amended below).
2. **`backfill_manifest.py` (ADR 0028 Mech C) — BUILD, census-first.** Reconstructs the USDA presence manifest (`extraction_run_identities`, 6c migration 0027) for historical runs so `recall_lifecycle.is_currently_active`/`was_ever_retracted` extend back to a `run_id` floor. USDA-only by construction (only source that is full-dump-daily **and** stably-keyable **and** actually retracts). Modest, data-dependent payoff → **ships census-by-default with an explicit `--apply` gate**; the operator runs the census, reads the floor + un-backfillable NULL-`run_id` count, then decides.
3. **`assert_nhtsa_daily_drift_under_threshold.sql` — DROP (obsolete).** 6c's NHTSA 7-tuple + SCD-2 refactor made the volatile-field drift it targeted (Pierce-class `mfr_comp_desc` backfill) **normal SCD-2 versioning**, not silver fragmentation. The still-dangerous case (anchor-field drift) is already covered by `assert_nhtsa_eleven_tuple_identity_stable.sql` + `assert_nhtsa_maketxt_drift_caught.sql`. Documented + the stale "fragments silver rows" comment on the cumulative test refreshed.

## Validated integration design (load-bearing)

Re-ingest and backfill reuse the *middle* of the 5-step lifecycle (`src/extractors/_base.py`) — `validate_records → check_invariants` — on an instantiated extractor, but **not** `run()` or the source's `load_bronze`, because two paths through the template corrupt silver (confirmed against the code):

- **Not `run()`/`_record_run`:** writes the USDA presence manifest (`default_track_presence`); a replay of a historical payload (`started_at=now()`) would become `usda_latest_run` and flip `recall_lifecycle` dims corpus-wide. Re-ingest hand-rolls the `extraction_runs` insert and writes **no** manifest.
- **Not the source `load_bronze`:** USDA's advances the freshness watermark (`_fsis_base._update_watermark_state`); a replay would mark the live incremental "freshly run." Re-ingest writes via `BronzeLoader.from_contract(...).load(...)` — the same incremental-mode oracle `recover-rejected` uses.
- **Re-land to a fresh key (never reuse the original):** the new bronze rows must join (via `raw_landing_path`) to a `schema_rebaseline` run, or `recall_event_history` synthesizes a false edit per record. `recall_event_history` keeps the rebaseline snapshot in the `LAG` sequence but excludes it from edit detection (`recall_event_history.sql` final WHERE). Reusing the key would let the `distinct on (raw_landing_path) order by started_at desc` runs-CTE reassign the *original* routine snapshot's change_type. Content-hash dedup → a no-op replay inserts zero bronze rows.

The new per-source seam is the inverse of `land_raw`: `Extractor.parse_landed_payload(raw_bytes)` — default raises (NHTSA/USCG), `RestApiExtractor` overrides with `json.loads`.

## Deliverables (as built)

| # | Path | What | Status |
|---|---|---|---|
| 1 | `src/extractors/_base.py` | `parse_landed_payload` seam (raising default + `RestApiExtractor` json.loads override) | ✅ |
| 2 | `src/bronze/reingest.py` | `REINGEST_CONFIG_BY_SOURCE_NAME` (5 JSON sources), `replay_to_passing`, `validate_and_check`, `select_candidate_runs` (run_id-required + already-replayed skip), `reingest_window`, hand-rolled rebaseline run insert with `replayed_from_run_id` lineage | ✅ |
| 2b | `migrations/versions/0029_*.py` + `src/extractors/_tables.py` | `extraction_runs.replayed_from_run_id` (re-ingest lineage; makes replay runs distinguishable from extract re-baselines + enables idempotency-skip) | ✅ |
| 3 | `src/cli/main.py` | `recalls re-ingest <source> --from-date --to-date --change-type {schema_rebaseline,hash_helper_rebaseline} [--dry-run] [--force]` (required change_type; JSON-source gate) | ✅ |
| 4 | `scripts/backfill_manifest.py` | USDA census (default, read-only) + `--apply` insert (ON CONFLICT DO NOTHING; original run_id; NULL-run_id skip + documented floor) | ✅ |
| 5 | tests | `tests/bronze/test_reingest.py`, `tests/scripts/test_backfill_manifest.py` | ✅ |
| 6 | docs | drift-test drop note + stale-comment refresh; `operations.md`/`commands.md` re-ingest live + backfill runbook; ADR 0028/0014 status | ⏳ |
| 7 | `pyproject.toml` | version bump (minor) | ⏳ |

NHTSA `assert_nhtsa_daily_drift_under_threshold.sql`: **not built** — obsolete (see §Context #3).

## Reuse map
- `R2LandingClient.get_raw` / `land` — `src/landing/r2.py`.
- per-source `validate_records` / `check_invariants` / `land_raw` — `src/extractors/*.py`.
- `BronzeLoader.from_contract` + `DEDUP_CONTRACT_BY_SOURCE_NAME` — `src/bronze/loader.py`, `dedup_contracts.py`.
- `build_presence_manifest_rows` — `src/bronze/manifest.py`.
- extractor factory — `cli/main.py` (`load_source_config` → `EXTRACTOR_BY_SOURCE_NAME` → `build_extractor_kwargs`).
- structure precedent — `src/bronze/recovery.py` + the `recover-rejected` CLI.

## Risks (designed-out or documented)
- *Manifest poisoning* & *watermark advance* — avoided by not routing through `run()`/`load_bronze`.
- *`recall_lifecycle.edit_count` tick-up* on hash-changing rebaselines — `edit_count = count(distinct content_hash)` has no change_type filter; documented in operations.md (pre-existing property, not corruption).
- *Temporal leapfrog* — re-ingest re-coerces each payload at its **original** `extraction_timestamp` (not `now()`), so a replayed older payload can't override genuinely-newer bronze (e.g. a later re-baseline) in silver's `max(extraction_timestamp)` pick. Mirrors `recovery.seed_extraction_timestamp`. Caught during verification: re-ingesting a pre-schema-change USDA window would otherwise have leapfrogged the corrective 20:02 re-baseline and shown un-split multi-value fields as current.
- *Wrong `--change-type`* — CLI **requires** it and rejects non-rebaseline values.
- *Key reuse* — re-ingest always re-lands to a fresh uuid key.
- *NHTSA/USCG* — `parse_landed_payload` raises; CLI rejects with "use deep-rescan".
- *NULL `run_id`* historical USDA runs — un-backfillable; census reports the floor; skip-don't-fail.
- *Re-run noise / run identifiability* — a re-ingest run is otherwise indistinguishable from a normal `extract --change-type=schema_rebaseline` re-baseline (both are `schema_rebaseline`). **Resolved (migration 0029): `extraction_runs.replayed_from_run_id`** records the original run's id on each replay run, so re-ingest runs are unambiguous (`replayed_from_run_id IS NOT NULL`) and re-ingest skips already-replayed originals (`--force` overrides). This surfaced during verification — the verify script's first cut false-flagged a legitimate USDA extract re-baseline's manifest. backfill stays idempotent via `NOT EXISTS` + ON CONFLICT.

## Verification
- **Gates (every changed file incl. `scripts/**`):** `ruff check`, `ruff format --check`, `pyright`, `pytest`. ✅ for code commits 1–5.
- **re-ingest e2e (user-run):** `recalls re-ingest cpsc --from-date <d> --to-date <d> --change-type schema_rebaseline --dry-run`, then for real; confirm new bronze rows join to a `schema_rebaseline` run; `dbt build` → `recall_event_history` synthesizes **no** false edits for the wave. **Idempotency gate** (`implementation_plan.md` Phase 6 gates): re-run the window twice → second run inserts **0** bronze rows.
- **backfill (user-run):** `python scripts/backfill_manifest.py` (census), review floor + NULL-run_id count; if warranted, `--apply`, then `scripts/sql/_pipeline/verify_presence_manifest.sql` + `dbt build` → USDA lifecycle dims populate back to the floor.

## ADR amendments
- **ADR 0028** Mechanism B: implemented as the `recalls re-ingest` CLI (core in `src/bronze/reingest.py`), driven off `extraction_runs` (not R2 prefix-listing), JSON sources only — supersedes the `scripts/re_ingest.py` sketch. Mechanism C: implemented as the census-first `scripts/backfill_manifest.py`.
- **ADR 0014** re-ingest procedure: now live (`recalls re-ingest`).
