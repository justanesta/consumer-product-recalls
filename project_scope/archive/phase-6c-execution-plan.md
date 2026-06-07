# Phase 6c Execution Plan — History, Lifecycle & SCD-2 Consolidation

- **Status:** **COMPLETE (2026-06-06)** — all commits 6c.0–6c.8 landed on `feature/phase-6c-history-lifecycle`; ready for the single end-of-branch PR to `main`. The v1.5 NHTSA migration (6c.6/6c.7) corrected the anchor from the planned 6-tuple to the **7-tuple** after the full-corpus over-collapse finding (ADR 0033 amendment + ADR 0034); the ~2-week observation window was waived (re-seeded corpus + forward-only snapshots). **Structure: staged commits 6c.0–6c.8 on this single branch, then one PR to `main`** — not a PR-per-rung ladder like 6b.
- **Owning master plan:** `project_scope/implementation_plan.md` Phase 6 (the `recall_event_history` / `extraction_run_identities` / `recall_lifecycle` deliverables at `:713–718`, the cross-source SCD-2 item at `:716`, the Policy-C value-selection sub-decision at `:720–765`). **Supersedes** the terse "Phase 6c — History + Lifecycle" section of `project_scope/phase-6-execution-plan.md` (lines 279–285), which was written before USCG reactivation, before the 6b firm-resolution + USCG SCD-2 work shipped (#59), and before the W3 SCD audit.
- **Branch state confirmed:** no `recall_event_history.sql` / `recall_lifecycle.sql` exist; latest alembic migration is `0026`; the USCG SCD-2 snapshot + flag already shipped in #59 (do not re-plan — see §1.1).
- **Hard operating constraints (every actionable step):** the USER runs ALL code (extractors, alembic, psql, dbt, any CLI); agents specify the exact command/SQL only. SQL exploration lives under `scripts/sql/<source>/<layer>/<purpose>.sql` — never pasted multi-line into prose. Any helper under `scripts/` or `src/` meets the `src/` bar: `ruff check`, `ruff format --check`, `pyright`, and `pytest` coverage of pure logic. The `firm.sql ↔ recall_event_firm.sql` lockstep (only 6c.5 touches it) is binding — see that commit.
- **⚠ Read §2 (the conceptual spine) before building.** 6c has two history mechanisms that look similar and are not interchangeable; §2 is the mental model the whole plan rests on.

---

## 1. Plan vs reality reconciliation

### 1.1 What shipped in 6b (#59) — DO NOT re-plan

The headline cross-source SCD-2 work (the only *measured* Type-2 NEED) already landed, verified in the working tree:

- **USCG `firm_manufacturer_attributes` SCD-2** — `dbt/snapshots/uscg_manufacturer_attributes_snapshot.sql` (`strategy='check'`, `unique_key='mic'`, `check_cols` = company/dba/parent/past_company_1-3/address/city/state/zip/country/status/out_of_business, **excludes** `date_modified`/`in_business` heartbeats; `schema='silver_snapshots'` via `dbt/macros/generate_schema_name.sql`). `firm_manufacturer_attributes.sql` repointed to the snapshot current-view (`dbt_valid_to is null`) + derived `mic_has_prior_holder` / `mic_oob_recycled` (word-boundary `\yOOB\y`) / `prior_holders` jsonb. `recall_event_firm.sql` USCG branch stamps `match_confidence ∈ {uscg_mic_unambiguous, uscg_mic_time_sensitive_unresolved}`. Tests `assert_uscg_mic_reassignment_flag_present` + `assert_uscg_scd2_no_forked_lineage`.
- **ADR 0033** (SCD-on-stable-anchor, NHTSA proof) + **ADR 0035** (cross-source SCD-2, Policy C, dbt-snapshot mechanism, USCG-only build scope) written. The W3 SCD verdict is single-homed in `documentation/audit/scd_field_designations.md` (per-field NEED/BENEFIT catalogue + monitor registry) + `documentation/audit/cross_source_consolidation.md` §8.
- **SCD monitor SQL** exists (read-only, measure-forward): `scripts/sql/cross_source/scd_monitors/{assert_classification_stable,assert_lifecycle_stable,assert_mic_holder_stable,probe_mic_prior_holder_not_oob}.sql`.
- **NHTSA v1.5 Layer 1** (ADR 0033 + `archive/silver_v15_migration_plan.md`) merged in #32.

**Doc-staleness fix (do now):** ADR 0035 still reads `Status: Proposed` although its PR 6b.5 work merged in #59 → flip to **Accepted** per the merge-status-flip convention.

### 1.2 The three original §6c items — still valid, with deltas

All three are genuinely pending and remain the core of 6c. Each needs deltas folded in (the §6c bullet predates them):

| Original §6c item | Verdict | Deltas to fold in |
|---|---|---|
| `extraction_run_identities` table + migration (ADR 0026) | Keep — USDA-only initial scope | Resolve ADR 0026 **Q1** (thread `run_id` into `BronzeLoader.load()` vs. write-after); lifecycle dims derive from `extraction_runs.started_at` + `content_hash`, **never** `last_modified_date` (ADR 0026 Phase-5c addendum — `last_modified_date` is an unreliable per-edit signal) |
| `recall_event_history` (ADR 0022) | Keep | Now **5 sources** not 4 (USCG reactivated); **join `extraction_runs.change_type`, exclude** `schema_rebaseline`/`hash_helper_rebaseline` (ADR 0027 + `documentation/operations/re_baseline_playbook.md`); **per-field whitespace-normalize before `LAG()`** (USDA Finding Q: ~1,024 phantom re-versions per cosmetic-whitespace wave); reference **post-remap canonical columns** (ADR 0036, e.g. `recall_reason`) |
| `recall_lifecycle` | Keep — design unchanged | Depends on the 6c.0 manifest for `is_currently_active`/`was_ever_retracted`; the manifest-independent dims (`first_seen_at`/`last_seen_at`/`edit_count`) compute for all 5 sources from bronze + `extraction_runs` |

### 1.3 SCD/6c work swept in from elsewhere (was scattered)

- **ADR 0035 §5 "Phase-6c follow-on"** (all three included per the scope decision): (i) historical-interval backfill seeding SCD intervals from source-native `Past Company (OOB year)` + `In Business`; (ii) the as-of-**build-date** HIN→holder attribution join (HIN chars 9–12); (iii) a rename-vs-recycle flag tier (`(previous name)` marker; the probe `probe_mic_prior_holder_not_oob.sql` already measured ~84% genuine reassignments / ~16% same-entity renames).
- **NHTSA v1.5 Layer 2 + Layer 3** (`archive/silver_v15_migration_plan.md`; ADR 0034 stub) — folded into 6c per the scope decision. Layer 2 is unblocked now that Phase 6a.5 is complete (snapshot baseline initializes against full-corpus bronze).
- **SCD-monitor governance** (`scd_field_designations.md` §4): promote the existing monitor SQLs to `severity=warn` dbt singular tests; add the join-key erasure tripwire (`implementation_plan.md:739`).
- **BENEFIT SCD-2 dims** (`cross_source_consolidation.md` §8; ADR 0035 §3): the "nice-to-have for the other sources" — scope decision was **build all now** for portfolio breadth, reconciled in 6c.4 to avoid redundancy with `recall_event_history`.

### 1.4 Corrected starting point

6c builds on the post-#59 shape. The recall-fact history layer (`recall_event_history` / `recall_lifecycle` / the manifest) is greenfield. The dimension history layer already has its measured-NEED instance (USCG); 6c adds the deferred USCG refinements (6c.5), the BENEFIT dim snapshots (6c.4), the NHTSA product-grain SCD (6c.6/6c.7), and the monitor governance (6c.3). Exactly one alembic migration (`0027`, the manifest table); everything else is dbt-managed.

---

## 2. The conceptual spine — two kinds of history, two mechanisms

Phase 6c recovers the **time dimension** that latest-wins silver discards. It does so in two distinct ways, because *two different things* change over time. Conflating them is the main design hazard.

**Substrate.** Bronze is insert-only with content-hash dedup: per recall identity `(source_recall_id[, langcode])` it keeps **one row per distinct content version** ever seen, stamped with `extraction_timestamp` + `content_hash` (a Type-4 history store). Silver `stg_*` keeps only the latest version per key (`row_number() … order by extraction_timestamp desc` → Type-1). That `row_number()` is where time gets discarded.

**Kind 1 — the recall fact changes (edits + retractions). Synthesized, stateless.**
- `recall_event_history` runs `LAG()` over bronze's content versions → a field-level edit log ("field X changed A→B at time T"). Bronze has the versions; this **diffs** them. Re-derivable any time; no new state.
- `extraction_run_identities` (the **presence manifest**) records *which IDs were in each run's response* — set-membership, not content. It closes the one gap bronze cannot infer: a retraction produces zero new bronze rows, identical to "unchanged, dedup skipped." The manifest is the positive presence signal that makes retraction detectable.
- `recall_lifecycle` summarizes both into 5 consumer columns (`first_seen_at`, `last_seen_at`, `edit_count`, `is_currently_active`, `was_ever_retracted`).

**Kind 2 — the dimensions a recall points to change (firm address, establishment status, MIC reassignment). Materialized, stateful.**
- **SCD-2 dbt snapshots** on stable anchors (`strategy='check'` → `dbt_valid_from`/`dbt_valid_to` peer tables in `silver_snapshots`) enable **as-of-date dimensional joins**. The snapshot is irreplaceable history (Policy C: silver-current is "what the source says today," the snapshot is "what it has ever said").

| Axis | Kind 1 (fact history) | Kind 2 (dimension history / SCD-2) |
|---|---|---|
| Versions what | the recall **event** (the news fact) | the **dimensions** behind it (firm / establishment / manufacturer / NHTSA product) |
| Question | "What edits did THIS RECALL undergo? Is it still active?" | "Who held this MIC / what was this establishment's status **when** the recall happened?" |
| Mechanism | `LAG()` over bronze + the presence manifest (stateless) | dbt snapshot `strategy='check'` (stateful, materialized) |
| Driver | editorial churn + implicit deletion | anchor reuse / attribute drift; the MIC case is a **correctness** bug |

The presence/retraction axis (the manifest) has **no SCD analog** — SCD-2 only tracks attribute drift on a stable key, never existence. Both kinds live in 6c because both are "the time dimension of the data."

---

## 3. Commit sequence overview

| Commit | Title | Depends on | Gate(s) |
|---|---|---|---|
| **6c.0** | Lifecycle manifest substrate — `extraction_run_identities` migration + `BronzeLoader` per-run write (ADR 0026) | — | confirm dedup already yields the identity set |
| **6c.1** | `recall_event_history.sql` — 5-source `LAG()` + change-type exclusion + whitespace-norm (ADR 0022/0027) | — | `change_type` populated incl. rebaseline values |
| **6c.2** | `recall_lifecycle.sql` — the 5 lifecycle dims | 6c.0, 6c.1 | — |
| **6c.3** | SCD-monitor governance — promote 3 monitors to warn-tests + erasure tripwire | 6c.1 | — |
| **6c.4** | BENEFIT SCD-2 dims — `firm_establishment_attributes` + `firm_fda_attributes` snapshots | — | re-confirm 0 edit-versions (forward-banking, not corrective) |
| **6c.5** | USCG SCD refinements — rename/recycle tier + historical backfill + HIN build-date join (ADR 0035 §5) | (extends shipped 6b.5) | rename-vs-recycle split (G-rename); HIN coverage |
| **6c.6** | NHTSA v1.5 Layer 2 — parallel snapshot + `recall_product_v15` + `recall_product_history` | — | Layer-2 gate (migration plan) |
| **6c.7** | NHTSA v1.5 Layer 3 cutover (ADR 0034) | 6c.1, 6c.6 | Layer-2 evidence + ≥1 novel drift event + 6c.1 grain settled |
| **6c.8** | Acceptance — e2e simulated-drift test + gate escalation | all | — |

6c.0→6c.1→6c.2 is the hard chain. 6c.3 follows 6c.1. 6c.4, 6c.5, 6c.6 are independent after 6c.1 and overlap (the 6c.6 ~2-week observation window runs concurrently). 6c.7 is the coordinated cutover; 6c.8 last.

---

## Commit 6c.0 — Lifecycle manifest substrate (ADR 0026)

**Scope.** The bronze-side half of lifecycle tracking: a per-run presence manifest so silver can answer "is this recall currently published / was it ever retracted." USDA-only initially per the accepted ADR (strongest retraction evidence; the loader is source-parameterized so adding a source later is a config change, not a refactor).

**Gate (run FIRST).** Confirm `BronzeLoader.load()` already computes the per-batch identity tuple set during dedup (it does — the manifest reuses it, no new compute). Confirm `extraction_runs` carries `started_at` (it does).

**Files — new.**
- `migrations/versions/0027_extraction_run_identities.py` (`revision='0027'`, `down_revision='0026'`) — `CREATE TABLE extraction_run_identities (run_id text NOT NULL REFERENCES extraction_runs(run_id), source text NOT NULL, source_recall_id text NOT NULL, langcode text NULL, PRIMARY KEY (run_id, source, source_recall_id, langcode))` + `ix_eri_source_recall_lookup (source, source_recall_id, langcode)`. Schema verbatim from ADR 0026 Option A. Follow the `0023`/`0024` docstring/typed-vars/reversed-drop convention; cite ADR 0026.
- `tests/...` — pure-logic unit tests for the manifest-tuple builder (happy path, USDA bilingual langcode split, empty batch).

**Files — modified.**
- `src/bronze/loader.py` (`BronzeLoader.load()`) — write the manifest rows in the **same transaction** as bronze inserts (ADR 0020 single-transaction commit), built from the identity tuples already computed during dedup. **Resolve ADR 0026 Q1: thread `run_id` explicitly into `load_bronze()`** so the manifest write stays inside the single transaction (the less-invasive write-after-`load_bronze` alternative loses atomicity on partial failure). Gate the write behind a per-source `track_presence` flag, **USDA ON** only.

**No silver model here** — `recall_lifecycle` (6c.2) is the consumer.

**Verification (user-run).** `alembic upgrade head` creates the table; a USDA extract (`recalls extract usda`) populates it in the same transaction as bronze; `pytest` the builder; `ruff`/`pyright` clean.

**Fits design.** Mirrors ADR 0026 Option A exactly; reuses the dedup identity set; single-transaction per ADR 0020; nullable `langcode` accommodates non-bilingual sources from day one.

**Risks.** The `run_id` threading touches the `Extractor` ABC contract — keep it minimal (a parameter, not a refactor). Manifest growth (~2K rows/run USDA) — retention deferred (keep forever for now per ADR 0026).

**Docs.** `data_schemas.md` (table + columns + PK); `architecture.md` (BronzeLoader component: the in-transaction manifest write + presence-vs-content semantics); `silver_design_notes.md` (why presence is a signal bronze can't infer); `operations.md` (per-run write, retention note). ADR 0026 implementation note.

---

## Commit 6c.1 — `recall_event_history.sql` (ADR 0022 + 0027)

**Scope.** The uniform field-level edit history for all five sources, synthesized via `LAG()` over bronze snapshots (no source-asymmetric path — FDA's native history endpoints are empty, ADR 0022). One row per changed field per snapshot interval.

**Gate (run FIRST).** Confirm `extraction_runs.change_type` is populated and the non-routine values (`schema_rebaseline`, `hash_helper_rebaseline`) appear where expected (migrations `0009`/`0012` added the column).

**Files — new.**
- `dbt/models/silver/recall_event_history.sql` — partition by `(source, source_recall_id [, langcode for USDA])`, order by `extraction_timestamp`; emit `(source, source_recall_id, field_name, old_value, new_value, changed_at, content_hash)` per changed field per interval. Sources: `cpsc_recalls_bronze`, `fda_recalls_bronze`, `usda_fsis_bronze`, `nhtsa_recalls_bronze`, `uscg_recalls_bronze`.
  - **Change-type exclusion:** join `extraction_runs.change_type`; exclude `schema_rebaseline`/`hash_helper_rebaseline` rows from edit detection so parser-driven re-version waves don't synthesize false edits (ADR 0027 + `documentation/operations/re_baseline_playbook.md`).
  - **Whitespace normalization before comparison:** compare `trim(regexp_replace(value,'\s+',' ','g'))` rather than raw equality, on churn-prone text (USDA `company_media_contact`/`summary`/press-release fields; audit each source for analogues). Placed in silver where the trade-off is visible, not at the bronze hash (which would lose isolated real edits).
  - Field set keyed to the **post-remap canonical columns** (ADR 0036).
- `dbt/tests/...` — uniqueness on `(source, source_recall_id, field_name, changed_at)`; a singular test asserting a rebaseline wave produces 0 synthetic edits.

**No migration.**

**Verification (user-run).** `dbt build --select recall_event_history`; spot-check a known USDA edit (`PHA-04092026-01`) appears once; a known rebaseline wave yields 0 synthetic edits; a whitespace-churn field yields 0 phantom rows.

**Fits design.** Single uniform `LAG()` path per ADR 0022; the cosmetic-noise filter lives in silver per ADR 0027 rationale; the `fda_product_history_bronze` / `fda_event_product_history_bronze` tables stay unused (endpoints empty) — if FDA ever populates them, a new ADR + a `UNION` branch.

**Risks.** Per-field unpivot across heterogeneous source schemas is verbose — generate from a per-source field list. USDA bilingual: partition must include `langcode` (EN/ES diverge, ADR 0026 §U2).

**Docs.** `data_schemas.md` (model + columns); `silver_design_notes.md` (the `LAG()` mechanism, change-type exclusion, whitespace rationale, FDA empty-native-history note); `architecture.md` (silver-layer narrative). ADR 0022 confirmation note.

---

## Commit 6c.2 — `recall_lifecycle.sql`

**Scope.** The consumer-facing lifecycle dimension, derived on top of 6c.1 + the 6c.0 manifest + `extraction_runs`.

**Files — new.**
- `dbt/models/silver/recall_lifecycle.sql` — per identity: `first_seen_at = MIN(extraction_runs.started_at)`, `last_seen_at = MAX(...)`, `edit_count = COUNT(DISTINCT content_hash)` (all five sources, manifest-independent); `is_currently_active` (identity in the most-recent **enumerating** run's manifest — the latest run that *wrote* manifest rows, NOT merely the latest success: a 304-Not-Modified run succeeds but enumerates nothing, so it writes no manifest; proven 2026-06-06) + `was_ever_retracted` (present in fewer enumerating runs than exist since first appearance — captures mid-lifespan toggles AND end retraction) for **USDA only** in v1. **Revised at build (2026-06-06):** the original "USDA + NHTSA" was wrong — NHTSA full-enumerates but `track_presence` is OFF for it, and bronze can't substitute (content-hash dedup hides unchanged campnos; deriving presence from raw R2 files is the rejected R2-manifest approach). So is_currently_active / was_ever_retracted are USDA-only until `track_presence` is enabled per source (a one-line `DedupContract` flip — the full-enumeration prerequisite); CPSC/FDA/NHTSA/USCG are NULL meanwhile. ADR 0026's source-parameterized design makes adding them additive. first_seen_at is built from bronze `extraction_timestamp` (not `extraction_runs.started_at`) and is bounded by the 6a.5 reseed (pipeline-observation, not recall age — use `announced_at`).
- `dbt/tests/...` — uniqueness on the identity; `is_currently_active` not-null for USDA/NHTSA.

**No migration.**

**Verification (user-run).** `dbt build --select recall_lifecycle`; spot-check the `PHA-04302026-01` retract/republish toggles → `was_ever_retracted = true`; an active recall → `is_currently_active = true`.

**Fits design.** Reads history rows, never the reverse (the ordering constraint). **Never** uses `last_modified_date` (ADR 0026 Phase-5c addendum). Derivations exactly per ADR 0026's table.

**Risks.** `was_ever_retracted` correctness depends on run completeness — restrict to `status='success'` runs. Partial cross-source coverage is intentional and documented, not a silent gap.

**Docs.** `data_schemas.md` (5 columns + derivation rules); `silver_design_notes.md` (derivations, manifest-dependence, why not `last_modified_date`).

---

## Commit 6c.3 — SCD-monitor governance

**Scope.** Wire the existing read-only monitor SQL into CI as `severity=warn` dbt singular tests so designations stay honest as incrementals re-bank history, and add the join-key erasure tripwire.

**Files — new (dbt singular tests in `dbt/tests/source_assumptions/`, mirroring the existing `assert_*` convention).**
- `assert_classification_stable.sql`, `assert_lifecycle_stable.sql`, `assert_mic_holder_stable.sql` — port the logic from `scripts/sql/cross_source/scd_monitors/` (keep the script copies as the ad-hoc exploration home; the dbt tests are the CI surface).
- `assert_no_join_key_erasure_usda.sql` — the `implementation_plan.md:739` query shape: warn when a join-key text field is populated in a prior snapshot and empty in the current one. Seed fields: USDA recalls `establishment`, `company_media_contact`. Generalize via a per-column config (vars block) so adding a monitored field is a one-line change.

**No migration.**

**Verification (user-run).** `dbt test --select source_assumptions` — all run, at `warn`. The classification/lifecycle monitors read ~0 today (re-seeds wiped versions) → they **measure forward**.

**Fits design.** Unifies the scattered change-detection asserts under SCD governance (`scd_field_designations.md` §4–5). Warn-level matches the existing source-assumption convention; promote to `error` per-monitor once a designation graduates ASSUMED→MEASURED.

**Risks.** None material (read-only, warn-level). Document in `scd_field_designations.md` that a monitor going non-zero is the trigger to (re)open the SCD-2 build decision for that field.

**Docs.** `scd_field_designations.md` (§4 registry: mark each monitor "wired as dbt warn-test"; update the validation-loop note); `silver_design_notes.md` (the monitor governance loop); `architecture.md` (testing/quality note).

---

## Commit 6c.4 — BENEFIT SCD-2 dims (build all now)

**Scope.** Demonstrate the SCD-2 pattern across the dimension sidecars (portfolio breadth). **Reconciliation:** `classification`/`severity` + `lifecycle_status` are recall_event-grain attributes **already captured by `recall_event_history`** (6c.1) — a separate recall_event snapshot would be redundant, so the BENEFIT build targets the **dimension tables not covered by event history**:

- **`firm_establishment_attributes` (USDA)** — `dbt/snapshots/firm_establishment_attributes_snapshot.sql`, `strategy='check'`, `unique_key='establishment_number'`, `schema='silver_snapshots'`, `check_cols` incl. `status_regulated_est` (the 2026-05-15 active→Inactive flip — the original motivating case), address, activities; driven from `stg_usda_fsis_establishments` (latest-per-`establishment_number`). Repoint `firm_establishment_attributes.sql` to the snapshot current-view (`dbt_valid_to is null`). `_snapshots.yml`: `unique_combination_of_columns: [establishment_number, dbt_valid_from]`.
- **`firm_fda_attributes` (FDA)** — `dbt/snapshots/firm_fda_attributes_snapshot.sql`, `unique_key='firm_fei_num'`, `check_cols` = the firm address/name fields; repoint `firm_fda_attributes.sql` to the current-view.
- **CPSC** — monitors only (name-keyed Type-1; fragmentation is a 6b normalization concern, not SCD).

Mirror the shipped USCG pattern exactly (`uscg_manufacturer_attributes_snapshot.sql` is the template).

**Gate (run FIRST).** Re-confirm 0 edit-versions per anchor for FDA/USDA establishments (the snapshot-hypothesis status, `scd_field_designations.md`) so the build is documented as **forward-banking, not corrective**.

**Honest caveat (state in docs — "no silent caps").** These snapshots bank exactly one version per anchor at first run and grow only as incrementals re-bank history (0 edit-versions post-reseed). Real infra, empty history today — the deliberate breadth demonstration.

**No migration** (dbt-managed snapshot DDL).

**Verification (user-run).** `dbt snapshot --select firm_establishment_attributes_snapshot firm_fda_attributes_snapshot` (banks current) → `dbt build --select firm_establishment_attributes firm_fda_attributes` → re-run `dbt snapshot` on an unchanged corpus → 0 spurious versions (`strategy='check'` idempotency).

**Fits design.** Policy C cross-source standard (ADR 0035); same primitive as USCG; `silver_snapshots` already provisioned + ADR 0007 pruning-exempt (extend the exemption note to the new snapshots).

**Risks.** A heartbeat field left in `check_cols` would spawn phantom versions (audit each source's churn fields, mirroring the USCG `date_modified`/`in_business` exclusion — e.g. USDA `latest_mpi_active_date` is already hash-excluded per ADR 0032; keep it out of `check_cols`).

**Docs.** `data_schemas.md` (snapshot rows in `silver_snapshots`, current-view repoint); `silver_design_notes.md` (Policy C, `strategy='check'`, the empty-history caveat, the heartbeat-exclusion rule); `architecture.md` (`silver_snapshots` schema). ADR 0007 pruning-exemption extension.

---

## Commit 6c.5 — USCG SCD refinements (ADR 0035 §5, all three)

**Scope.** Three refinements on top of the **already-shipped** USCG snapshot + flag. All **lockstep-safe** — additive bridge enrichment; the `firm_id` recipe and `firm.sql uscg_normalized` are untouched (the only lockstep-relevant commit in 6c; the bridge LEFT JOIN already exists from 6b.5).

**(a) Rename-vs-recycle flag tier (ship first — cheapest).** Split the binary flag: keep `uscg_mic_time_sensitive_unresolved` for OOB / unmarked-distinct-firm reassignments; **downgrade** source-tagged `(previous name)` renames (same entity) to `uscg_mic_unambiguous`.
- **Gate (run FIRST):** validate `(previous name)`-marker coverage vs the unmarked-rename tail using the existing `scripts/sql/cross_source/scd_monitors/probe_mic_prior_holder_not_oob.sql` (measured ~84% genuine reassignments / ~16% renames). Lock the rule only if the `(previous name)` marker reliably tags renames.
- Edit `firm_manufacturer_attributes.sql` (a `mic_renamed_not_recycled` derive) + the `recall_event_firm.sql` USCG branch's `match_confidence` CASE.

**(b) Historical-interval backfill.** Seed SCD intervals from source-native `Past Company (OOB year)` + `In Business`; undated entries get **open-ended low-confidence** intervals.
- **Honest payoff:** only ~13/221 OOB years parse (~6%) — document the coverage explicitly; this is a recall (completeness) refinement, not a correctness gap.
- A backfill model/seed feeding the snapshot history with the parsed intervals; flag low-confidence rows.

**(c) As-of-build-**year** join.** Which holder held the MIC when the boat was built; stamp `match_confidence = uscg_mic_build_date_resolved` (enum reserved in 6b.0) where the build year resolves to a single historical holder.
- **All dbt-SQL, no Python** (revised 2026-06-06, user-prompted). The source's reassignment markers are `(OOB YYYY)` — **year**-grain — and the recall already carries **`model_year`** as a first-class field (`stg_uscg_recalls.model_year`). So the join is a deterministic `model_year` vs MIC-reassignment-year comparison in SQL — no HIN parse needed for the common case. The original "Python HIN parser in `src/enrichment/`" was a mis-applied extractor-`_parse_*` precedent: a HIN decode is a deterministic `substring`+`CASE` transform, which per ADR 0027 belongs in **dbt-SQL** (a macro), not a Python stage (contrast ADR 0037's RapidFuzz, which genuinely can't run in-warehouse).
- A HIN-year decode is at most a *fallback* (recover `model_year` when null/`9999`); build it as a dbt macro `{{ hin_build_year(hin) }}` only if Gate G-c shows the fallback is worth it. ~52.8% of USCG recalls have a HIN (`field_audit_2026_w22 §4`).
- **HIN encoding (33 CFR 181, for the macro)** — 12 chars: `[1-3]` MIC · `[4-8]` serial (excludes O/I/Q) · `[9]` build month `A=Jan … L=Dec` (**`I`=September** — the serial drops I/O/Q but the month uses I) · `[10]` last digit of build year (decade-ambiguous on its own) · `[11-12]` model year, 2-digit (needs a decade pivot, e.g. `26`→2026 vs `95`→1995). Model-year window shifted Aug1–Jul31 → Jun1–Jul31 for MY2019+ (designated by the ending year), but our reassignment markers are year-grain `(OOB YYYY)`, so sub-year detail is noise. The macro returns `substring(hin,11,2)` pivoted to 4 digits; prefer the `model_year` field, fall back to this only when it's null.

**No migration.**

**Verification (user-run).** `dbt build --select firm_manufacturer_attributes recall_event_firm`; `dbt test --select recall_event_firm assert_uscg_mic_reassignment_flag_present assert_uscg_scd2_no_forked_lineage` (lockstep relationships still pass); spot-check a `(previous name)` MIC → `uscg_mic_unambiguous`; a HIN-resolvable pre-reassignment recall → `uscg_mic_build_date_resolved`; `pytest tests/enrichment/`.

**Fits design.** Discharges the recall-side refinements ADR 0035 §5 deferred; the correctness NEED was already met by the 6b.5 flag. Additive only — lockstep preserved.

**Risks.** HIN parse edge cases (pre-1984 formats, missing/`N/A` HINs) — precision-first: only stamp `build_date_resolved` on a confident single-holder resolution, else keep the time-sensitive flag. Backfill date sparsity is documented, not hidden.

**Docs.** `data_schemas.md` (new `match_confidence` values, `prior_holders` / backfill semantics); `silver_design_notes.md` + `firm_manufacturer_attributes` notes; `documentation/uscg/` (HIN parse + OOB-year coverage); glossary: as-of-build-date, HIN. ADR 0035 §5 status (refinements built).

---

## Commit 6c.6 — NHTSA v1.5 Layer 2 (parallel prototype)

> **As-built (2026-06-06):** the full-corpus Layer-2 build showed the 6-tuple **over-collapses** (`recall_product` −127,163 / −40%, 99.4% structural `mfr_comp_ptno` multi-part — not drift), so the anchor shipped as the **7-tuple** (+`mfr_comp_ptno`; ADR 0033 amendment 2026-06-06) with `check_cols` = `desc`/`name`/`bgman`/`endman` + widened business fields, and the ~2-week observation window was **waived** (re-seeded corpus + forward-only snapshots can't populate it; full-corpus diff + the 6c.8 sim-drift test substitute). The 6-tuple / window / `compare_v1_v15_cardinality` / `assert_pre_2008_six_tuple_unique` items below are the plan-as-conceived; the as-built record is [ADR 0034](../documentation/decisions/0034-nhtsa-silver-v15-migration.md) + `archive/silver_v15_migration_plan.md` + `silver_design_notes.md` §12.

**Scope.** The `archive/silver_v15_migration_plan.md` Layer-2 deliverables — a product-grain SCD running **alongside** v1 silver, reversible, no consumer impact. Now unblocked (Phase 6a.5 complete → the snapshot baseline initializes against full-corpus NHTSA bronze).

**Files — new.**
- `dbt/snapshots/nhtsa_recall_product_snapshot.sql` — 6-tuple `unique_key (campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id)`, `strategy='check'`, `check_cols` = the 5 attribute fields, `schema='silver_snapshots'`.
- `dbt/models/staging/stg_nhtsa_recalls_current.sql` — `DISTINCT ON (6-tuple) … ORDER BY 6-tuple, extraction_timestamp DESC` (latest bronze row per logical product).
- `dbt/models/silver/recall_product_v15.sql` (current-view, `dbt_valid_to is null`, same column shape as `recall_product`) + `recall_product_history.sql` (full versioned view).
- `scripts/sql/nhtsa/silver/compare_v1_v15_cardinality.sql`; `dbt/tests/source_assumptions/assert_pre_2008_six_tuple_unique.sql`.

**Gate to Layer 3.** The migration-plan Layer-2 gate: cardinality reduction matches predictions (Pierce −96, Nissan −2), snapshot behaves across daily regen, no new 6-tuple drift in the window, downstream-commitment check. Write a brief evidence summary.

**No migration.**

**Verification (user-run).** `dbt snapshot --select nhtsa_recall_product_snapshot` (one version per logical product at init); `dbt build --select recall_product_v15 recall_product_history`; `compare_v1_v15_cardinality.sql` shows v1.5 ≤ v1 matching known events; ~2-week observation runs concurrently with 6c.4/6c.5.

**Fits design.** ADR 0033 Layer 2 verbatim; parallel models, reversible (drop = delete files + one table). Event-grain history (6c.1) is unaffected — different grain (see 6c.7).

**Risks.** Pre-2008 NULL `rcl_cmpt_id` 5-tuple collisions (the defensive test catches it). Snapshot init against full-corpus bronze (~thousands of versions) — dry-run check.

**Docs.** `data_schemas.md`; `silver_design_notes.md` (the 6-tuple anchor + parallel-run); `operations.md` (`dbt snapshot` step + observation window).

---

## Commit 6c.7 — NHTSA v1.5 Layer 3 cutover (ADR 0034)

> **As-built (2026-06-06):** executed as described (recall_product → snapshot current view; `recall_product_v15` dropped; `recall_product_history` kept) on the **7-tuple** recipe (not the 6-tuple referenced below), plus a deterministic tiebreaker on `stg_nhtsa_recalls_current` (provable snapshot idempotency). Pre-condition #2 (a novel drift event in the window) is N/A — the window was waived; the 6c.8 simulated-drift test substitutes. ADR 0034 = Accepted; ADR 0033 = Accepted; ADR 0031 NHTSA row migrated.

**Resolve the integration-mode fork (migration-plan Open Q#3 / Layer-3 pre-cond #3).** `recall_event_history` is **event-grain** (`source, source_recall_id`); the v1.5 snapshot is **product-grain**. They are **complementary, not competing** — ADR 0033 keeps the event-grain `LAG()` unchanged and adds a parallel product-grain layer. **Decision: `recall_event_history` stays on `LAG()`-over-bronze and does NOT consume the snapshot.** The only real coordination is the Layer-3 re-key of `recall_product_id` consumers — sequenced after 6c.1's grain is settled.

**Scope (gated, one-way).** Rewrite `recall_product.sql` to consume `nhtsa_recall_product_snapshot` (effectively rename `recall_product_v15` → `recall_product`); drop `recall_product_v15.sql`; keep `recall_product_history.sql`; re-key any model joining `recall_product_id`.

**Pre-conditions to fire (all four, per the migration plan):** Layer-2 gate passes; ≥1 additional novel drift event observed in the window; 6c.1's `recall_event_history` grain settled (it is — event-grain, no dependency on the product key); no consumer blocker.

**Files — modified.** `recall_product.sql` (rewrite); ADRs `0034` (Proposed→Accepted, cite Layer-2 evidence), `0033` (Proposed→Accepted), `0031` (NHTSA per-source row → "migrated per ADR 0034"); `archive/silver_v15_migration_plan.md` (Layer 3 complete).

**No migration.**

**Verification (user-run).** `dbt build`; post-cutover relationships/uniqueness tests green; `recall_product` cardinality reflects the 6-tuple recipe.

**Fits design.** Treat as irreversible (consumers re-keyed). Event history untouched by the re-key (different grain).

**Risks.** Downstream re-key blast radius — sized at gate time (as of now the only known consumer is internal). If a Phase 8 API contract or BI dashboard has shipped against the 11-tuple key, defer.

**Docs.** `data_schemas.md` (recipe change); `silver_design_notes.md`; `operations.md` (consumers re-keyed). ADR 0034/0033/0031 edits.

---

## Commit 6c.8 — Acceptance & quality gates

> **As-built (2026-06-06):** the in-infra automated drift proof is a `recall_event_history` unit-test case (R2: a tracked field `value→''` **field-clearing erasure EMITS** — the real model logic, non-circular; norm folds whitespace + `''`↔NULL, so this proves `value→''` is *not* folded). `recall_event_history` does NOT track `establishment`, so the literal PHA-04302026-01 *establishment* erasure is the `assert_no_join_key_erasure_usda` tripwire's domain — a separate mechanism. **Two items deferred to Phase 7's DQ framework** (Soda/GE — already where `dbt_project.yml` routes warn→error; scoped in detail in `implementation_plan.md` Phase 7 → "Data-quality assertion maturation"): (1) **monitor escalation** — the SCD monitors (`assert_classification_stable`/`lifecycle`/`mic_holder`) are *forward-measuring*, so escalating to error would fail the build on the first *expected* drift; the `dbt_project.yml` policy defers escalation to the threshold-aware DQ framework. (2) a **synthetic tripwire-fire test** — there is no dbt-fixture-DB harness (the integration tests are HTTP-cassette *extractor* tests), so a hand-crafted fire-test would re-run copied SQL on copied data (circular / false coverage); the tripwire's positive case is the real PHA-04302026-01 event, its negative case verified live (0 rows). The **snapshot versioning** acceptance is the 6c.7 live cutover (320,303 = v1 − 1,237; idempotency `INSERT 0 0`; uniqueness; `characterize_v15_*`), not a new test — version-on-`check_col`-change is dbt-core, proven external (ADR 0033). Full `dbt build`+`dbt test` **green** (247 pass / 4 pre-existing source-data warns, none cutover-related). Version **0.16.1 → 0.17.0** (minor).

**Scope.** The Phase-6 history quality gate + final coherence.
- **e2e simulated-drift test** (`implementation_plan.md:776`): recreate the `PHA-04302026-01` erasure pattern in test fixtures; assert `recall_event_history` synthesizes the edit and `assert_no_join_key_erasure_usda` fires; assert `recall_lifecycle` resolves `was_ever_retracted`.
- Escalate monitors `warn → error` where the corpus signal is now known.
- Full `dbt build` + `dbt test` green.
- Version **minor** bump (user edits `pyproject.toml`; `recalls version` reads via importlib.metadata).

**Verification (user-run).** the e2e test passes; full suite green.

**Docs.** `architecture.md` (testing strategy); `silver_design_notes.md` (final coherence pass over the 6c surfaces).

---

## Documentation sync (per-commit)

Every 6c change documents **what the table/view/mechanism is, how it works, and what it enables** — in the same commit, not deferred to 6f. Respects the **single-home rule** (`documentation/documentation_model.md`): *shape* → `data_schemas.md`, *mechanism + rationale* → `silver_design_notes.md`, *component + flow* → `architecture.md`, *runbook/cadence* → `operations.md`, *decision/why* → the ADR. The single end-of-branch PR is the review surface. **Phase 6f still owns the diagrams** (DAG refresh + silver-gold ERD) and the final coherence pass — 6c does the prose/table-level sync only, so 6f never redraws against stale text. Per-commit doc targets are listed in each commit's **Docs** line above.

**Cross-cutting.**
- **Glossary** (`data_schemas.md`): SCD Type-2, Type-4 bronze store, presence manifest, as-of-date / as-of-build-date join, `valid_from`/`valid_to`, HIN⊃MIC.
- **ADR status flips** (ratify at the single end-of-branch PR merge): 0035 Accept (now — catching up the flip missed when its 6b.5 work merged in #59); 0026 implementation note (6c.0); 0022 confirmation (6c.1); 0007 pruning-exemption extended (6c.4); 0034 Accept + 0031 NHTSA-migrated row + 0033 Accepted (6c.7).
- **Audit docs** (`scd_field_designations.md`): flip monitor rows "wired as dbt warn-test" (6c.3) and ASSUMED→MEASURED as data accrues.

---

## Sequencing (folds into `branch_sequencing_strategy.md`)

`6c.0 → 6c.1 → 6c.2` is the hard chain (manifest → history → lifecycle). `6c.3` after 6c.1. `6c.4`, `6c.5`, `6c.6` are independent after 6c.1 and can interleave; the 6c.6 ~2-week observation window overlaps 6c.4/6c.5. `6c.7` is gated by the 6c.6 Layer-2 gate **and** 6c.1's grain decision. `6c.8` last. **All staged as commits on the single branch `feature/phase-6c-history-lifecycle`; one PR to `main` at the end.** The v1.5 Layer-2 observation window means either that PR stays open across the ~2-week window, or 6c.6/6c.7 land as a fast-follow PR — decide at the 6c.6 gate. Per `branch_sequencing_strategy.md`, 6c sits after the silver-remap on the dependency graph; this plan supersedes that graph's bare "recall_event_history + recall_lifecycle" 6c node with the full scope here.

---

## Appendix A — Corpus gates (every script the USER runs)

| Gate | Script | Commit | Tag | Decision |
|---|---|---|---|---|
| dedup-yields-identity-set | (inspection of `BronzeLoader`) | 6c.0 | confirm-in-code | reuse the dedup tuples; no new compute |
| change_type-populated | `scripts/sql/cross_source/...` (NEW) | 6c.1 | needs-requery | confirm rebaseline values present before relying on the exclusion |
| benefit-dims-0-edit-versions | `scripts/sql/{usda_establishments,fda}/...` (extend existing) | 6c.4 | confirm-in-doc | document forward-banking, not corrective |
| rename-vs-recycle split | `scripts/sql/cross_source/scd_monitors/probe_mic_prior_holder_not_oob.sql` (EXISTS) | 6c.5 | confirm-in-doc | lock the `(previous name)` downgrade only if the marker is reliable |
| HIN coverage | `scripts/sql/uscg/...` (NEW) | 6c.5 | needs-requery | size the build-date-resolvable fraction (~52.8% have a HIN) |
| Layer-2 cardinality | `scripts/sql/nhtsa/silver/compare_v1_v15_cardinality.sql` (NEW) | 6c.6 | needs-requery | v1.5 ≤ v1, deltas match known events → Layer-3 gate |

## Appendix B — Schema-change summary

- **Migrations:** exactly **one** — `0027_extraction_run_identities.py` (6c.0). Everything else dbt-managed.
- **New dbt snapshots** (`silver_snapshots`, ADR 0007 pruning-exempt): `nhtsa_recall_product_snapshot` (6c.6), `firm_establishment_attributes_snapshot` + `firm_fda_attributes_snapshot` (6c.4). USCG snapshot already exists.
- **New silver models:** `recall_event_history` (6c.1), `recall_lifecycle` (6c.2), `recall_product_v15` + `recall_product_history` (6c.6; v15 dropped at cutover 6c.7).
- **New silver columns (dbt-computed):** USCG `mic_renamed_not_recycled` + new `match_confidence` values (6c.5).
- **New dbt tests:** 3 promoted monitors + erasure tripwire (6c.3), `assert_pre_2008_six_tuple_unique` (6c.6), `recall_event_history`/`recall_lifecycle` uniqueness + the e2e drift test (6c.1/6c.2/6c.8).
- **New Python:** manifest-tuple builder (6c.0) only. 6c.5 is all dbt-SQL (an optional `hin_build_year` macro, not a Python parser — revised 2026-06-06). **No `CREATE EXTENSION`.**

## Phase 6c Quality Gates

- [ ] `extraction_run_identities` populated in-transaction on a USDA extract.
- [ ] `recall_event_history` captures a simulated schema-drift event in an e2e test (Phase 6 master gate).
- [ ] `recall_lifecycle` resolves `is_currently_active` / `was_ever_retracted` for USDA + NHTSA.
- [ ] SCD monitors run as dbt warn-tests; designations honest.
- [ ] BENEFIT + USCG-refinement + NHTSA v1.5 snapshots idempotent (0 spurious versions on unchanged corpus).
- [ ] All dbt tests pass; lockstep relationships test green after 6c.5.
- [ ] Each commit's prose docs updated (single-home rule); ADR status flips landed.

## References

- `project_scope/implementation_plan.md` Phase 6 (`:701–776`) — master deliverables + the Policy-C sub-decision.
- `project_scope/archive/silver_v15_migration_plan.md` — NHTSA v1.5 Layer 1/2/3.
- `documentation/decisions/{0022,0026,0027,0031,0032,0033,0034,0035,0036}.md`.
- `documentation/audit/{scd_field_designations,cross_source_consolidation}.md` — the W3 SCD verdict + monitor registry.
- `project_scope/phase-5d-uscg-manufacturers-detail.md` §11 — the USCG SCD-2 build spec (the §5 refinements built in 6c.5).
- `project_scope/archive/phase-6b-execution-plan.md` — structural precedent + the shipped 6b.5 USCG SCD-2.
- `project_scope/branch_sequencing_strategy.md` — dependency graph.
