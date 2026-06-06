# Branch sequencing and workflow strategy

- **Status:** Active — as of 2026-06-01: Phase 6a audit done (#39), Phase 6a.5 historical backfill in progress (FDA full-corpus seed run 2026-06-01); the USCG detail-capture branch merged (#42).
- **Scope:** Coordinates concurrent workstreams off `main` — Phase 6 execution, NHTSA silver v1.5 migration, weekly daily-findings branches, and (now merged) USCG manufacturer detail capture (Phase 5d Step 7 detail)
- **Supersedes:** Branching guidance scattered across `project_scope/silver_v15_migration_plan.md` (its "Branching strategy" section, now a pointer back here), `project_scope/phase-6-execution-plan.md`, and prior weekly findings retros
- **Sunset condition:** Phase 6 complete + v1.5 migration landed (Layer 3 merged); update or delete this doc when both are true

## Workstreams in flight

| Workstream | Plan doc | Branch name pattern |
|---|---|---|
| NHTSA silver v1.5 migration | `project_scope/silver_v15_migration_plan.md` | `feature/silver-v15-scd-prototype` (Layer 2), `feature/silver-v15-migration` (Layer 3) |
| Phase 6 execution (6a foundation audit → 6b firm res → 6c history → 6d ops → 6e gold → 6f diagrams) | `project_scope/phase-6-execution-plan.md` | `feature/phase-6{a,b,c,d,e,f}-<topic>` — one branch per phase letter |
| Daily findings + diagnostic scripts | (per-branch staged-tasks doc) | `docs/findings-YYYY-Wn` — one branch per ISO week, short-lived |
| USCG Phase 5d detail capture (Path B, **bronze-only**) — ✓ MERGED (#42) | `project_scope/phase-5d-uscg-manufacturers-detail.md` | `feature/uscg-manufacturers-detail-addition` — merged in PR #42; SCD-2 silver half deferred to Phase 6b (ADR 0035) |

## Dependency graph

```
main (post-USCG)
  │
  ▼
feature/phase-6a-foundation-audit  — audit docs DONE (#39); silver fixes → the remap;
              hard prereq for 6b/6c/6e (§ Sequencing Constraints)
  │
  ▼
Phase 6a.5  — historical backfill (CPSC/NHTSA/FDA full-corpus seed)
              + Neon tier upgrade + production cutover  (§ 6a.5; § Sequencing Constraints "6a precedes 6a.5")
  │
  ▼
feature/silver-field-remap  — audit-driven staging/silver fixes on full-corpus bronze;
              builds cross_source_consolidation.md; precedes 6b/6c  (§ Sequencing Constraints)
  │
  ▼
  ├──> feature/silver-v15-scd-prototype (Layer 2) — STARTS AFTER 6a.5 (not just 6a):
  │       full-corpus snapshot baseline + NHTSA remap columns; its ~2-wk observation
  │       window runs CONCURRENTLY with 6b/6c/6d below
  ├──> feature/phase-6c-history-lifecycle — recall_event_history + recall_lifecycle
  └──> feature/phase-6b-firm-resolution — RapidFuzz + USCG SCD-2 dim (ADR 0035);
          needs the remapped silver baseline + 6a foundation
  │
  ▼
feature/silver-v15-migration (Layer 3 cutover) — gated by Layer 2 evidence +
              6c's recall_event_history integration decision (pre-cond #3);
              re-keys recall_product_id consumers → COORDINATE the cutover with
              6c/6b (not free parallel)
  │
  ▼
feature/phase-6d-operational-tooling
  │
  ▼
feature/phase-6e-gold-layer
  │
  ▼
feature/phase-6f-diagrams  (last — diagrams freeze the schema)
  │
  ▼
(Phase 6 complete)

Throughout: docs/findings-YYYY-Wn branches ship daily/weekly, low conflict
risk because file scope is documentation/ and scripts/sql/ only.
```

> **Phase 6c scope expanded + v1.5 folded in (2026-06-06).** `project_scope/phase-6c-execution-plan.md` now owns Phase 6c as a sequence of commits **6c.0–6c.8 on the single `feature/phase-6c-history-lifecycle` branch + one PR** (not a PR-per-rung ladder), and **absorbs NHTSA v1.5 Layer 2 + Layer 3** (the `feature/silver-v15-*` nodes above) because Layer 3 is gated on 6c's `recall_event_history` grain decision (`silver_v15_migration_plan.md` Open Q#3). The standalone v1.5 branch names are retained for reference, but the work lands as 6c commits. The bare "recall_event_history + recall_lifecycle" 6c node above is superseded by that doc's full scope (manifest, history, lifecycle, monitor governance, BENEFIT SCD-2 dims, USCG §5 refinements, v1.5).

**USCG Phase 5d detail-capture branch (`feature/uscg-manufacturers-detail-addition`).** **Merged in PR #42** (2026-05-30); **bronze-capture only** (migrations `0017`/`0018` + `UscgManufacturerDetailExtractor` + schema + thin `stg_uscg_manufacturer_details.sql`). It was **independent of Phase 6a** — touches no silver `firm.sql` / field-mapping files — so it landed before/parallel to 6a. Its **still-deferred** SCD-2 silver half (`firm_manufacturer_attributes` history + the as-of-build-date / flag-as-time-sensitive recall join) is **Phase 6 work (new ADR 0035)** deliberately kept off-branch to avoid a `firm.sql` ↔ `recall_event_firm.sql` lockstep collision with Phase 6b — bundle it into the Phase 6b PR that already edits both files. Conflict risk: **low** (new files + bronze/staging only). Plan: `project_scope/phase-5d-uscg-manufacturers-detail.md`.

## Recommended sequence (rationale per step)

1. **`feature/phase-6a-foundation-audit` first.** Hard prereq per `phase-6-execution-plan.md` § Sequencing Constraints. Corrects silver field mappings (FDA `description ← distribution_area_summary_txt` is the known case; CPSC/USDA/NHTSA likely have analogues). Building any of 6b/6c/6e on broken foundations bakes in rework.
2. **Then Phase 6a.5 (historical seed + Neon tier upgrade + production cutover), then `feature/silver-field-remap`.** Hard chain per `phase-6-execution-plan.md` § Sequencing Constraints: 6a → 6a.5 → silver-remap → 6b/6c. The remap makes per-source silver identity/grain decisions on full-corpus bronze (and builds `documentation/audit/cross_source_consolidation.md`) — the right place to fold in the **cross-source SCD-applicability** question (which of CPSC/FDA/USDA/USCG-recalls also warrant a stable-anchor snapshot dim; `silver_v15_migration_plan.md` Open Q#4).
3. **After the remap: `feature/silver-v15-scd-prototype` (Layer 2), `feature/phase-6c-history-lifecycle`, and `feature/phase-6b-firm-resolution`.** Layer 2 **starts after 6a.5, not after 6a**: its dbt snapshot must initialize against the full-corpus NHTSA bronze so the 6a.5 PRE_2010 backfill doesn't land as a spurious version-wave, and the 6-tuple anchor is validated against the full corpus; it also mirrors NHTSA's remapped `recall_product` columns (`silver_v15_migration_plan.md` Open Q#2 + the `full_corpus_validation` principle). The *dbt mechanics* can be authored anytime (cheap, reversible), but the *snapshot baseline + ~2-week observation* that feeds the Layer 3 gate must run post-6a.5. All three branches add/edit non-overlapping files (Layer 2: new `recall_product_v15.sql`/`recall_product_history.sql`/snapshot; 6c: `recall_event_history.sql`/`recall_lifecycle.sql`; 6b: `firm.sql`/`recall_event_firm.sql`), so Layer 2's observation window runs concurrently with 6b/6c/6d. Coordinate at gate time on whether 6c consumes the v1.5 snapshot directly (ADR 0033 forward-integration note).
4. **Then `feature/silver-v15-migration` (Layer 3).** Gated by Layer 2 evidence + Phase 6c's `recall_event_history` integration decision (migration-plan pre-condition #3); it re-keys `recall_product_id` consumers, so it's a **coordinated cutover with 6c/6b**, not free parallel.
5. **Then `feature/phase-6d` → `feature/phase-6e` → `feature/phase-6f`.** Per the execution plan's sequencing constraints (6f last because diagrams freeze schema).
6. **Daily/weekly findings throughout.** `docs/findings-YYYY-Wn` branches stay tiny (days, not weeks), open PR + merge same day or next.

## Git workflow

### Sync ritual — after a PR and merge of a branch into `main`

```bash
# Make sure you are on main after running gh pr merge <pr_num> --squash --delete-branch
git pull # (or git pull origin main to be specific) 
git checkout <current-branch>
git rebase origin/main         # replays your commits on top of latest main
# resolve any conflicts: edit, git add <file>, git rebase --continue
git push --force-with-lease origin <current-branch>   # if previously pushed
```

Rebase (not merge) keeps each branch's history linear and surfaces conflicts incrementally per commit.

### Starting a new branch off main

```bash
git checkout main
git pull origin main
git checkout -b <new-branch-name>
# ... work ...
git push -u origin <new-branch-name>
```

### Pre-merge ritual

```bash
# Identify likely conflicts before starting:
git fetch origin main
comm -12 \
  <(git diff origin/main...<branch> --name-only | sort) \
  <(git diff <branch>...origin/main --name-only | sort)

# Rebase + resolve:
git rebase origin/main

# Open PR:
gh pr create --base main --head <branch> \
  --title "<concise summary>" \
  --body "<scoped to what's in the diff>"
```

### Cross-branch information sharing — cherry-pick sparingly

If a diagnostic script written on one branch is needed urgently on another:

```bash
git checkout <target-branch>
git cherry-pick <source-commit-hash>
```

Postgres won't double-apply when the source eventually merges and the target rebases — git detects the already-applied change. But prefer waiting for the source to merge to main when timing allows; cherry-picks fragment review surface.

## Cross-branch coordination rules

| Rule | Why |
|---|---|
| **Scope each branch to its own files.** | The branches above are designed to minimize file overlap. The natural exception is Phase 6a editing existing `stg_<source>_recalls.sql` while v1.5 builds a new `stg_nhtsa_recalls_current.sql` on top — sequence handles this (6a first). |
| **Findings branches stay days, not weeks.** | Long-lived docs branches accumulate divergence and become merge headaches. If a findings cycle is taking >1 week, split it. |
| **Feature branches stay focused.** | One branch = one Phase 6 layer OR one v1.5 layer. Don't bundle "Phase 6c + a fix to 6a" — make the 6a fix on a separate small branch first. |
| **Rebase daily.** | `git rebase origin/main` first thing each session. Cheaper than one big-bang rebase at PR time. |
| **Don't `--force` to main.** | Only force-push to your own feature/docs branches (and only with `--force-with-lease` to avoid clobbering remote work). |

## Conflict resolution playbook

| File type | Typical resolution |
|---|---|
| `documentation/<source>/*.md` | Different sources almost never overlap. If two branches added to the same source doc, accept both additions in section order. |
| `documentation/decisions/*.md` (ADRs) | If two branches amended the same ADR, prefer the one already merged to main; re-apply the second branch's amendment after. ADR amendments should be atomic anyway. |
| `TODO.md` | Usually trivial. Conflicts are typically two branches closing or adding bullets. Accept both, deduplicate any genuine duplicates. |
| `dbt/models/silver/*.sql` | These are the highest-risk files. If both branches touched the same model, hand-merge field-by-field; run `dbt build` after to verify. Most likely conflict point: a source-specific CTE added in two places. |
| `scripts/sql/...` (new files) | New files in separate branches don't conflict; rebase finds no overlap. |
| `pyproject.toml` | Version bumps usually conflict — accept the higher version. |

## Per-merge conflict notes

> Removed 2026-06-01. A predicted-conflict table for one *specific* pending merge is transient — it belongs in that PR's description, not in this tracked doc (per `documentation/documentation_model.md`: in-body tactical notes don't outlive the merge). The `docs/findings-2025-05-w3 → main` table that lived here was dropped after that branch merged (PR #32). The general conflict-resolution playbook above stays — it's durable guidance, not merge-specific state.

## References

- `project_scope/phase-6-execution-plan.md` — Phase 6 execution plan with internal sequencing constraints
- `project_scope/silver_v15_migration_plan.md` — v1.5 migration plan with Layer-by-Layer gates
- `documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md` — architectural decision for v1.5 + Real_drift taxonomy (subsection added 2026-05-25)
- `project_scope/implementation_plan.md` — master phase plan
