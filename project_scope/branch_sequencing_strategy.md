# Branch sequencing and workflow strategy

- **Status:** Active 2026-05-25
- **Scope:** Coordinates three concurrent workstreams off `main` — Phase 6 execution, NHTSA silver v1.5 migration, and weekly daily-findings branches
- **Supersedes:** Branching guidance scattered across `project_scope/silver_v15_migration_plan.md` (lines 160–191), `project_scope/phase-6-execution-plan.md`, and prior weekly findings retros
- **Sunset condition:** Phase 6 complete + v1.5 migration landed (Layer 3 merged); update or delete this doc when both are true

## Workstreams in flight

| Workstream | Plan doc | Branch name pattern |
|---|---|---|
| NHTSA silver v1.5 migration | `project_scope/silver_v15_migration_plan.md` | `feature/silver-v15-scd-prototype` (Layer 2), `feature/silver-v15-migration` (Layer 3) |
| Phase 6 execution (6a foundation audit → 6b firm res → 6c history → 6d ops → 6e gold → 6f diagrams) | `project_scope/phase-6-execution-plan.md` | `feature/phase-6{a,b,c,d,e,f}-<topic>` — one branch per phase letter |
| Daily findings + diagnostic scripts | (per-branch staged-tasks doc) | `docs/findings-YYYY-Wn` — one branch per ISO week, short-lived |

## Dependency graph

```
                            main (post-USCG)
                                 │
                                 ▼
              feature/phase-6a-foundation-audit
              (hard prereq for 6b, 6c, 6e per
               phase-6-execution-plan.md:159)
                                 │
                  ┌──────────────┴──────────────┐
                  ▼                             ▼
   feature/silver-v15-scd-prototype    feature/phase-6c-history-lifecycle
       (Layer 2 — parallel models;        (recall_event_history + recall_lifecycle;
        new files, low overlap risk)       consumes from snapshot eventually)
                  │                             │
                  └──────────────┬──────────────┘
                                 ▼
                  ┌──────────────┴──────────────┐
                  ▼                             ▼
   feature/silver-v15-migration         feature/phase-6b-firm-resolution
       (Layer 3 — gated by               (RapidFuzz; needs 6a's corrected
        Phase 6c integration                staging baseline)
        decision per migration plan
        pre-condition #3)
                  │                             │
                  └──────────────┬──────────────┘
                                 ▼
              feature/phase-6d-operational-tooling
                                 │
                                 ▼
              feature/phase-6e-gold-layer
                                 │
                                 ▼
              feature/phase-6f-diagrams
                                 │
                                 ▼
                            (Phase 6 complete)

Throughout: docs/findings-YYYY-Wn branches ship daily/weekly, low conflict
risk because file scope is documentation/ and scripts/sql/ only.
```

## Recommended sequence (rationale per step)

1. **`feature/phase-6a-foundation-audit` first.** Hard prereq per `phase-6-execution-plan.md:159–164`. Corrects silver field mappings (FDA `description ← distribution_area_summary_txt` is the known case; CPSC/USDA/NHTSA likely have analogues). Building any of 6b/6c/6e on broken foundations bakes in rework.
2. **After 6a merges: `feature/silver-v15-scd-prototype` and `feature/phase-6c-history-lifecycle` in parallel.** Both add new files (Layer 2: `recall_product_v15.sql`, `recall_product_history.sql`, snapshot; 6c: `recall_event_history.sql`, `recall_lifecycle.sql`). File scopes don't intersect at code level; coordinate at gate-evaluation time on whether 6c's history model consumes the v1.5 snapshot directly (per ADR 0033's "Real_drift taxonomy" subsection forward-integration note).
3. **After both land: `feature/silver-v15-migration` (Layer 3) and `feature/phase-6b-firm-resolution` in parallel.** Layer 3 needs Phase 6c's `recall_event_history` design done (migration plan pre-condition #3). 6b also needs 6a's foundation. Neither blocks the other.
4. **Then `feature/phase-6d` → `feature/phase-6e` → `feature/phase-6f`.** Per the execution plan's sequencing constraints (6f last because diagrams freeze schema).
5. **Daily/weekly findings throughout.** `docs/findings-YYYY-Wn` branches stay tiny (days, not weeks), open PR + merge same day or next.

## Daily git workflow

### Sync ritual — start of every work session

```bash
git fetch origin
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

## The current pending merge (`docs/findings-2025-05-w3` → main)

Predicted conflict surface (USCG-integration merged into main while this branch was open):

| File | Likely conflict | Resolution |
|---|---|---|
| `TODO.md` | Possible — if USCG closed shared bullets | Accept both sides |
| `documentation/decisions/0031-silver-row-fragmentation-strategy.md` | Possible — if USCG added per-source-table row | Accept main's USCG row + this branch's amendments to other rows |
| `documentation/source_assumption_audit.md` | Possible — if USCG added assumptions | Accept both sides |
| `project_scope/implementation_plan.md` | Possible — if USCG updated Phase 5d status | Accept main's status update; reconcile any Phase 6 references |
| `pyproject.toml` | Possible — if USCG bumped version | Accept higher version (this branch should also bump per the version-bump-reminder memory) |
| `documentation/nhtsa/*.md` | Unlikely — different scope | n/a |
| `documentation/usda/*.md` | Unlikely — different scope | n/a |
| `scripts/sql/...` (new files) | None — all new | n/a |

If the rebase gets unexpectedly messy, fall back to cherry-pick: branch fresh off `main`, cherry-pick this branch's commits in order, resolve per-commit. Slower but more granular.

## References

- `project_scope/phase-6-execution-plan.md` — Phase 6 execution plan with internal sequencing constraints
- `project_scope/silver_v15_migration_plan.md` — v1.5 migration plan with Layer-by-Layer gates
- `documentation/decisions/0033-silver-row-versioning-via-scd-on-stable-anchor.md` — architectural decision for v1.5 + Real_drift taxonomy (subsection added 2026-05-25)
- `project_scope/implementation_plan.md` — master phase plan
