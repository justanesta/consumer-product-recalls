# Documentation Operating Model

- **Status:** Active (adopted 2026-06-01)
- **Purpose:** the *process* doc — how this project records future work, decisions, and findings so they stay coherent as the project grows. (Reference docs like `architecture.md` describe the *system*; this describes the *paperwork*.)
- **Audience:** future-me. Read this before creating any new planning/decision/findings document.

---

## The one rule

> **Every fact has exactly one home. Other docs *point* at that home; they never restate it.**

Drift happens when the same fact is written in two places and one copy goes stale. Single-homing makes coherence the path of least resistance instead of a discipline you have to remember. Almost every documentation conflict this project has accumulated traces back to a violated single-home rule (a shipped feature still "planned" in three docs; an ADR number guessed in a plan and gone stale; branch sequencing copied between two docs).

---

## The six document types

Six types, each with one responsibility. The mnemonic:

> **Why → ADR. What-we'll-do → plan. What-we-learned → findings. When-relative-to-other-branches → branch sequencing. What-we're-building → vision. Small loose ends → TODO.**

| # | Type | Single responsibility | Belongs in it | Explicitly NOT | Lifecycle | Home |
|---|---|---|---|---|---|---|
| 1 | **ADR** | one architectural decision + its rejected alternatives | Context / Decision / Consequences; inline dated amendments | task lists, sprint sequencing, daily run logs, migration-tracking tables, operator runbooks | `Proposed → Accepted → (Amended inline \| Superseded by 00MM)`; immutable once Accepted | `documentation/decisions/00NN-kebab-title.md`, indexed in `decisions/README.md` |
| 2 | **Vision** | durable "what we're building and why" | goals, end-consumers, hard constraints (cost, Python-first, free-tier) | tooling picks, phase status, anything time-bound | `Active (frozen)` — never updated to reflect progress | `project_scope/project_vision_and_constraints.md` |
| 3 | **Master implementation plan** | the *thin* sequencing authority + the index pointing at every ADR / plan / findings doc | phase list with status; **one line per item** pointing at the doc that owns the detail | the detail itself (it is a table of contents, not a container) | `Active`; sunset at v1 | `project_scope/implementation_plan.md` |
| 4 | **Phase / feature plan** | how we execute one bounded chunk | steps, file touch-list, migrations, checklist, "done" markers, in-chunk sequencing | design rationale that should be an ADR (link out); cross-branch order (→ type 5) | `Design → Active → Complete → Archived` | `project_scope/phase-NN[x]-<slug>.md` or `project_scope/<feature>-plan.md` |
| 5 | **Branch sequencing** | the cross-workstream dependency graph (which branch waits on which) | the graph + ordering constraints **only** | git how-to (→ `development.md`); per-merge tactical notes; sequencing already stated in a phase plan | `Active → Archived` at its sunset condition (most disposable type) | `project_scope/branch_sequencing_strategy.md` |
| 6 | **Findings** | what we empirically learned about a source/behavior — *describes the world, never prescribes work* | observations, statistics, dated probe results, as-built records | future work / task lists (note an open question and point at the plan that owns the follow-up) | `Active`; append-only (new dated sections) | `documentation/<source>/<topic>_findings.md` / `_observations.md`; `documentation/audit/` for cross-source |

**The two biggest leak patterns to watch:** (a) ADRs accumulating task-lists / migration logs / Go-Stop tables — those are *plan* content; (b) plan docs *guessing* ADR numbers instead of letting `decisions/README.md` be the registry.

---

## Q: TODO.md, or a per-branch task file?

**Keep one `TODO.md` at repo root for the whole project. Do not create a `current_branch_staging_tasks.md`** (a tracked, branch-scoped task file is a drift magnet — after merge it lurks stale or makes merge noise). Branch-scoped granular tasks live in the **draft-PR body checklist** (`gh pr create --draft`) — branch-scoped by construction, gone when the branch merges.

`TODO.md` = the project-wide backlog of loose ends too small to warrant a plan or ADR. Its closed items (with `Done YYYY-MM-DD (PR #N)` notes) double as a lightweight changelog.

**Graduation rules (these prevent "shipped feature still marked Planned in 3 places"):**

| Direction | Trigger | Action |
|---|---|---|
| idea → TODO | a loose end that won't be done this branch, no design needed | one-line `- [ ]`, no detail |
| TODO → PR checklist | you pick it up on a branch | copy to the draft-PR body; leave the TODO line as the audit trail |
| TODO → plan | it grows a design / multi-step shape | replace the TODO line's body with a **pointer** to the new plan; keep the checkbox |
| TODO → ADR | it's a "why X over the obvious Y" | file the ADR; TODO line becomes `→ ADR 00NN` |
| anything → done | work merges | flip `[x]`, append `Done YYYY-MM-DD (PR #N, commit)` — **once, at merge** |

A granular item is "tracked" in at most one of {TODO line, PR checklist} at a time (plus an optional pointer). It is never simultaneously a TODO line **and** re-described in `implementation_plan.md` **and** in its own plan doc.

---

## Decision tree — "I have something to write down"

```
Ask in order:
1. "Why we chose X over the obvious Y?"            → ADR (reserve the number in decisions/README.md)
2. Empirical observation about how a source works? → findings (no future-work; point at a plan for that)
3. Multi-step chunk needing a checklist?          → phase/feature plan (+ 1-line pointer from the master plan)
4. "Branch B must wait for branch A"?             → branch_sequencing_strategy.md (anchors, never line numbers)
5. Small loose end, no design?                    → TODO.md one-liner (+ draft-PR checklist if doing it now)
6. Durable "what we're building / our constraints"? → vision doc (add a pointer, never a status section)

If a fact already has a home → DON'T rewrite it; add a POINTER to the owner.
About to put a task-list in an ADR, or future-work/stats in a findings doc? → STOP, wrong type.
```

---

## States & transitions for plan docs (types 3–5)

Every plan doc carries a **`Status:` line as its first content line**. The only events that change it:

```
Design  ──work starts on a branch──▶  Active      "Active — in progress on feature/X"
Active  ──PR merges──────────────────▶ Complete    "Complete — PR #N (commit), YYYY-MM-DD"
Complete ─successor phase begins─────▶ Archived     move file to project_scope/archive/
Active  ──decision/plan changed──────▶ Superseded   "Superseded by <doc>"  (move to archive/)
```

Rules that kill stale "in progress" tables:

- **PR-merge is the single status-flip event.** A plan is `Active` until its PR merges, then immediately `Complete`. No plan is left saying "in progress" after merge — the merge *is* the trigger to flip it.
- **In-body progress tables are banned in long-lived docs.** Daily migration-tracking rows, Go/Stop logs, "current pending merge" notes are *transient* — they live in the PR or get deleted, never in a tracked plan that outlives them.
- **One `Status:` line at the top**, not status scattered per section. A reader checks one line.
- **Archived ≠ deleted.** Move to `project_scope/archive/` (mirrors the ADR-immutability ethic; keeps portfolio provenance). The file stops being a live surface a reader can be misled by.
- **The master plan reflects status by pointer**, so when a phase plan flips to `Complete`, the master plan's one-liner is the only other place to touch — one line, not a re-described block.

---

## Directory & naming layout

```
README.md                                  → links to documentation/documentation_model.md
TODO.md                                    → project-wide loose ends only
documentation/
  documentation_model.md                   → this file (the process)
  decisions/
    README.md                              → ADR registry: the SOLE authority on "next free number"
    00NN-kebab-title.md                    → ADRs (immutable; Proposed→Accepted→Superseded)
  <source>/                                → findings: *_findings.md / *_observations.md
  audit/                                   → cross-source findings / audits
  architecture.md | operations.md | data_schemas.md | development.md   → reference docs (describe the system)
project_scope/
  project_vision_and_constraints.md        → vision (frozen)
  implementation_plan.md                   → master sequencing index (thin)
  branch_sequencing_strategy.md            → cross-branch dependency graph (ephemeral)
  phase-NN[x]-<slug>.md | <feature>-plan.md → active phase/feature plans (Status line required)
  archive/                                 → completed / superseded plans land here
```

ADR numbers are reserved **in `decisions/README.md`, not in plan docs.** Reference other docs by **section anchor or doc-level pointer, never by line number** (line numbers rot on the first edit).

---

## "Keep it honest" — the PR-time checklist

Five checks at every PR — the cheapest possible drift defense:

1. Did I write a new fact? It has **one** owner doc; everyone else points.
2. Did a plan/phase reach merge? Flip its `Status:` to `Complete (PR #N)`; if a successor phase is live, move it to `archive/`.
3. Did I reference an ADR number? Confirm it against `decisions/README.md`; reserve there if new.
4. Did I put a task-list in an ADR, or future-work/stats in a findings doc? Move it to the right type.
5. Did I cite another doc by line number? Change it to a section anchor or doc-level pointer.

---

## Related

- `documentation/decisions/README.md` — the ADR registry and "how to write a new ADR" (this doc governs everything *other* than ADRs).
- `documentation/de-project-decision-guide.md` — a portfolio *teaching* artifact (generic DE decision points). Different purpose; this doc is the project's own process, not a teaching narrative.
