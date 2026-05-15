Operational recommendations (in priority order, mention-only per project conventions)

  1. Refactor the dbt assertion (dbt/tests/source_assumptions/assert_nhtsa_eleven_tuple_identity_stable.sql) to apply the
  per-path-value-set filter by default. The script header explicitly foreshadows this at line 47-48. The warn count would
  drop from 104 → 9 and become operationally readable. (Mid-effort, ADR-worthy refactor.)
  2. Re-baseline ADR 0031:84's silver-fragmentation trigger on the cleaner real_drift number. Current rate is 9 out of
  however many flagged 11-tuples exist across the full corpus — well under the >0.01% trigger almost certainly.
  3. Probe the Mack NULL-regression cluster. A targeted query joining the 4 Mack rows back to their raw_landing_path archives
   would confirm whether the NULLs are upstream (NHTSA emitted blank fields) or extractor (we parsed populated values as
  NULL). One-off forensic, not a new committed script unless the result is non-trivial.
  4. Update documentation/nhtsa/incremental_delta_findings.md Finding [whichever covers eleven-tuple drift] with the 95/9
  split as the new calibration baseline.

  Wrap-up — the full investigation arc

  You asked about 1 USDA load and 235 NHTSA loads. Across all 5 queries we now have:

  Pathology: USDA PHA last_modified_date frozen on content edit
  n: 3/3
  Class: derived-flag toggles + NULL/"" serialization noise
  Phase 6 implication: Use extraction_timestamp as edit signal — confirmed
  ────────────────────────────────────────
  Pathology: USDA bilingual EN/ES non-atomic
  n: 105/789 (13.31%)
  Class: EN amended retroactively, ES frozen at original publication
  Phase 6 implication: Lifecycle keyed per (recall_id, langcode) — confirmed
  ────────────────────────────────────────
  Pathology: NHTSA 235 inserts today
  n: 23 net_new + 212 amendments (122 MB, 31 KTM, 59 other)
  Class: KTM lifecycle transition (interim→final remedy) is the highest-value signal
  Phase 6 implication: recall_event_history should detect corrective_action prose transitions
  ────────────────────────────────────────
  Pathology: NHTSA source_recall_id renumbers on amendment
  n: 212/212
  Class: Hash-excluded per ADR 0030 — empirically validated
  Phase 6 implication: 11-tuple is the stable identity
  ────────────────────────────────────────
  Pathology: NHTSA 11-tuple drift
  n: 9/104 actionable
  Class: mostly mfr_comp_ptno false positives (95); 5 date-revisions + 4 NULL-regressions
  Phase 6 implication: Refactor assertion to drop the 95; investigate Mack NULL cluster



  You'll want to validate before the next dbt build:
  - dbt parse to confirm the staging model still compiles after the trim() changes
  - The dbt singular test at dbt/tests/source_assumptions/assert_usda_field_last_modified_date_advances_on_edit.sql will fire
   on the 1235 silent edits with severity=warn — expected behavior, but worth knowing it'll be noisy until Phase 6 lands the
  whitespace-normalization filter

  1. Push feature/full-silver-gold to remote if it's not already
  2. Open the PR to main with the title something like Phase 6 prep: findings, diagnostics, silver hygiene (bb4bc5e..245d073)
  3. After merge: git checkout main && git pull && git branch -D feature/full-silver-gold && git push origin 
  :feature/full-silver-gold
  4. Then branch feature/uscg-bronze off main and start USCG work
  5. Update ~/.claude/plans/phase-6-execution-plan.md if it has stale "4-source" references — that planning doc is now
  slightly behind the new sequence

  The "phase" lives in implementation_plan.md and ~/.claude/plans/phase-6-execution-plan.md as the planning artifact. The
  code lands as a sequence of focused branches, each named for what it actually contains:

  Likely branch: feature/uscg-bronze
  Scope: The pre-Phase-6 work we just talked about
  Rough order: 1
  ────────────────────────────────────────
  Likely branch: feature/scd-strategy-adr
  Scope: File the SCD-2 ADR per today's Phase 6 deliverable; design-only, small
  Rough order: 2
  ────────────────────────────────────────
  Likely branch: feature/silver-multi-source-staging
  Scope: Extend existing CPSC/FDA staging+silver to UNION ALL USDA + NHTSA + USCG; update accepted_values test for 5-source
  Rough order: 3
  ────────────────────────────────────────
  Likely branch: feature/recall-event-history
  Scope: The ADR 0022 LAG()-based history model + the per-field whitespace-normalization we filed today
  Rough order: 4
  ────────────────────────────────────────
  Likely branch: feature/recall-lifecycle
  Scope: ADR 0026 silver-side: first_seen_at, last_seen_at, is_currently_active, etc. Bundles with extraction_run_identities
    Alembic migration since they're the same ADR
  Rough order: 5
  ────────────────────────────────────────
  Likely branch: feature/firm-resolution
  Scope: FDA firmfeinum anchor + RapidFuzz cross-source matching
  Rough order: 6
  ────────────────────────────────────────
  Likely branch: feature/gold-aggregates
  Scope: Dashboards + denormalized search index
  Rough order: 7
  ────────────────────────────────────────
  Likely branch: feature/silver-gold-erd
  Scope: The column-level ERD documentation deliverable
  Rough order: 8 (or interleaved)

  Each is one PR. Each PR title matches the branch name. Each merge deletes the branch. git log reads as a sequence of Phase
  6 deliverables, not as "phase 6 mega-merge".

  Names follow the pattern feature/<what-it-builds> — describes the artifact (model, ADR, layer), not the project phase. Good
   branch names answer "what's in this diff?" before you open it.

Recommendation

  A short-lived docs/findings-<date> branch off main, opened and merged within the investigation cycle.

  Branch name pattern: docs/findings-2026-05-15 (or weekly: docs/findings-2026-05-w20)
  Lifetime: Hours to a day or two
  What it contains: New diagnostic SQL scripts, findings doc updates, small ADR amendments, small invariant tests
    (accepted_values), silver hygiene fixes (trim()) — i.e., today's exact diff

  Today's branch effectively WAS this kind of branch, just mis-named as feature/full-silver-gold. Going forward, name them
  docs/-prefixed so the scope is obvious.

  When an exception is warranted

  Situation: Finding requires a schema migration, extractor change, or hash-exclude rule (today's ADR 0032 would qualify)
  Branch instead: fix/<specific-thing> — e.g., fix/usda-establishment-hash-exclude-latest-mpi-active-date
  ────────────────────────────────────────
  Situation: Finding directly relates to source you're actively building (e.g., observing USCG bronze data while on
    feature/uscg-bronze)
  Branch instead: Inline on that branch — it's a within-scope discovery
  ────────────────────────────────────────
  Situation: Finding suggests a Phase 6 silver model design change
  Branch instead: Inline on the relevant Phase 6 sub-branch when it exists; otherwise on docs/findings-<date> for now

  Why not just put findings on feature/uscg-bronze (or whatever's active)?

  Because findings about NHTSA or USDA aren't USCG-bronze work. Mixing them blurs the PR scope and makes the diff hard to
  review. The docs/findings-<date> pattern keeps each PR a coherent unit.

  Practical workflow

  When a daily extract surfaces something interesting:
  1. git checkout main && git pull
  2. git checkout -b docs/findings-2026-05-22 (or whatever the date is)
  3. Investigate, write scripts, update findings docs
  4. Commit + push + PR + merge same/next day
  5. git checkout feature/uscg-bronze && git rebase main to pick up the findings
  
  Findings branches stay tiny and ship fast; feature work branches stay focused and don't drift.

  When findings could merge into the current work branch instead

  If you're 90% done with feature/uscg-bronze and a small finding comes up that touches USCG specifically, just commit it on
  that branch — branch-switching ceremony isn't worth it. The rule of thumb: does the finding's diff make sense in the work 
  branch's PR description? If yes, inline. If no, separate branch.