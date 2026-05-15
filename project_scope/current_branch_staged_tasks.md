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
