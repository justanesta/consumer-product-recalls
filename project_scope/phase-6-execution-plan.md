# Phase 6 Master Plan — Foundation Audit + Existing Work Streams

## Context

Phase 6 of `project_scope/implementation_plan.md` (lines 631–656) was originally scoped as "unified data model across all five sources" with deliverables spanning firm resolution, history modeling, gold materialization, operational tooling, and ERDs. Since it was scoped:

1. **Several Phase 6 items already shipped** as Phase 5 prerequisites — FDA firm role reconciliation (2026-05-09), ADR 0012 source-config loader Wave 2 (2026-05-10), USDA ETag enablement (2026-05-09), silver models for 4 sources (Phases 5a–5c).
2. **USCG is indefinitely deferred** (2026-05-09 website outage; memory note); Phase 6 is now a 4-source effort, not 5.
3. **Three new questions have surfaced** (`prompts/phase_6_deliverable_plan.md`) that the user wants to address before building on top of silver:
   - Is the firm model (`firm` / `firm_establishment_attributes` / `recall_event_firm`) the most logical representation, especially for USDA's split-API integration?
   - Are silver field mappings correct? The FDA `description` column is mapped to `distribution_area_summary_txt` (geographic distribution) instead of `product_description_txt` — a confirmed error. Other sources likely have similar problems.
   - Existing diagrams (`pipeline-architecture.drawio`, `orchestration-schedule.drawio`) don't reflect current architecture (e.g., FDA deep rescan), and no silver/gold ERD exists yet (TODO.md item 5).

The user's three streams are not a bolt-on — they reshape Phase 6 into a **foundation audit first** (Phase 6a), then existing Phase 6 work proceeds on a corrected silver layer, with diagrams as the closing deliverable. Building firm resolution (RapidFuzz), history (ADR 0022), and gold on top of broken silver mappings bakes in technical debt; this plan front-loads the audit to prevent that.

## Phase 6 (Re-organized)

### Items already complete — remove from Phase 6 scope

| Original Phase 6 item | Status | Evidence |
|---|---|---|
| Silver models for CPSC/FDA/USDA/NHTSA | ✓ Done | Phases 5a–5c (git: `8738c75`, `dfca522`) |
| FDA firm role reconciliation | ✓ Done | `silver_design_notes.md:62` (2026-05-09) |
| ADR 0012 source-config loader | ✓ Done | Wave 2 shipped 2026-05-10 (`dfca522`) |
| USDA ETag enablement | ✓ Done | `etag_viability.sql` per `b3ac952` |
| USCG silver model (5th source) | ✗ Cut | USCG indefinitely deferred per memory |

### Phase 6a — Foundation Audit (user Streams 1 + 2, merged)

**Why merged:** Firm fields are a subset of the full field-mapping audit. Treating them as one workstream avoids reading the same staging/silver SQL twice and keeps the firm architectural review grounded in concrete field evidence.

**Deliverables:**
- `documentation/cpsc/field_mapping.md` — per-source mapping reference: raw API field → bronze → staging → silver, with audit verdict (correct / fix-now / missing / drop) for each.
- `documentation/fda/field_mapping.md`
- `documentation/usda/recalls_field_mapping.md`
- `documentation/usda/establishments_field_mapping.md`
- `documentation/nhtsa/field_mapping.md`
- `documentation/silver_design_notes.md` — corrected and expanded to cover all 4 sources (currently only CPSC/FDA per `silver_design_notes.md:6–8`).
- SQL changes to `dbt/models/staging/*.sql` and `dbt/models/silver/*.sql` implementing audit fixes (fix-immediately policy per user decision).
- New ADR if firm model needs structural change (TBD based on findings).

**Per-source audit method** (apply to CPSC, FDA, USDA recalls, USDA establishments, NHTSA):

1. **API dictionary read** — open the source's authoritative reference and produce a definitive field list with semantics:
   - CPSC: `documentation/cpsc/cpsc_recalls_retrieval_web_services_programmers_guide_v1_4.pdf` (pages 2–3)
   - FDA: `documentation/fda/enforcement_report_api_definitions.pdf`
   - USDA recalls: `documentation/usda/usda_fsis_recall_api_documentation.pdf`
   - USDA establishments: `documentation/usda/usda_fsis_establishment_listing_api_data_documentation.pdf`
   - NHTSA: `documentation/nhtsa/RCL.txt` (canonical field reference)
2. **Raw R2 sample retrieval** — pull 3–5 raw payloads from R2 for each source:
   - Query `<source>_bronze` for representative rows (recent, varied), collect `raw_landing_path`.
   - Use `src/landing/r2.py:R2LandingClient.get_raw(key)` pattern (see `scripts/promote_error_to_cassette.py:1–22, 67–68` for the working access pattern) to retrieve and decompress.
   - User runs these queries; this plan documents the SQL and Python commands. (Per memory: user runs all code execution themselves.)
3. **Bronze schema cross-check** — read Pydantic schema and confirm every API field is captured (or document why omitted):
   - `src/schemas/cpsc.py`, `src/schemas/fda.py`, `src/schemas/usda.py`, `src/schemas/usda_establishment.py`, `src/schemas/nhtsa.py`
4. **Staging projection check** — for each staging model, confirm bronze→staging coverage is complete, and that empty-string-to-null normalization (ADR 0027) is applied to every nullable text field:
   - `dbt/models/staging/stg_cpsc_recalls.sql`, `stg_fda_recalls.sql`, `stg_usda_fsis_recalls.sql`, `stg_usda_fsis_establishments.sql`, `stg_nhtsa_recalls.sql`
5. **Silver semantic check** — for each silver column, verify the staging field chosen is the **best semantic fit** for that silver column. This is the audit's core analytical work. Known issue to triage: FDA `recall_event.description` ← `distribution_area_summary_txt` (geographic distribution) should be `product_description_txt`. Look for analogous semantic mismatches in CPSC/USDA/NHTSA. Look for staging fields that should be promoted to silver but aren't (e.g., FDA `product_description_txt` is in staging but never used).
6. **Firm-specific deep dive** (Stream 1, executed within the FDA + USDA + CPSC + NHTSA audits):
   - Each source contributes firm data differently — JSONB arrays (CPSC), scalar with FEI (FDA), free-text with separate establishment-listing API join (USDA), scalar manufacturer (NHTSA). Verify each path in `dbt/models/silver/firm.sql`, `firm_establishment_attributes.sql`, `recall_event_firm.sql` is semantically clean.
   - USDA specifically: assess whether the LEFT JOIN in `firm.sql:70–81` against `stg_usda_fsis_establishments` (HTML-entity decode, 99.27% match rate per `establishment_join_coverage.md:196`) is the right place for the join, or whether it belongs in bronze/staging. Investigate the 4 multi-establishment edge cases (`establishment_join_coverage.md:218–224`).
   - Cross-source firm modeling question: should `firm.sql`'s normalized-name key remain primary, or should `firm_fei_num` (FDA) and `establishment_number` (USDA) anchor a separate `firm_identifier` table? Evaluate against ADR 0002's RapidFuzz roadmap.
7. **Per-finding decision** — for each issue surfaced, classify and act:
   - **Inline fix** (rename, swap staging→silver source, add missing field): patch staging/silver SQL during 6a.
   - **Structural change** (e.g., firm model refactor): if scoped small, fold into 6a; if large, document and decide whether to pre-empt 6b firm resolution or defer.
   - **No action / accept** (intentional design choice): document in `silver_design_notes.md` so future readers don't re-audit.

**6a phase ordering rationale:** Doing the audit before Phase 6b (firm resolution), 6c (history/lifecycle), and 6e (gold) avoids two failure modes:
- Building RapidFuzz on top of a wrong firm grain forces a retro-fit.
- Designing gold aggregates / search index against semantically wrong silver columns (e.g., a `description` field that's actually a distribution area) produces wrong dashboards.

### Phase 6b — Firm Entity Resolution

Pending Phase 6 items, executed **after 6a settles the firm model**:

- Add `rapidfuzz` to `pyproject.toml` (per ADR 0002, `implementation_plan.md:599`).
- Implement cross-source firm matching with FDA `firm_fei_num` as anchor (per ADR 0002).
- Resolve AC DELCO / ACDELCO drift class (currently produces 2 rows per `firm.sql:21–22`).
- Resolve 4 multi-establishment USDA edge cases (per `establishment_join_coverage.md:218–224`).
- dbt tests for cross-source firm rollups (Honda, Tyson, etc.) per Phase 6 quality gate.

### Phase 6c — History + Lifecycle

Existing Phase 6 items, unchanged in scope but only safe to build after 6a:

- `extraction_run_identities` table + Alembic migration (per ADR 0026, USDA-only initially).
- `recall_event_history` silver model (per ADR 0022) — snapshot-based history with `LAG()` over bronze across all 4 sources; filters non-routine change types per ADR 0027.
- `recall_lifecycle` silver model — derives `first_seen_at`, `last_seen_at`, `is_currently_active`, `was_ever_retracted`, `edit_count` from `recall_event_history` (Phase 6 ordering constraint per `implementation_plan.md:487`).

### Phase 6d — Operational Tooling

Existing Phase 6 items, independent of audit findings:

- `scripts/backfill_manifest.py` — R2 payload replay per ADR 0028 Mechanism C.
- `scripts/re_ingest.py` — re-ingest CLI per ADR 0014 for schema-drift recovery.
- `dbt/tests/source_assumptions/assert_nhtsa_daily_drift_under_threshold.sql` — per-day drift-spike alert that warns when a single NHTSA extract's `real_drift` row count exceeds a threshold (initial: 50 — Section K's Pierce event was 96, so 50 catches Pierce-class events plus smaller editorial bursts). Severity `warn` for v1; promote to `error` if false-positive rate stays low across the first ~4 weeks of operation. Mechanism: filter `decompose_eleven_tuple_drift.sql`'s real_drift output to rows attributable to the most recent `extraction_runs.raw_landing_path`, compare against threshold. Identified 2026-05-25 (`docs/findings-2025-05-w3` retrospective) as a "would have caught Pierce in real time" gap. Companion to the existing per-corpus `assert_nhtsa_eleven_tuple_identity_stable.sql`; that one alerts on **cumulative** drift, this one alerts on **velocity**.

### Phase 6e — Gold Layer + Full Test Suite

Existing Phase 6 items, safe to build after audit-corrected silver is in place:

- Gold models: aggregates, denormalized views, search index targets (`dbt/models/gold/`).
- Full dbt test suite: 60–80 generic tests + 5 singular tests + freshness assertions per ADR 0015.
- Silver/gold Alembic migrations.
- dbt promotion of structural uniqueness (e.g., `recall_event_firm._silver.yml:114–116`) to enforced tests.

### Phase 6f — Diagrams (Stream 3)

Final deliverable, after all schema work is done (user-confirmed: "after Phase 6 complete"):

- **ERD** (`documentation/diagrams/silver-gold-erd.drawio` + `.svg`) — column-level ERD covering both silver and gold (closes TODO.md item 5 + `implementation_plan.md:647`).
- **DAG update** — refresh `pipeline-architecture.drawio` to reflect current 4-source architecture, FDA deep rescan (per ADR 0023, `b3ac952`), four-layer medallion (per ADR 0004), dbt staging→silver→gold flow.
- **Cadence diagram review** — confirm `orchestration-schedule.drawio` is current with ADR 0010 amendments (CPSC/USDA per 2026-05-01 amendment).
- **Walkthrough** — Claude guides setup, design choices, and SVG export workflow.

## Phase 6 Quality Gates (post-reorganization)

Re-checked against `implementation_plan.md:649–654`, all four still apply but gain a prerequisite (6a):

- [ ] **Foundation audit complete** (Phase 6a) — all field mappings reviewed, errors fixed, `silver_design_notes.md` covers 4 sources.
- [ ] All dbt tests pass (60–80 generic + 5 singular + freshness).
- [ ] Firm resolution works on cross-source examples (Honda, Tyson, etc.).
- [ ] Re-ingest command idempotent.
- [ ] History captures simulated schema-drift event in e2e test.

## Critical Files

**To audit (6a):**
- API dictionaries: `documentation/{cpsc,fda,usda,nhtsa}/*.pdf` + `documentation/nhtsa/RCL.txt`
- Bronze schemas: `src/schemas/{cpsc,fda,usda,usda_establishment,nhtsa}.py`
- Staging: `dbt/models/staging/stg_*.sql`
- Silver: `dbt/models/silver/{recall_event,recall_product,firm,firm_establishment_attributes,recall_event_firm}.sql`
- R2 access: `src/landing/r2.py` (existing `R2LandingClient.get_raw()` pattern)

**To write/update (6a):**
- `documentation/{cpsc,fda,usda,nhtsa}/field_mapping.md` (new, per source)
- `documentation/silver_design_notes.md` (correct + expand to 4 sources)
- Staging/silver SQL changes per audit findings

**To touch later (6b–6f):**
- `pyproject.toml` (add rapidfuzz in 6b)
- `dbt/models/gold/*.sql` (6e)
- `documentation/diagrams/silver-gold-erd.drawio` (new, 6f)
- `documentation/diagrams/pipeline-architecture.drawio` (update, 6f)
- `implementation_plan.md` (update Phase 6 scope to reflect this reorg + USCG cut)
- `TODO.md` (close item 5 after 6f)

## Verification

**Phase 6a (audit):**
- Manual cross-check: for each source, sample 3 raw R2 payloads, manually trace every API field through bronze → staging → silver, confirm no semantic mismatches remain.
- `dbt build` succeeds after staging/silver changes.
- Spot-check: run a query against `silver.recall_event` for FDA rows and confirm `description` now contains a product description, not a distribution area.
- `silver_design_notes.md` reviewed line-by-line against the per-source `field_mapping.md` files; no contradictions.

**Phase 6b–6e:** existing Phase 6 quality gates from `implementation_plan.md:649–654`.

**Phase 6f:** diagrams render correctly in draw.io, SVG exports embed cleanly in markdown docs.

## Sequencing Constraints

- **6a precedes 6b/6c/6e** — corrected silver is a hard prerequisite for firm resolution, history, and gold (otherwise you build on broken foundations).
- **6c internal order** — `recall_event_history` before `recall_lifecycle` (per `implementation_plan.md:487`).
- **6d is independent** — can run any time after 6a (or in parallel with 6b/6c).
- **6f is last** — diagrams freeze the schema; doing them before 6e means redrawing.

## Open / Deferred Items (not in this plan)

- TODO.md item 12: Blog post on string quoting/escaping (independent, not Phase 6).
- TODO.md item 33: 2–3 day local run-through before historical seeding (Phase 7 prerequisite).
- USCG (`Phase 5d`): indefinitely deferred per memory; reframe Phase 6 deliverable list as 4-source.
- ADR 0029 v2 observability triggers: still v1 stance; revisit per ADR 0029 upgrade triggers.

## Notes on Implementation Plan Updates

Once this plan is approved, `project_scope/implementation_plan.md` Phase 6 section (lines 631–656) should be rewritten to reflect:
1. The 6a foundation audit (currently missing).
2. The completed items moved out of Phase 6 scope.
3. USCG removed from scope language.
4. Phase ordering within 6 (6a → 6b/6c/6d → 6e → 6f).
