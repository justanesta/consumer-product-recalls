# Phase 6 Master Plan — Foundation Audit + Existing Work Streams

## Context

Phase 6 of `project_scope/implementation_plan.md` (lines 631–656) was originally scoped as "unified data model across all five sources" with deliverables spanning firm resolution, history modeling, gold materialization, operational tooling, and ERDs. Since it was scoped:

1. **Several Phase 6 items already shipped** as Phase 5 prerequisites — FDA firm role reconciliation (2026-05-09), ADR 0012 source-config loader Wave 2 (2026-05-10), USDA ETag enablement (2026-05-09), silver models for 4 sources (Phases 5a–5c).
2. **USCG website reactivated 2026-05-29** (per user — site was down 2026-05-09 to ~late May; extractors, validators, schemas, cassettes are all integrated and the 2026-05-17 Phase 5d historical seed captured the full 1,763-record corpus). Phase 6 is back to a 5-source effort.
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
| USCG silver model (5th source) | ✓ Done | Phase 5d Steps 4-6 shipped 2026-05-17 (commit `7e9edbe`) — bronze + staging + silver branches in `recall_event.sql`/`recall_product.sql`/`firm.sql`/`recall_event_firm.sql` + cassettes + short-circuit per Finding J |

### Phase 6a — Foundation Audit (user Streams 1 + 2, merged)

**Why merged:** Firm fields are a subset of the full field-mapping audit. Treating them as one workstream avoids reading the same staging/silver SQL twice and keeps the firm architectural review grounded in concrete field evidence.

**Deliverables:**
- `documentation/cpsc/field_mapping.md` — per-source mapping reference: raw API field → bronze → staging → silver, with audit verdict (correct / fix-now / missing / drop) for each.
- `documentation/fda/field_mapping.md`
- `documentation/usda/recalls_field_mapping.md`
- `documentation/usda/establishments_field_mapping.md`
- `documentation/nhtsa/field_mapping.md`
- `documentation/uscg/field_mapping.md` *(added 2026-05-29 after USCG reactivation)*
- `documentation/silver_design_notes.md` — corrected and expanded to cover all 5 sources (currently only CPSC/FDA per `silver_design_notes.md:6–8`).
- SQL changes to `dbt/models/staging/*.sql` and `dbt/models/silver/*.sql` implementing audit fixes (fix-immediately policy per user decision).
- New ADR if firm model needs structural change (TBD based on findings).

**Per-source audit method** (apply to CPSC, FDA, USDA recalls, USDA establishments, NHTSA, USCG):

1. **API dictionary read** — open the source's authoritative reference and produce a definitive field list with semantics:
   - CPSC: `documentation/cpsc/cpsc_recalls_retrieval_web_services_programmers_guide_v1_4.pdf` (pages 2–3)
   - FDA: `documentation/fda/enforcement_report_api_definitions.pdf`
   - USDA recalls: `documentation/usda/usda_fsis_recall_api_documentation.pdf`
   - USDA establishments: `documentation/usda/usda_fsis_establishment_listing_api_data_documentation.pdf`
   - NHTSA: `documentation/nhtsa/RCL.txt` (canonical field reference)
   - USCG: `documentation/uscg/scraping_observations.md` (Findings A-S; no published spec — reverse-engineered) + `documentation/uscg/USCG-2013-0133-0005_attachment_1.pdf` (HIN/MIC regulatory background) + `documentation/uscg/NRBSS-Exposure-Survey-Final-Report-20201130-v3.0.pdf` (boat-type verbal taxonomy reference)
2. **Raw R2 sample retrieval** — pull 3–5 raw payloads from R2 for each source:
   - Query `<source>_bronze` for representative rows (recent, varied), collect `raw_landing_path`.
   - Use `src/landing/r2.py:R2LandingClient.get_raw(key)` pattern (see `scripts/promote_error_to_cassette.py:1–22, 67–68` for the working access pattern) to retrieve and decompress.
   - User runs these queries; this plan documents the SQL and Python commands. (Per memory: user runs all code execution themselves.)
3. **Bronze schema cross-check** — read Pydantic schema and confirm every API field is captured (or document why omitted):
   - `src/schemas/cpsc.py`, `src/schemas/fda.py`, `src/schemas/usda.py`, `src/schemas/usda_establishment.py`, `src/schemas/nhtsa.py`, `src/schemas/uscg.py`
4. **Staging projection check** — for each staging model, confirm bronze→staging coverage is complete, and that empty-string-to-null normalization (ADR 0027) is applied to every nullable text field:
   - `dbt/models/staging/stg_cpsc_recalls.sql`, `stg_fda_recalls.sql`, `stg_usda_fsis_recalls.sql`, `stg_usda_fsis_establishments.sql`, `stg_nhtsa_recalls.sql`, `stg_uscg_recalls.sql`
5. **Silver semantic check** — for each silver column, verify the staging field chosen is the **best semantic fit** for that silver column. This is the audit's core analytical work. Known issue to triage: FDA `recall_event.description` ← `distribution_area_summary_txt` (geographic distribution) should be `product_description_txt`. Look for analogous semantic mismatches in CPSC/USDA/NHTSA/USCG. Look for staging fields that should be promoted to silver but aren't (e.g., FDA `product_description_txt` is in staging but never used).
6. **Firm-specific deep dive** (Stream 1, executed within the FDA + USDA + CPSC + NHTSA + USCG audits):
   - Each source contributes firm data differently — JSONB arrays (CPSC), scalar with FEI (FDA), free-text with separate establishment-listing API join (USDA), scalar manufacturer with no structured ID (NHTSA), scalar manufacturer with MIC structured ID (USCG — 93.2% populated per Finding S). Verify each path in `dbt/models/silver/firm.sql`, `firm_establishment_attributes.sql`, `recall_event_firm.sql` is semantically clean.
   - USDA specifically: assess whether the LEFT JOIN in `firm.sql:70–81` against `stg_usda_fsis_establishments` (HTML-entity decode, 99.27% match rate per `establishment_join_coverage.md:196`) is the right place for the join, or whether it belongs in bronze/staging. Investigate the 4 multi-establishment edge cases (`establishment_join_coverage.md:218–224`).
   - Cross-source firm modeling question: should `firm.sql`'s normalized-name key remain primary, or should `firm_fei_num` (FDA) and `establishment_number` (USDA) anchor a separate `firm_identifier` table? Evaluate against ADR 0002's RapidFuzz roadmap.
7. **Per-finding decision** — for each issue surfaced, classify and act:
   - **Inline fix** (rename, swap staging→silver source, add missing field): patch staging/silver SQL during 6a.
   - **Structural change** (e.g., firm model refactor): if scoped small, fold into 6a; if large, document and decide whether to pre-empt 6b firm resolution or defer.
   - **No action / accept** (intentional design choice): document in `silver_design_notes.md` so future readers don't re-audit.

**6a phase ordering rationale:** Doing the audit before Phase 6b (firm resolution), 6c (history/lifecycle), and 6e (gold) avoids two failure modes:
- Building RapidFuzz on top of a wrong firm grain forces a retro-fit.
- Designing gold aggregates / search index against semantically wrong silver columns (e.g., a `description` field that's actually a distribution area) produces wrong dashboards.

### Phase 6a.5 — Historical Backfill (CPSC + NHTSA + FDA)

**Why insert between 6a and 6b.** Phase 6b's load-bearing work (RapidFuzz fuzzy matching thresholds, CPSC suffix-strip regex, USDA disambiguation signal calibration, NHTSA `mfgname` normalization) is sensitive to the long-tail shape of the data. The 2026-05-29 SQL run on ~1,400 CPSC records surfaced 3-4 data-quality outliers ("United Stateso", "R", narrative-as-RemedyOption, NULL Description); the same ratios on full historical corpora plausibly surface qualitatively new patterns (alternate country-name spellings, archaic enum values, schema-drift artifacts from older records). Tuning normalization rules and fuzzy-match thresholds against the full corpus once is cheaper than tuning twice — once on dev-bronze, then again on production-bronze. Aligns with the `feedback_full_corpus_validation` principle of not committing to architecture on a dev-bronze slice.

**Scope — explicit:**

- **In scope:** One-time historical seed for CPSC, NHTSA (both PRE_2010 + POST_2010 archives), and FDA. Replay against full available history.
- **Out of scope:**
  - Production cron readiness (Phase 7).
  - TODO.md #33's 2-3 day local production-simulation (Phase 7).
  - ETag tuning, per-environment YAML overlays (Phase 7).
  - Periodic re-seeding (one-shot now; recurring = Phase 7).
  - USDA — already returns the full ~2,000-record corpus on every fetch; no backfill needed.
  - USCG — already at full corpus. The Phase 5d 2026-05-17 historical seed captured the full 1,763-record corpus per Finding J's "Records Found: 01763" — no further backfill needed. Daily incremental + Finding J short-circuit per `_should_short_circuit` handles ongoing freshness.

**Sources + treatment:**

| Source | Action | Acquisition | Storage signal | Risk class |
|---|---|---|---|---|
| **CPSC** | `recalls deep-rescan cpsc --lookback-days 7700 --change-type=historical_seed` | Fresh API extraction (no auth, no Akamai) | ~30 MB | Low |
| **NHTSA** | `recalls deep-rescan nhtsa --change-type=historical_seed` | Fresh re-download of both PRE_2010 + POST_2010 archives via `NhtsaDeepRescanLoader` (`src/extractors/nhtsa.py:565`). Config's `historical_seed_urls` already lists PRE_2010 (`config/sources/nhtsa.yaml:16`); no code change needed. | ~400-450 MB | Low operationally; dominant on storage |
| **FDA** | `recalls deep-rescan fda --change-type=historical_seed` w/ multi-year window covering everything iRES offers (per user 2026-05-29: "Pull in everything") | Fresh API extraction | ~1.5-3 GB (revised up from ~500 MB - 1 GB after the 2026-05-29 capture-expansion probe — `codeinformation` max length empirically 205,424 chars/record; even a few % of records near that ceiling drives the historical seed materially larger than the original estimate. Tighten after FDA depth probe.) | Medium (Akamai) |

**Pre-flight (one-time, completes before any seed runs):**

1. **FDA depth probe** — Bruno request or one-shot API call to find oldest available `eventlmd`. Determines the FDA deep-rescan date window. Per user direction, the target is "everything iRES will give us" — no artificial cap.
2. **R2 inventory check** — `aws s3 ls` against the NHTSA + CPSC + FDA R2 prefixes to size existing payloads. Confirms what's already there. NHTSA POST_2010 confirmed present from prior daily incremental runs; PRE_2010 + CPSC/FDA depth TBD.
3. **Storage estimate finalized** — tighten the per-source estimates above with the depth probe + R2 inventory data.
4. **Neon tier upgrade** — pick a tier with 6 months of growth headroom past the combined ~2-3.5 GB estimate (CPSC ~30 MB + NHTSA ~400-450 MB + FDA ~1.5-3 GB; FDA estimate revised up per 2026-05-29 capture-expansion probe — see FDA row above). Single upgrade event, not per-source. User-executed.
5. **Akamai readiness for FDA** — `data/user_agents.json` rotation current; plan to run the FDA seed off-peak hours.
6. **NHTSA bronze assertion handling — reactive (no pre-flight action)** — per user 2026-05-29. The existing `dbt/tests/source_assumptions/assert_nhtsa_eleven_tuple_identity_stable.sql` is `severity=warn` (cannot block dbt build) and runs at dbt-test time, not bronze-insert time. POST_2010 re-download produces identical content (content-hash dedup → no new bronze rows → no new assertion signal); PRE_2010 brings brand-new campnos with no group overlap. Reactive plan: run the seed, then run dbt, then inspect warn-count delta. Investigate any new drift groups — likely real signal worth folding into the audit, not noise.

**Execution — CPSC → NHTSA → FDA (low → medium-storage → high-operational risk):**

| Step | Command | Notes |
|---|---|---|
| 1. CPSC | `recalls deep-rescan cpsc --change-type=historical_seed` | Smallest, validates 6a.5 mechanics end-to-end. Uses `CpscDeepRescanLoader` (added 2026-05-31): fixed `LastPublishDateStart=1970-01-01` floor, **bypasses the 5,000-row `_MAX_INCREMENTAL_RECORDS` guard** that would otherwise abort the full ~9,828-record pull, and does not advance the watermark (mirrors FDA/USDA). The routine `extract cpsc` path keeps the guard. Verified empirically 2026-05-31: corpus = 9,828 (RecallDate back to 1973-06-08; earliest LastPublishDate 1975-04-07). Archive-migration race handled by content-hash dedup (`documentation/cpsc/array_stability_findings.md`). |
| 2. NHTSA | `recalls deep-rescan nhtsa --change-type=historical_seed` | Pulls both PRE_2010 + POST_2010 in one run via `NhtsaDeepRescanLoader`. This is the storage-dominant step; verify Neon tier holds before moving to FDA. |
| 3. FDA | `recalls deep-rescan fda --start-date 1990-01-01 --end-date $(date +%F) --change-type=historical_seed` | **Apply migration `0019` first** (`alembic upgrade head`) — the Phase 6a.5 capture expansion added the 11 §7a SHIP fields to bronze + `_DISPLAY_COLUMNS` so this one-time pull captures everything silver + 6b need (R2 replay can't recover un-requested columns). `codeinformation` drops the page size 5000→2500, so the pull is ~2× the request count. No count guard on `FdaDeepRescanLoader`, so the over-broad `1990-01-01` floor is safe (captures all; iRES realistically starts ~2004). Akamai posture: rate-limited, off-peak, pause-and-resume if HTTP 204 / `text/html` throttle hit. Saved for last so we've validated Neon tier + 6a.5 mechanics first. |

After each step, before moving to the next:
- Verify expected bronze row count landed (within ~10% of projection)
- Quarantine rate < 5%, or investigate the dominant quarantine pattern
- Existing dbt assertions still green-or-known-warn (warn count delta documented per step)

**Post-seed re-validation (drives audit doc updates):**

- Re-run `python scripts/cpsc/audit/inspect_landed_payloads.py --date <multi-year date list>` against expanded corpus
- Re-run `psql -f scripts/sql/cpsc/bronze/inspect_array_field_population.sql` (now at corpus scale)
- Build FDA equivalent: `python scripts/fda/audit/inspect_landed_payloads.py --date <multi-year list>` (script exists from Phase 6a)
- NHTSA: the existing `scripts/nhtsa/tsv_analysis/` toolkit + `assert_nhtsa_eleven_tuple_identity_stable.sql` + `decompose_eleven_tuple_drift.sql` already cover most of what an audit script would do; supplement only if a corpus-scale field-rate inspect doesn't already exist
- Fold §9 updates into CPSC + FDA + NHTSA audit docs. NHTSA audit doc is created as part of Phase 6a continuation when we get to that source.

**Quality gates:**

- [ ] FDA depth probe complete; date window documented in this plan
- [ ] R2 inventory sized; storage estimate within 20% of pre-flight projection
- [ ] Neon tier upgraded; new monthly cost recorded
- [ ] CPSC seed: bronze row count >= 9,000 (or documented reason for shortfall — e.g., archive migration still incomplete at CPSC's end)
- [ ] NHTSA seed: bronze row count = (existing) + (~380-440k PRE_2010 + POST_2010 combined, modulo content-hash dedup of existing POST_2010)
- [ ] NHTSA bronze 11-tuple assertion: warn-count delta documented; any new drift groups triaged
- [ ] FDA seed: bronze row count > 5× current; no Akamai 204 blocks during seed
- [ ] Combined quarantine rate < 5% (or each pattern's quarantine class triaged)
- [ ] CPSC + FDA + NHTSA field audit §9 re-run; deltas folded into audit docs
- [ ] No Phase 6a architectural surprises that require restart (Bug 1/2/3 framings + Option B still hold at corpus scale)

**Risks + mitigations:**

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Akamai blocks FDA mid-seed | Medium | Medium — partial seed, resume needed | Pre-flight UA rotation check; seed during off-peak hours; rate-limit; resume from last `started_at` if blocked |
| Storage tier surprise mid-NHTSA-load | Medium | Medium — abort, upgrade, resume | R2 inventory sizing during pre-flight; Neon tier picked with headroom |
| NHTSA bronze 11-tuple assertion fires new spurious warnings | Low (per analysis above) | Low (severity=warn, doesn't block) | Reactive triage; per user decision 2026-05-29 |
| NHTSA older-year schema drift | Medium | Low (quarantine handles) | Review quarantine class profile post-seed; tweak Pydantic optionals if pattern > 50 records |
| CPSC archive-migration race | Already proven OK | Low | Content-hash dedup; documented in `array_stability_findings.md` |
| Schema drift on old FDA records | Medium | Low (quarantine handles) | Accept quarantine; review patterns; iterate schema if a class > 50 records |
| New `LastPublishDate` semantics surprise on old CPSC records | Low | Low | Per `last_publish_date_semantics.md` we already understand the bimodal distribution |
| One-shot vs iterative recovery (too-expensive Neon surprise) | Low | High | Run CPSC first (small); validate Neon tier holds at NHTSA before kicking off FDA |

### Phase 6b — Firm Entity Resolution

Pending Phase 6 items, executed **after 6a settles the firm model**.

**Broader framing (2026-05-29):** Phase 6b is the **pre-silver text-processing stage** for everything firm-adjacent. RapidFuzz fuzzy matching is the headline algorithm, but the stage also bundles regex-based name cleaning (CPSC suffix stripping), DBA extraction (CPSC + USDA), text-embedded identifier extraction (USDA `field_product_items` establishment numbers), and other "prepare the name before anchoring" preprocessing. Per user 2026-05-29: "the name cleaning [is] bundled in the rapidfuzz stage 6b with FDA firms and other information that needs to be extracted/manipulated with rapidfuzz before settling in silver."

Pending items:

- Add `rapidfuzz` to `pyproject.toml` (per ADR 0002, `implementation_plan.md:599`).
- Implement cross-source firm matching with FDA `firm_fei_num` as anchor (per ADR 0002).
- Resolve AC DELCO / ACDELCO drift class (currently produces 2 rows per `firm.sql:21–22`).
- **CPSC firm-name normalization** — strip "of <location>" + "dba <name>" suffixes from Manufacturers/Importers/Distributors before fuzzy matching; extract DBAs as alternate-name candidates. See `documentation/cpsc/field_audit_2026_w22.md` §3 Bug 2 + §6 Option B (2026-05-29 audit). Detail in subsection below.
- **USDA recall-to-establishment disambiguation** — expanded scope per Phase 6a audit findings. See `documentation/usda/field_audit_2026_w22.md` §6 + §9 (2026-05-28 R2 validation). Detail in subsection below.
- dbt tests for cross-source firm rollups (Honda, Tyson, etc.) per Phase 6 quality gate.

#### USDA recall-to-establishment disambiguation (Phase 6a audit follow-on)

**Problem statement.** USDA recalls carry only free-text `field_establishment` (firm name) — there is no FSIS establishment number on the recall side. The current silver join in `firm.sql:75-86` matches on `upper(trim(name))` and stores all matched FSIS numbers in `firm.observed_company_ids` as a JSONB array. Per the 2026-05-28 R2 validation (`documentation/usda/field_audit_2026_w22.md` §9):

- **~14% of FSIS establishments share names** with at least one other establishment (6885 distinct names across 7970 records — same business with multiple grant numbers, or genuinely different businesses with identical legal names).
- When "Tyson Foods, Inc." appears as a recall's `field_establishment`, the LEFT JOIN fans out to multiple Tyson establishments.
- `recall_event_firm` currently has no `establishment_number` column — only `firm_id` (md5 of normalized name). For a recall of "Tyson Foods, Inc.", silver knows it was *a* Tyson establishment but not *which* one.

This blocks:

- Landing-page rendering of the specific establishment's address, MPI lifecycle dates, activities, DBAs
- FastAPI endpoint `GET /recalls/{id}/firm` returning the right `establishment_number` (per `project_scope/implementation_plan.md`'s API plan)
- Geographic aggregations like "all recalls from Texas establishments" that need per-recall location
- Approximately ~14% of USDA recall-side rows being silently ambiguous

**Empirical category breakdown of duplicate-name groups (2026-05-28, via `scripts/sql/usda_establishments/bronze/inspect_duplicate_names.sql`):**

| Category | Groups | Records | Avg/group | Description |
|---|---|---|---|---|
| `multi_grant_same_state` | 276 (60%) | 600 (39%) | 2.17 | Same name + same state, different establishment_numbers. Typically the same physical facility holding multiple FSIS grants (Meat M, Poultry P, Voluntary V) — `M1234 + P1234 + V1234` at one address |
| `multi_state` | 103 (23%) | 248 (16%) | 2.41 | Same name in different states — likely different businesses with identical legal names. State-match alone resolves most |
| `mixed` | 77 (17%) | 693 (45%) | 9.00 | Multi-location chains — same name across many states + multiple grants. Max group size 59 (Lineage Logistics, LLC). Dominant pattern by record count |

**Two architectural reframings surfaced by the empirical breakdown — adopt as Phase 6b sub-tasks alongside the signal hierarchy:**

- **Same-facility collapse for `multi_grant_same_state`.** A (name, city, state) tuple with multiple establishment_numbers (M1234 + P1234 + V1234) almost always refers to one physical facility holding multiple FSIS grant types, not multiple facilities at the same site. Collapsing the disambiguation problem from "pick one of N grant numbers" to "this facility has N grants" drops 60% of duplicate-name groups out of the signal-hierarchy work entirely. Implementation: in `firm_establishment_attributes` (or a sibling table), allow `establishment_id` to be a tuple of FSIS numbers when they share (name, city, state). Downstream consumers see one "facility" entity.
- **Cold-storage vs producer firm attribution.** The largest duplicate-name groups in the `mixed` bucket are cold-storage operators — Lineage Logistics (59), Lineage Logistics PFS (38), Americold Logistics (32 + 31). Cold-storage facilities don't typically produce recalled products — they store products from other producers. When `field_establishment` is a cold-storage operator, the firm-of-interest for landing-page rendering is the *producer*, not the storage facility. This is **not** a disambiguation problem — it's a wrong-firm-attribution problem. The `field_product_items` structured-parse workstream (Phase 6/7) becomes the producer-extraction surface: extract the producer name from product description text, then resolve the producer (not the storage facility) as the recall_event_firm.

**Disambiguation signal hierarchy (highest confidence first).**

1. **`field_product_items` text-embedded establishment number.** The FSIS establishment number frequently appears verbatim in the product description (per audit sample: `"...with establishment number P-33901..."`). When extraction yields a number matching exactly one candidate establishment's `establishment_number`, that's a *deterministic* match. Coverage TBD; expected to be a meaningful fraction of multi-match cases since FSIS labeling regulations require the establishment number to appear on product packaging.

2. **`field_states` ∩ `establishment.state`.** A recall affecting "Texas" most likely came from a candidate establishment located in Texas. When `field_states` parses to exactly one state and exactly one candidate establishment's `state` matches, confident assignment. (Watch for "Nationwide", "Midwest", and other non-state values from §1a's `field_states` finding — those are non-disambiguating.)

3. **`field_processing` ∩ `establishment.activities`.** A "Heat Treated - Not Fully Cooked - Not Shelf Stable" recall most plausibly came from a Meat Processing or Poultry Processing establishment, not a pure Slaughter or Egg Product one. When processing-to-activities intersection narrows candidates to one, confident assignment.

4. **Combined signals 2+3.** Even when state or processing alone doesn't disambiguate, the intersection often does (Texas × Meat Processing).

5. **`field_recall_date` proximity to `establishment.LatestMPIActiveDate`.** A 2026-05 recall shouldn't associate with an establishment whose MPI active date is 2022 (likely inactive at time of recall). Useful as a tiebreaker after signals 1-4.

**Principle: precision over recall (literally).** When signals don't uniquely disambiguate, store NULL `establishment_number` on `recall_event_firm` rather than guess. A wrong association (Texas Tyson recall attributed to a Georgia Tyson establishment) is *worse* than no association — landing pages render misleading addresses, geographic aggregates carry errors. The audit's 35.1% recall-side `field_establishment` NULL ceiling already accepts that ~37% of USDA recalls have no `firm.company_id`; the additional ambiguity from name-fan-out is a smaller incremental gap that downstream consumers can handle with "no specific establishment identified — see candidates: [...]".

**Implementation approach.**

1. Add `recall_event_firm.establishment_number` column (nullable). Populates only when disambiguation produces a confident single match.
2. Add `recall_event_firm.match_confidence` column with structured values: `'unambiguous'` (only one candidate), `'product_items_extract'` (signal 1), `'state_match'` (signal 2), `'processing_match'` (signal 3), `'multi_signal'` (signals 2+3+5), `'ambiguous_null'` (multiple candidates, no signals resolved). Useful for downstream consumers (can filter to high-confidence only) and for retrospective quality measurement.
3. New silver model `dbt/models/silver/recall_event_establishment_resolution.sql` that implements the signal hierarchy. Runs before `recall_event_firm.sql` so the bridge can use the resolved `establishment_number`.
4. dbt test: verify `match_confidence='ambiguous_null'` rate is < 10% of multi-match cases (signal hierarchy resolves at least 90% of fan-outs).
5. Operational query: surface the `ambiguous_null` rate as a per-extraction-run quality KPI; investigate signal-extraction quality if it climbs.

**Signal effectiveness by category (empirically informed):**

| Category | Primary disambiguation lever | Expected resolution rate |
|---|---|---|
| `multi_grant_same_state` (60% of groups, 39% of records) | Same-facility collapse (architectural reframing) | Drops the problem from disambiguation to facility aggregation |
| `multi_state` (23% of groups, 16% of records) | Signal 2 (`field_states` ∩ `establishment.state`) + Signal 5 (Active-vs-Inactive) tiebreaker | ~80% of groups resolvable to a single Active facility |
| `mixed` (17% of groups, 45% of records) | Signal 1 (`field_product_items` extraction) is dominant; Signals 2+3 narrow but rarely resolve alone | Highly dependent on Phase 6/7 extraction quality |

The `mixed` bucket dominates by record count — most disambiguation work happens here — and is where Signal 1 (text-embedded establishment number from `field_product_items`) matters most. Phase 6b ships with whatever extraction quality is available; the `match_confidence` column lets us track improvement as the structured-parse workstream matures.

**Cross-reference to RapidFuzz work.** The name-matching step in `firm.sql:75-86` is currently `upper(trim()) =` (exact-after-normalization). The RapidFuzz work above will add fuzzy matching for cases like " Tyson Foods, Inc." vs "Tyson Foods Inc" (no period, leading whitespace). Fuzzy matching *expands* the candidate set — a 90%-similarity match may surface 5 candidates instead of 3 — which makes the disambiguation hierarchy *more important*, not less. Plan RapidFuzz and disambiguation as paired workstreams.

**Phase 6/7 dependency for Signal 1.** Text-embedded establishment number extraction from `field_product_items` is part of the Phase 6/7 structured-parse enrichment workstream (per `documentation/usda/field_audit_2026_w22.md` §7 decision #4). For Phase 6b it can be partially mocked with a regex like `r'establishment number ([MPIGV]-?\d+[A-Z]?(?:\s*\+\s*[MPIGV]-?\d+[A-Z]?)*)'` and refined as the structured-parse work matures. Phase 6b ships with whatever extraction quality is available; the `match_confidence` column lets us track improvement over time.

**Future-source generalization.** This pattern (per-recall disambiguation when name matching fans out) likely applies to NHTSA and possibly CPSC. NHTSA's `mfgname` doesn't have a structured-ID counterpart at all (no FEI analog), so the name-fan-out problem there is *worse*. NHTSA disambiguation by `maketxt` (vehicle make) + `yeartxt` is an analogous Phase 6b workstream — keep the signal-hierarchy + match_confidence pattern source-agnostic so it generalizes.

#### CPSC firm-name normalization (Phase 6a audit follow-on)

**Problem statement.** CPSC encodes firms in four parallel arrays (Manufacturers, Importers, Distributors, plus Retailers which is being lifted out of the firm dim under §6 Option B of the CPSC audit). Per `documentation/cpsc/field_audit_2026_w22.md` §3 Bug 2:

- The remaining three firm-role arrays carry deterministic suffix patterns that fragment the same firm into multiple rows in the current `firm.sql` `upper(trim())` keying:
  - **Geography appended** — `"ZOLIQUEX, of China"`, `"Apex Gaming PCs Inc., of Houston, Texas"`, `"Aria Child Inc. of Dedham, Mass."`
  - **DBA parentheticals** — `"Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China"`, `"Jiangxi Runfuyuan Biotechnology Co., Ltd dba Agiiman, of China"`
- `CompanyID` is empirically `""` across all 4 firm-role arrays in the cassette (§3 Bug 3). CPSC contributes **no structured identifier** to cross-source firm rollup — name is the only join lever.

**Why this is preprocessing, not fuzzy matching.** A purely fuzzy step (`rapidfuzz.fuzz.ratio("ZOLIQUEX" , "ZOLIQUEX, of China")` ≈ 60%) would either fail to collapse these (high threshold) or over-merge unrelated firms (low threshold). The fix is deterministic: strip the suffix patterns *before* the keying / fuzzy step. The suffix forms are bounded — "of <location>", "dba <name>", trailing entity tokens — and capture the dominant fragmentation modes the audit surfaced.

**Approach: regex strip + DBA extract as the pre-silver normalization stage.**

1. **Strip patterns** (applied in order on `Manufacturers/Importers/Distributors` `.Name` before normalize):
   - `r',?\s*of\s+[^,]+\.?$'` — trailing geographic clause (`, of China`, ` of Dedham, Mass.`)
   - `r',?\s*(?:dba|doing business as)\s+([^,]+?)(?:,\s*of\s+[^,]+)?$'` — DBA + optional trailing geography. Both forms observed in the R2 corpus 2026-05-29 (abbreviated `"dba"` and full-form `"doing business as"`, e.g. `"Shenzhen Maikeer Industrial Co., Ltd., doing business as MalkerDirect, of China"`). Capture group 1 = the DBA candidate.
   - Final pass: `r'\s*,\s*$'` to clean trailing comma after stripping
2. **DBA preservation.** Extracted DBA candidates feed `firm.alternate_names` (JSONB array, new column) so the surface-form variants are preserved for FastAPI search and for the RapidFuzz match-explanation surface ("matched as DBA of <legal name>").
3. **Then RapidFuzz.** After stripping, the cleaned names enter the standard cross-source fuzzy-matching step alongside FDA `firm_legal_nam` and USDA `establishment_name`. A `match_confidence` column (parallel to USDA's §6 design) records the path: `'cpsc_suffix_strip_exact'`, `'cpsc_dba_extract_exact'`, `'rapidfuzz_high'`, `'rapidfuzz_low_ambiguous_null'`, etc.

**Implementation approach.**

1. New normalization helpers — `src/silver/firm_normalization.py` (or a dbt macro `{{ cpsc_clean_firm_name(col) }}` for SQL-side execution). Unit tests against the §3 Bug 2 cassette examples.
2. CPSC staging adds two columns per firm-role array element: `normalized_name` (post-strip) and `extracted_dba` (nullable). The existing `firm.sql` CPSC branches consume `normalized_name`; `firm_event_firm.firm_id` keys on it.
3. `firm.alternate_names` JSONB column populated from extracted DBAs across sources (CPSC `extracted_dba`, USDA `dbas` array).
4. dbt test: assert no firm row in `firm.normalized_name` ends with `, of <X>` or contains ` dba ` — fails loudly if the strip regex doesn't keep up with new patterns.
5. Operational query: surface the count of firm rows that strip changed (`raw_name != normalized_name`) as a per-extraction quality KPI; investigate if the count drops sharply (regex over-broad) or climbs sharply (new pattern variant emerging).

**Cross-reference to USDA disambiguation.** Both workstreams share the `match_confidence` column on `recall_event_firm` — CPSC's confidence values describe the *normalization path* that produced the firm match; USDA's describe the *disambiguation signal* that resolved a name fan-out. Same column, complementary semantics. The pre-silver normalization step (CPSC suffix strip, USDA DBA-aware matching, FDA name normalization) is the shared substrate; per-source signal extraction sits on top of it.

**Future-source generalization.** NHTSA's `mfgname` (also no structured-ID counterpart per ADR 0031's AC DELCO / ACDELCO drift class) likely has analogous suffix patterns — vehicle-industry mfgname strings sometimes include parent-company suffixes, regional divisions, or model-year qualifiers. Reuse the strip-then-RapidFuzz pattern; add NHTSA-specific regex variants as a Phase 6b sub-task.

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

### Phase 6f — Diagrams + written-documentation sync (Stream 3)

Final deliverable, after all schema work is done (user-confirmed: "after Phase 6 complete"). By this stage the diagrams AND the top-level prose docs lag the real architecture badly — they describe a ~4-source REST/flat-file pipeline and predate: USCG reactivation (2026-05-29), the USCG manufacturer **listing** source (Phase 5d Step 7, #40), the **`uscg_manufacturer_details` detail-capture source + its SCD-2 `firm_manufacturer_attributes` dim** (`feature/uscg-manufacturers-detail-addition`), and the cross-source SCD-2 work (ADR 0035). 6f must bring ALL of it current:

- **ERD** (`documentation/diagrams/silver-gold-erd.drawio` + `.svg`) — column-level ERD covering silver + gold. Must include the USCG manufacturer surfaces: `uscg_manufacturers_bronze` + **`uscg_manufacturer_details_bronze`**, their staging models, the **SCD-2 `firm_manufacturer_attributes`** dim + its snapshot/history table, and the `mic` join into `firm` / `recall_event_firm`. (Closes TODO.md item 5 + `implementation_plan.md:647`.)
- **DAG update** — refresh `pipeline-architecture.drawio` to the CURRENT architecture: **all 8 extraction sources** (CPSC, FDA, USDA recalls, USDA establishments, NHTSA, USCG recalls, **`uscg_manufacturers`**, **`uscg_manufacturer_details`**), four-layer medallion (ADR 0004), the CPSC/FDA/USCG deep-rescan paths, and — load-bearing — the **`uscg_manufacturers` → `uscg_manufacturer_details` work-list dependency** (the detail source reads its per-MIC work-list from the listing bronze) plus the listing/detail → `firm_manufacturer_attributes` SCD-2 flow.
- **Cadence diagram review** — `orchestration-schedule.drawio` current with ADR 0010 amendments AND the USCG cadences: `uscg_manufacturers` daily (Records-Found short-circuit, ~3s steady state) + `uscg_manufacturer_details` two-tier (Tier-1 listing-delta incremental + periodic Tier-2 full-sweep deep-rescan, ~4.5h). Rationale: `project_scope/phase-5d-uscg-manufacturers-detail.md` §3.
- **Written-documentation sync (do this in 6f too).** Bring the prose docs current with everything that landed since they were written — they are materially stale (e.g. `operations.md` still lists USCG as "indefinitely deferred"; `architecture.md` says `HtmlScrapingExtractor` is "reserved for future use" and the registry has "5 entries / 3 entries" — now **8 / 6**; `data_schemas.md`'s bronze-table list omits all three USCG tables). At minimum update:
  - `documentation/architecture.md` — extractor component table (add `_html_scraping.py`, the USCG concrete subclasses, `uscg_manufacturer_detail.py`), the registry counts (8/6), the source lists in the cron-schedule + end-to-end-flow diagrams, and the medallion narrative.
  - `documentation/data_schemas.md` — bronze rows for `uscg_recalls_bronze` / `uscg_manufacturers_bronze` / **`uscg_manufacturer_details_bronze`**, the staging + `firm_manufacturer_attributes` silver rows, and glossary terms (MIC as a **temporal SCD anchor**; `Out of Business` vs `Past Company (OOB)`; HIN ⊃ MIC).
  - `documentation/operations.md` — replace the stale USCG "indefinitely deferred" row with the live 3-source USCG cadence table; add the `uscg_manufacturer_details` extract/deep-rescan runbook + the bulk-`Date Modified`-re-touch re-baseline note (`change_type='schema_rebaseline'`).
  - `documentation/silver_design_notes.md` — the `firm_manufacturer_attributes` SCD-2 mapping + the flag-as-time-sensitive recall→manufacturer join (ADR 0035).
  - `documentation/commands.md` — the `recalls extract|deep-rescan uscg_manufacturer_details` commands.
- **Walkthrough** — Claude guides setup, design choices, and SVG export workflow.

## Phase 6 Quality Gates (post-reorganization)

Re-checked against `implementation_plan.md:649–654`, all four still apply but gain prerequisites (6a, 6a.5):

- [ ] **Foundation audit complete** (Phase 6a) — all field mappings reviewed, errors fixed, `silver_design_notes.md` covers 4 sources.
- [ ] **Historical backfill complete** (Phase 6a.5) — CPSC + NHTSA + FDA seeded against full available history; audit doc §9 sections reflect corpus-scale figures; no architectural surprises requiring 6a restart.
- [ ] All dbt tests pass (60–80 generic + 5 singular + freshness).
- [ ] Firm resolution works on cross-source examples (Honda, Tyson, etc.).
- [ ] Re-ingest command idempotent.
- [ ] History captures simulated schema-drift event in e2e test.

## Critical Files

**To audit (6a):**
- API dictionaries: `documentation/{cpsc,fda,usda,nhtsa}/*.pdf` + `documentation/nhtsa/RCL.txt` + `documentation/uscg/{scraping_observations.md,USCG-2013-0133-0005_attachment_1.pdf,NRBSS-Exposure-Survey-Final-Report-20201130-v3.0.pdf}`
- Bronze schemas: `src/schemas/{cpsc,fda,usda,usda_establishment,nhtsa,uscg}.py`
- Staging: `dbt/models/staging/stg_*.sql`
- Silver: `dbt/models/silver/{recall_event,recall_product,firm,firm_establishment_attributes,recall_event_firm}.sql`
- R2 access: `src/landing/r2.py` (existing `R2LandingClient.get_raw()` pattern)

**To write/update (6a):**
- `documentation/{cpsc,fda,usda,nhtsa,uscg}/field_mapping.md` (new, per source)
- `documentation/silver_design_notes.md` (correct + expand to 5 sources)
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

- **6a precedes 6a.5** — audit findings drive the seed quality gates and post-seed §9 re-validation; running the seed before the audit would force re-doing the audit at corpus scale (the audit's purpose is to design what to validate, not to be validated against arbitrary data).
- **6a.5 precedes 6b** — Phase 6b normalization tuning (RapidFuzz thresholds, CPSC suffix-strip regex, NHTSA `mfgname` patterns) is materially better against the full corpus. Running 6b first means tuning twice (dev-bronze, then production-bronze).
- **6a.5 precedes the (a) silver-remap PR** — same dev-bronze-slice argument as the 6b constraint above. Silver semantic decisions (population-driven lift/skip choices, cross-source column-name alignment, enum-distribution shape) are materially better against full-corpus bronze. The (a) PR (on `feature/silver-field-remap`) builds `documentation/audit/cross_source_consolidation.md` informed by post-6a.5 bronze, then edits `dbt/models/silver/*.sql`. **The (a) PR is also the home for the cross-source SCD-applicability investigation** — does CPSC / FDA / USDA / USCG-recalls also warrant a stable-anchor dbt-snapshot dim, like NHTSA's silver-v1.5? It is a read-only, full-corpus profiling of each source's edit-rate + silver-identity fragmentation (NEED = fragmentation fix; BENEFIT = attribute-history capture even where the key is stable — e.g. the USDA `status_regulated_est` flip). Priors + per-source assessment live in **ADR 0033 "Cross-source implications"** and the Phase-6 cross-source SCD-2 item in `implementation_plan.md` (confirm, don't assume — only NHTSA and USCG are *measured*; CPSC ordinality-shift and FDA/USDA stability are hypotheses). The verdict folds into `cross_source_consolidation.md` + the cross-source SCD ADR; per-source SCD *builds* then follow NHTSA's silver-v1.5 **Layer 3** proof (`silver_v15_migration_plan.md` Open Q#4 "post-Layer 3"), sequenced into 6b (USCG `firm_manufacturer_attributes` via ADR 0035) or a dedicated branch (CPSC). Note: this supersedes the line-40 6a deliverable's "fix-immediately policy" framing — clear-cut Bug 1/2/3 inline swaps were intended to land during 6a, but in practice no silver SQL edits shipped on `feature/phase-6a-foundation-audit` and the full silver-remap scope was deferred to its own PR per `documentation/audit/capture_expansion_backlog.md:148`.
- **(a) silver-remap PR precedes 6b** — Phase 6b's firm-resolution work edits `firm.sql` / `recall_event_firm.sql` / `firm_establishment_attributes.sql` on top of the corrected silver layer. Doing 6b before the silver remap forces a rebase of the firm-resolution branch against renamed/restructured silver columns.
- **6a / 6a.5 / (a) silver-remap precede 6c/6e** — corrected silver is a hard prerequisite for history and gold (otherwise you build on broken foundations).
- **6c internal order** — `recall_event_history` before `recall_lifecycle` (per `implementation_plan.md:487`).
- **6d is independent** — can run any time after 6a (or in parallel with 6b/6c).
- **6f is last** — diagrams freeze the schema; doing them before 6e means redrawing.

## Open / Deferred Items (not in this plan)

- TODO.md item 12: Blog post on string quoting/escaping (independent, not Phase 6).
- TODO.md item 33: 2–3 day local run-through before historical seeding (Phase 7 prerequisite).
- ADR 0029 v2 observability triggers: still v1 stance; revisit per ADR 0029 upgrade triggers.
- **USCG manufacturer SCD-2 + time-sensitive recall join** — `firm_manufacturer_attributes` Type-2 history (dbt snapshot `strategy='check'` on the `mic` anchor) + the as-of-build-date / flag-as-time-sensitive recall→manufacturer attribution. Designed in `project_scope/phase-5d-uscg-manufacturers-detail.md` §11; **hard prerequisite:** the bronze-capture branch `feature/uscg-manufacturers-detail-addition` lands first (it provides the detail-page succession lineage the SCD-2 seed needs). Formalized by **new ADR 0035** (cross-source SCD-2; 0034 is reserved for NHTSA Layer-3). Bundle the silver model + the `recall_event_firm.match_confidence` time-sensitive flag into the **Phase 6b** firm-resolution PR to respect the `firm.sql` ↔ `recall_event_firm.sql` lockstep (the lockstep comment lives at `recall_event_firm.sql:22` / `:98`; `firm.sql` carries no explicit lockstep warning). Part of the `implementation_plan.md:707` cross-source SCD-2 item, not net-new scope.

## Notes on Implementation Plan Updates

Once this plan is approved, `project_scope/implementation_plan.md` Phase 6 section (lines 631–656) should be rewritten to reflect:
1. The 6a foundation audit (currently missing).
2. The completed items moved out of Phase 6 scope.
3. USCG reactivation (2026-05-29) — back in 5-source scope.
4. Phase ordering within 6 (6a → 6a.5 → 6b/6c/6d → 6e → 6f).
