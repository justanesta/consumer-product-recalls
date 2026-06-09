# Silver field capture-expansion — plan (the "(b) PR")

- **Status:** Design — not started, no branch yet. Sibling to `project_scope/silver-field-remap-plan.md` (the "(a) PR"); follows it in the hard chain `6a → 6a.5 → (a) remap → {6b, this} → 6c` per `archive/phase-6-execution-plan.md` § Sequencing Constraints.
- **Owns:** the "(b) capture-expansion PR" execution — the FDA work that the (a) remap deferred. Two parts: **(A)** surface FDA firm-address + firm-continuity + posted-date fields (already in bronze via migration 0019) into silver behind a new `firm_fda_attributes` sidecar + a `recall_event` posted-date column; **(C)** the one genuinely-new extraction — FDA Tier-3 per-event press releases (new bronze child table + extractor). Tier-2 is **excluded** (settled skip — see Scope contract).
- **Points at** (single-home — this plan does not restate any of them):
  - `documentation/audit/capture_expansion_backlog.md` § FDA — the per-source candidate parking lot (the tier tables + engineering-tax decisions). This plan *executes* the FDA section; it does not re-list the fields.
  - `documentation/audit/cross_source_consolidation.md` §7 deferral register (the FDA firm fields routed here) + the canonical firm-attribute naming.
  - `documentation/silver_design_notes.md` §3 — the firm supertype / per-source-attribute-subtype model that `firm_fda_attributes` instantiates as a third sidecar (alongside `firm_establishment_attributes` / `firm_manufacturer_attributes`).
  - `documentation/fda/api_observations.md` Finding **K0.3** (Tier-2 = no net-new content), **K0.1** (`productlmd` null), **K0** (bulk-POST 33-col datagroup), **K0.2** (sort∈displaycolumns invariant for any `_DISPLAY_COLUMNS` edit).
  - `project_scope/freetext-enrichment-backlog.md` — owns the FDA `code_information` → `recall_product_code` parse (deferred out of this PR; see below).
  - ADR 0035 (cross-source SCD-2 authority — `firm_fda_attributes` is Type-1 now, SCD-2 a 6c concern), ADR 0036 (cross-source canonical naming), ADR 0002 (FEI as the FDA firm anchor), ADR 0027 (bronze storage-forced only), ADR 0013 (extractor lifecycle — Tier-3 extractor follows it).

## Context

The (a) remap conforms silver over fields already in bronze and **deferred every not-yet-surfaced FDA column to this PR** (`silver-field-remap-plan.md` Scope contract). Migration 0019 (2026-05-31) already landed the FDA firm-address, firm-continuity (`firm_surviving_*`), `posted_internet_dt`, and `code_information` columns in `fda_recalls_bronze` and the 2026-06-02 seed populated them — so the FDA firm work here is a **silver lift, not new extraction**. The only genuinely-new extraction in this PR is Tier-3 press releases (lookup-endpoint-only; bulk POST 406s them per Finding K0). Phase 6a audits + the 2026-06-03 Tier-2 probe (`scripts/fda/audit/probe_tier2_shorts.py`) closed the remaining "capture vs derive vs skip" questions, so the (b) scope is now decidable rather than speculative.

## Scope contract

Every FDA item sorts into exactly one bucket:

### IN — (A) Tier-1 silver lift (fields already in bronze; no extraction change)
- **`firm_fda_attributes` sidecar**, keyed on `firm_fei_num` (FDA FEI), reconstructed `DISTINCT ON (firm_fei_num) … ORDER BY extraction_timestamp DESC` from `stg_fda_recalls` (latest-per-FEI; the one sidecar fed by the recall feed rather than a dedicated directory source — identical join semantics to the USDA/USCG sidecars). Holds firm identity/address/continuity: `firm_legal_nam`, `firm_city_nam`, `firm_state_cd`, `firm_state_prvnc_nam`, `firm_country_nam`, `firm_postal_cd`, `firm_line1_adr`, `firm_line2_adr`, `firm_surviving_nam`, `firm_surviving_fei`. **Type-1 (latest)** now; SCD-2 deferred to 6c (a firm that moved shows different addresses across recalls — same call as USCG MIC, ADR 0035). The conformed `firm` dim stays identity-only; `firm_fei_num` already rides in `firm.observed_company_ids`.
- **`recall_event` posted-internet date** — `posted_internet_dt` → a `recall_event` date distinct from `event_lmd` (Definitions PDF: blank for recalls before 2022-10-25).
- Staging: extend `stg_fda_recalls` to project the migration-0019 columns it currently drops.
- UNION column-list parity: any new `recall_event`/`recall_product` column is NULL-cast to the right type in **every** source branch (the top correctness risk, same as the (a) PR).

### IN — (C) Tier-3 per-event press releases (genuinely new extraction)
- **New bronze child table** `fda_event_press_releases_bronze`, event-grain, keyed `(recalleventid, pressreleaseurl)` (M:1 — multiple releases per event). Columns: `recalleventid, pressreleaseurl, pressreleaseissuedt, pressreleasetype` (+ `createdt` as a free rider from the event GET). Alembic migration + `_rejected` table + Pydantic schema (ADR 0014).
- **New `FdaPressReleaseExtractor`** — work-list = `DISTINCT recalleventid` from `fda_recalls_bronze`; throttled per-event `GET /search/pressreleaseurls/?eventid={id}` (Akamai pacing; ~25K GETs on a deep-rescan, ~50–180/day incremental). Direct analog of `UscgManufacturerDetailExtractor` (bronze-sourced work-list → polite per-record fetch → archive → drift-fenced parse); reuse that precedent. Incremental + deep-rescan paths per the Phase 5 standing requirement.
- **Silver** `recall_event_press_release` child (M:1 to `recall_event`) — external authoritative links + issue date + type ("State/Firm/FDA").

### EXCLUDED — Tier-2 per-product GET (settled SKIP)
- `productdescriptionshort` / `recallreasonshort` / `codeinfoshort` and the three `*indicator` flags — **SKIP**. Empirically settled 2026-06-03: the `*short` fields are whitespace-normalized truncations of full fields already in bronze (`content ⊆ full` for every populated short: 8/8, 7/7, 5/5), and the `*indicator`s are pure "was-truncated" presence flags (`'true' ⟺ short present`, zero exceptions across 60). Zero net-new content for ~134K Akamai-paced GETs. Evidence: `documentation/fda/api_observations.md` **K0.3**. **Bug 2** (FDA `product_name` should prefer the short) → derive in silver instead: `regexp_replace(product_description_txt,'\s+',' ','g')` then truncate — no fetch.
- `productlmd` — SKIP per K0.1 (empirically null on every surface).

### DEFERRED OUT — `code_information` → `recall_product_code` (post-6b)
`code_information` is in bronze (migration 0019, full-bronze max ~8.86M chars). Its silver form is a **parse into a structured product-code child table**, not a raw pass-through — a free-text-enrichment workstream owned by `project_scope/freetext-enrichment-backlog.md` (§ "FDA — code_information → product-code records"), scoped for after Phase 6b. **This PR does not surface `code_information` to silver.** Raw stays in bronze as source of truth.

## Guardrails
- **The user runs all code** (psql, dbt, python, extractor runs, deep-rescan) — workstreams describe SQL/commands, never run them. SQL lands under `scripts/sql/fda/<layer>/<purpose>.sql`.
- **`_DISPLAY_COLUMNS` is unchanged in this PR** — (A)'s fields are already requested (migration 0019). Any future edit must preserve `sort ∈ displaycolumns` (Finding K0.2) and the 33-col datagroup limit (K0).
- **Tier-3 needs Akamai accommodation** — courteous pacing (mirror the FDA throttle handling in `probe_tier2_shorts.py` / `probe_displaycolumns.py`: Mozilla UA, signature cache-bust, HTML-apology + 204 detection, sleep between GETs); possibly a static-IP allowlist for the ~25K-GET deep-rescan. Decide pacing before the rescan, not during.
- **Don't disturb the `firm.sql` ↔ `recall_event_firm.sql` lockstep** (comment at `recall_event_firm.sql:22`/`:98`); `firm_fda_attributes` is an outrigger off `firm.observed_company_ids`, not a change to the conformed dim.
- **One deep-rescan** backfills the new Tier-3 bronze; runs on the dev Neon branch first (pre-cron deep-rescan validation, master plan).

## Workstreams (Design-level; mature into a checklist when the branch opens)
| # | Workstream | Notes |
|---|---|---|
| W1 | `firm_fda_attributes` sidecar (silver) + `recall_event` posted-date | mirror `firm_establishment_attributes` / `firm_manufacturer_attributes`; `_silver.yml` tests (unique on `firm_fei_num`, not_null key); staging projection of the migration-0019 columns |
| W2 | Tier-3 bronze: Alembic migration + Pydantic schema + `_rejected` | `fda_event_press_releases_bronze`, key `(recalleventid, pressreleaseurl)` |
| W3 | `FdaPressReleaseExtractor` (incremental + deep-rescan) | precedent `UscgManufacturerDetailExtractor`; work-list from `fda_recalls_bronze`; cassettes per Phase 5 Step 4; drift-fence |
| W4 | Tier-3 silver `recall_event_press_release` + tests | M:1 child; relationship test to `recall_event` |
| W5 | Backfill: `recalls deep-rescan fda-press-releases` on dev → validate | row counts vs ~25K events; `change_type` recorded; watermark untouched |
| W6 | Doc sync | flip `capture_expansion_backlog.md` FDA tier rows to as-built; `silver_design_notes.md` (3→ note firm_fda sidecar); this plan → Complete on merge |

## Open questions
- **Tier-3 endpoint choice** — `GET /search/pressreleaseurls/?eventid={id}` (4 cols) vs `GET /recalls/event/{eventid}` (also yields `createdt`, `distributionpatternshort`). Confirm which returns the press-release array cleanly via the existing `bruno/fda/lookup/get_press_release_urls.yml` before writing the extractor. (`distributionpatternshort` is itself a derivable short — skip, like the other shorts.)
- **`createdt` vs `posted_internet_dt`** — capture_expansion notes these are "likely distinct but unclear." If the event GET is used, capture `createdt` as a free rider and compare.
- **`firm_fda_attributes` SCD** — Type-1 here; the SCD-2 "firm moved between recalls" question is owned by the cross-source SCD work (ADR 0035 / master plan Phase 6 SCD item), not this PR.

## Related
- `project_scope/silver-field-remap-plan.md` (the "(a)" sibling that deferred these fields here), `documentation/audit/capture_expansion_backlog.md` (the parking lot), `project_scope/archive/phase-6-execution-plan.md` (parent + sequencing), `project_scope/branch_sequencing_strategy.md` (cross-branch order), `project_scope/freetext-enrichment-backlog.md` (the `code_information` parse, deferred further).
- ADRs 0036 / 0035 / 0002 / 0027 / 0013 as above.
