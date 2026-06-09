# Phase 6f — Diagrams + Documentation Sync (execution plan)

- **Status:** Active — plan landed 2026-06-07 on `feature/phase-6f-diagrams-docs-sync`; stages 6f.1–6f.5 pending.
- **Owning master plan:** `project_scope/phase-6-execution-plan.md` §6f (Stream 3) → `project_scope/implementation_plan.md` Phase 6.
- **Structure:** staged commits on this one branch, one-line commit messages at the breaks, single PR to `main` at the end (per the "phase = commits + single PR" convention). Every commit is a clean stopping point — designed to be picked up across multiple sittings.

## Context

Phase 6f is the **closing deliverable of Phase 6** — the last step before Phase 7 (orchestration go-live). All schema work (6a audit → 6a.5 backfill → silver remap → 6b firm resolution → 6c history/lifecycle → 6d operational tooling → 6e gold + indexes) has merged. The diagrams and top-level prose now lag the real architecture badly, and there is **no silver/gold ERD at all**.

The driving goal is comprehension, not box-ticking doc hygiene: *help the reader understand the database content and the integration flow of the app.* Two pillars:

- **Database content** → a column-level ERD set + a written silver/gold overview (why the firm sidecars, why SCD-2, what each table is for).
- **Integration flow** → a refreshed pipeline DAG + a *sorted-and-documented* set of data-pull/update cadence decisions.

The **cadence (timing) decisions are sorted and documented here**, even though the cron *implementation* is Phase 7. The design is ~80% settled in [ADR 0010](../documentation/decisions/0010-ingestion-cadence-and-github-actions-cron.md) (+ amendments); 6f resolves the ~6 genuinely-open questions to documented defaults so Phase 7 just implements.

### Current staleness (2026-06-07 survey)

- **`pipeline-architecture.drawio`** — shows 5 sources, hard-codes "5 source tables", no deep-rescan cycles, no USCG manufacturer sources, no `uscg_manufacturers → uscg_manufacturer_details` work-list dependency, no SCD-2 dims, no FDA press-release tier.
- **`orchestration-schedule.drawio`** — 5 swimlanes; missing FDA deep-rescan; USCG shown as one source (should be 3: recalls + manufacturers + details); misleading "no rescan needed" FDA note; USDA "rescan" is really a full-dump.
- **No ERD exists** (`silver-gold-erd.*` is net-new).
- **`architecture.md`** — extractor table / registry counts / source lists / medallion narrative stale; "reserved for future use" past-tense on a live component.
- **`data_schemas.md`** — `recall_event_firm.role` glossary + quick-lookup still say `['manufacturer, retailer, importer, distributor, establishment]`; current `_silver.yml` is `['manufacturer, importer, distributor, establishment, filer]` (retailer dropped, filer added). Bronze + gold table lists were already refreshed (2026-06-01 / 2026-06-07) — verify, don't re-do.
- **`operations.md`** — USCG cadence row incomplete (the 3-source table was explicitly deferred to "the Phase 6f §6f rewrite").
- **`silver_design_notes.md`** — covers the 5 recall sources + roles correctly; needs the `firm_manufacturer_attributes` SCD-2 mapping confirmed and the 8-vs-5-source framing tidied.
- **`commands.md`** — source list omits `uscg_manufacturers` + `uscg_manufacturer_details`.

## Decisions locked at plan time

1. **Diagram tooling = Mermaid** (text-based, renders natively on GitHub, authored directly from the dbt `_silver.yml`/`_gold.yml` column defs, git-diffable, **no manual SVG-export step**). The two legacy `.drawio`/`.svg` files are kept but marked **superseded**, pointing at their new Mermaid homes.
2. **Cadence output = decision doc that resolves + defaults** — amend ADR 0010 with the full 9-source cadence matrix; settle the doc-only conflicts now; assign documented defaults + a "revisit with X signal" for the empirical ones. Phase 7 just implements.
3. **ERD = subject-area split + overview** — 4 Mermaid diagrams (overview map, recall-event core, firm cluster + SCD-2 sidecars, gold marts), not one dense monolith.

## Mermaid workflow (single-home rule honored)

- Each diagram's **single canonical home is a fenced ` ```mermaid ` block inside the markdown doc where it belongs** — no `.mmd` mirror files (avoids drift; GitHub renders the fenced block inline, so no `.svg` export is needed).
  - The 4 **ERDs** + silver/gold textual summary → new `documentation/database_overview.md` (the "understand the database" deliverable).
  - The **pipeline DAG** → `documentation/architecture.md` (replaces the stale medallion figure).
  - The **cadence/schedule diagram** → the cadence section (ADR 0010 amendment / `operations.md`).
- Hand-editing: copy the fenced block into [`mermaid.live`](https://mermaid.live), or install the VS Code **"Markdown Preview Mermaid Support"** (bierner) extension to live-preview the block in the `.md` itself; the official **"Mermaid Chart"** extension is an alternative with a side preview pane. GitHub.com renders fenced `mermaid` blocks with zero setup.
- A short `documentation/diagrams/README.md` documents this workflow and marks the legacy `.drawio` files superseded.

---

## Workstreams + commit sequence

Order is **dependency-driven** (understanding-first; later stages consume earlier outputs). Each commit is a stopping point.

### 6f.1 — ERD set + database overview  *(the "database content" pillar — do first; it forces a full schema re-read whose facts 6f.4 reuses)*

New `documentation/database_overview.md`:

- **Written silver/gold summary** — table/view inventory with grain + purpose + the *rationale* (why firm is a cross-source canonical cluster; why 3 SCD-2 sidecars keyed on `establishment_number` / `firm_fei_num` / `mic`; why snapshots; why gold splits `mart_*` serving vs `fct_*` aggregates). Source facts from `_silver.yml`, `_gold.yml`, `silver_design_notes.md`, `gold_design_notes.md`.
- **4 Mermaid `erDiagram`s:**
  1. **Overview map** — every silver + gold entity as a box (no columns), grouped silver-core / firm-cluster / gold, with the key relationships.
  2. **Recall-event core** — `recall_event`, `recall_product` (+ `recall_product_history` snapshot), `recall_event_firm` bridge, `recall_event_history`, `recall_lifecycle`, `recall_event_press_release`, `recall_distribution_area`, `recall_event_establishment_resolution`. Columns + PK/FK.
  3. **Firm cluster + SCD-2 sidecars** — `firm`, `firm_crosswalk` (enrichment), the 3 sidecar current-views + their `silver_snapshots.*` history tables, `firm_fei_edges`, `uscg_mic_reassignment_years`, and the `recall_event_firm` link. Columns + the `mic` / `firm_fei_num` / `establishment_number` anchors.
  4. **Gold marts** — `mart_recall_summary` / `_firm_profile` / `_product_search` + the 9 `fct_*`, with lineage edges back to their silver inputs.

**Commit:** `Phase 6f: silver+gold ERD set + database overview (Mermaid)`

### 6f.2 — Pipeline DAG refresh  *(integration-flow, structural)*

Author a Mermaid `flowchart` in `architecture.md` replacing the stale medallion figure. Must show:

- **All 9 bronze-writing sources** (CPSC, FDA, **FDA press-releases**, USDA recalls, USDA establishments, NHTSA, USCG recalls, **uscg_manufacturers**, **uscg_manufacturer_details**) → R2 landing → bronze → silver → gold → API/dashboard.
- **Deep-rescan cycles** (CPSC/FDA/USCG) as feedback edges into bronze (content-hash dedup).
- The **`uscg_manufacturers → uscg_manufacturer_details` work-list dependency** and the **`fda_recalls → fda_press_releases` work-list dependency** (load-bearing).
- SCD-2 dim build (`firm_*_attributes` snapshots) in the silver box.
- Mark `pipeline-architecture.drawio` superseded (header note → new home).

> ⚠️ **Verify source counts at edit time** — `architecture.md` claims `EXTRACTOR_BY_SOURCE_NAME` = 8 / `DEEP_RESCAN_BY_SOURCE_NAME` = 7, but the bronze inventory is 9 sources (incl. `fda_press_releases`). Read `src/config/source_registry.py` and reconcile the exact numbers before writing them (don't trust the stale doc).

**Commit:** `Phase 6f: refresh pipeline DAG to current sources + deep-rescan/work-list edges (Mermaid)`

### 6f.3 — Cadence decisions (amend ADR 0010) + cadence diagram  *(must precede 6f.4 — operations.md consumes these)*

Amend `documentation/decisions/0010-ingestion-cadence-and-github-actions-cron.md` with a **full 9-source cadence matrix** and resolutions to the open questions (table below). Add a Mermaid schedule diagram (daily lane, weekly lane, deep-rescan lane, quarterly lane). Cross-link from `operations.md`.

**Recommended resolutions (resolve + default):**

| Open question | 6f decision | Confidence |
|---|---|---|
| NHTSA daily vs weekly (docs conflict) | **Weekly (Mon)** — full flat-file; a daily re-download content-hash-dedups to nothing. Settle docs + workflow comment to weekly. | High — settle now |
| FDA press-release **incremental** cadence | **Daily cascade after FDA recalls** (work-list from `fda_recalls_bronze`, `event_lmd` cursor). | High |
| FDA press-release **deep-rescan** | **Not needed** — per-event work-list already re-touches edited events. Recommend removing/disabling `deep-rescan-fda-press-releases.yml`; document why. | Confirm before disabling |
| USCG-manufacturers **listing** deep-rescan | **Weekly full-walk backstop** (mirror the recalls sibling); workflow created in Phase 7. | Medium |
| USCG-details **Tier-2** quarterly vs annual | **Quarterly default**, revisit with production detail-edit-frequency after Phase 7 go-live (matches ADR 0010 2026-06-02). | High |
| Exact cron times | Concrete UTC grid (proposal below), **Phase-7-tunable**. | Propose |
| Cross-source run ordering | Document the 2 work-list deps as ordering constraints; implement as scheduled offsets / `workflow_run` triggers in Phase 7. | High |

**Proposed cron grid (UTC, Phase-7-tunable):** daily extracts staggered 02:00–02:50 (CPSC 02:00, FDA 02:10, USDA 02:20, USDA-est 02:30, USCG-recalls 02:40, USCG-mfr 02:50); cascades 03:00 FDA-PR / 03:10 USCG-details Tier-1; NHTSA weekly Mon 04:00; Sunday deep-rescans CPSC 05:00 / FDA 05:30 / NHTSA 06:00 / USCG-mfr 06:30; USCG-details Tier-2 quarterly (1st Jan/Apr/Jul/Oct 07:00).

**Commit:** `Phase 6f: consolidate ingestion cadence decisions (ADR 0010 amend) + cadence diagram`

### 6f.4 — Prose doc-sync sweep  *(consumes 6f.1 schema facts + 6f.3 cadence decisions)*

Targeted, evidence-based edits (not a rewrite):

- **`architecture.md`** — extractor component table (concrete subclasses already listed; fix the "reserved for future use" past-tense), registry counts (reconciled per 6f.2 note), source lists in any remaining figures, medallion narrative.
- **`data_schemas.md`** — fix `recall_event_firm.role` in **both** the glossary (~line 128) and the quick-lookup (~line 197): drop `retailer`, add `filer`. Confirm bronze + gold lists are current (already refreshed — verify only).
- **`operations.md`** — replace the incomplete USCG row with the **3-source USCG cadence table** from 6f.3; add the `uscg_manufacturer_details` extract/deep-rescan runbook + the bulk-`Date Modified` re-baseline note (`change_type='schema_rebaseline'`).
- **`silver_design_notes.md`** — confirm/extend the `firm_manufacturer_attributes` SCD-2 mapping + the flag-as-time-sensitive recall→manufacturer join (ADR 0035); tidy 8-vs-5-source framing.
- **`commands.md`** — add `uscg_manufacturers` + `uscg_manufacturer_details` to the source list + example commands.
- Flagged for 6f.4 (not bundled into this docs work): src/config/source_registry.py's own docstring still says "8-entry / 6-entry dict" and "Seven of eight sources" — stale code-doc; and the line-88 deep-rescan rationale ("current-state directory") should be reframed as "full-dump → redundant." I left a visible flag in the architecture.md note pointing at it.
- Still open for the diagrams stream: the pipeline-architecture.drawio superseded marker — that rolls naturally into 6f.5 (the documentation/diagrams/README.md + supersede markers), so I'll catch it there rather than touch the .drawio now.

**Commit:** `Phase 6f: sync prose docs to current source/gold/SCD-2 reality`

### 6f.5 — Firm-geography note + diagram README + close-out

- **Firm-location geography exploration note** (surfaced in 6e) — in `gold_design_notes.md` / `database_overview.md`: decide + document the intended semantics of `fct_recalls_by_geography`'s `firm_location` lens (it inherits the cross-source canonical firm's **HQ / FDA-FEI registered** address — Walmart→AR, Target→MN — not the production/distribution site; a name-merged firm can carry multiple FEIs/MICs across states). Decide whether to **relabel** the lens to say what it is vs. prefer a single primary address; note the `distribution` lens is the cleaner "where did the product go" answer.
- **`documentation/diagrams/README.md`** — the Mermaid workflow + legacy `.drawio` superseded markers.
- **Close-out** — `TODO.md` item 5 closed; `implementation_plan.md` Phase 6 reflects 6f done + the 6f cadence decisions feeding Phase 7.

**Commit:** `Phase 6f: firm-geography semantics note + diagram workflow README + Phase 6 close-out`

### End — PR to main

Open the PR (`gh pr create`, on explicit request) summarizing the ERD set, DAG/cadence refresh, cadence decisions, and prose sync. **Version bump** (`pyproject.toml`, minor) — docs/diagrams phase. User runs the merge.

---

## Critical files

**Create:** `documentation/database_overview.md`, `documentation/diagrams/README.md`.
**Edit:** `documentation/architecture.md`, `documentation/data_schemas.md`, `documentation/operations.md`, `documentation/silver_design_notes.md`, `documentation/commands.md`, `documentation/gold_design_notes.md`, `documentation/decisions/0010-ingestion-cadence-and-github-actions-cron.md`, `TODO.md`, `project_scope/implementation_plan.md`, `pyproject.toml`.
**Mark superseded (header note only):** `documentation/diagrams/pipeline-architecture.drawio` + `.svg`, `documentation/diagrams/orchestration-schedule.drawio` + `.svg`.
**Read-for-facts (don't edit):** `dbt/models/silver/_silver.yml`, `dbt/models/gold/_gold.yml`, `src/config/source_registry.py`, `config/sources/*.yaml`, `documentation/decisions/0023-*.md`, `project_scope/phase-5d-uscg-manufacturers-detail.md`.

## Verification

- **Render check** — open each touched `.md` in GitHub (or VS Code with the Mermaid preview extension); every fenced `mermaid` block renders without syntax error.
- **ERD fidelity** — each entity/column/PK/FK in the 4 ERDs traces to a real column in `_silver.yml` / `_gold.yml` (spot-check 3 tables per diagram against the yml).
- **Cadence completeness** — the ADR 0010 matrix has a row for all 9 sources; each of the 6 open questions has an explicit disposition; no "TBD" left except the 2 flagged "revisit with prod signal".
- **No regressions in claims** — `data_schemas.md` role values match `_silver.yml` exactly; registry counts match `source_registry.py`; no doc still calls USCG "deferred" or the pipeline "5-source".
- **Single-home** — each diagram appears in exactly one doc; legacy `.drawio` files carry a superseded pointer, not a duplicate.

## Open items / to confirm

- **FDA press-release deep-rescan removal** (6f.3) — the one cadence decision with medium confidence; confirm before disabling `deep-rescan-fda-press-releases.yml`.
- **Delete vs keep** the legacy `.drawio`/`.svg` — recommend keep-with-superseded-header for history.
- **ERD relationship cardinalities** for the fuzzy/optional joins (e.g. `recall_event_firm.establishment_number` nullable) — render as optional (`}o`) and confirm during 6f.1.

## Sequencing constraints

- **6f.1 precedes 6f.4** — the ERD/overview build is a full schema re-read; its canonical facts (table list, grains, roles, accepted_values) feed the prose-sync, so doing it first avoids double-work.
- **6f.3 precedes 6f.4** — `operations.md`'s cadence table consumes the settled cadence decisions.
- **6f.2 is independent** of cadence specifics (structural DAG) and can move earlier or later.
- **6f.5 is last** — it closes out TODO/implementation_plan after the substantive work lands.
