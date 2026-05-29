# Field-audit methodology

- **Status:** Active 2026-05-28
- **Scope:** Methodology for Phase 6a foundation-audit field investigations across CPSC, FDA, USDA recalls, USDA establishments, NHTSA, and USCG
- **Companions:**
  - Per-source findings: `documentation/<source>/field_audit_<period>.md`
  - Cross-source backlog: `documentation/audit/capture_expansion_backlog.md`

## Goal

For each source, produce evidence that the silver mapping (`dbt/models/silver/*.sql`) faithfully represents the publisher's data semantics, and identify:

- **(a) silver-side mismappings** — fix without re-extraction (the (a) PR)
- **(b) bronze-side capture gaps** — backfill via a follow-on extraction-expansion PR (the (b) PR)

The two PRs split intentionally — they have different risk profiles, different review surfaces, and (b) needs a deep-rescan to backfill historical bronze rows.

## Source order — layers consulted in this order

Each layer answers a question the previous can't. Don't skip layers.

| Layer | Question it answers |
|---|---|
| **API docs** (`documentation/<source>/*.pdf`) | What does the publisher *say* each field means? What fields *should* exist? |
| **R2 raw payloads** (gzipped under `<source>/<YYYY-MM-DD>/`) | What did the API *actually* return? Including undocumented-but-present fields, or documented-but-absent ones |
| **Bronze tables** (`<source>_recalls_bronze` etc.) | What survived Pydantic validation into Postgres? |
| **Staging + silver SQL** (`dbt/models/...`) | What did dbt do with what survived? |

**Why bronze is not the starting point:** bronze reflects past decisions about which fields to capture. Auditing field selection against bronze is circular — you'd re-validate your own past choices. The FDA `description ← DISTRIBUTIONAREASUMMARYTXT` mismapping is invisible from bronze alone, because the column exists and has plausibly-populated values; you only spot the bug by going to FDA's doc and reading what `DISTRIBUTIONAREASUMMARYTXT` is supposed to mean.

## Per-source artifact: `documentation/<source>/field_audit_<period>.md`

Each source's audit produces a structured doc with these sections:

1. **Status / scope / methodology link**
2. **API field universe** — every field documented in the source's API doc
3. **Current bronze capture** — what we pull today (from `src/schemas/<source>.py` + `src/extractors/<source>.py` displaycolumns/equivalent)
4. **Mismappings** — silver fields whose semantic doesn't match the API doc
5. **Underused captures** — bronze fields buried in `source_payload_raw` JSONB that should be lifted to structured columns
6. **Field-naming gotchas** — places where the publisher's field name lies about its content (e.g., FDA's `productshortreasontxt` actually being a full-text field)
7. **Decisions locked in** — what we agreed in conversation, with dated context
8. **Capture-expansion items deferred to backlog** — high/medium/low priority adds with engineering-tax notes
9. **R2 validation status** — checklist of inspect/probe runs that have or haven't been done

`<period>` is the ISO week the audit was performed, e.g., `field_audit_2026_w22.md`.

## R2 validation pattern (per-source scripts)

Each source gets two audit scripts under `scripts/<source>/audit/` once we audit it. They serve two distinct needs that cassettes and bronze can't cover:

### Need 1 — inspect what's *already in R2*

Validates findings against the broader R2 corpus (months of daily extracts) rather than the cassette snapshot. Confirms NULL rates, value distributions, edge cases against many more records than a cassette captures.

- FDA: `scripts/fda/audit/inspect_landed_payloads.py` (JSON object array)
- CPSC / USDA / NHTSA / USCG: pattern adapts per payload format — see each source's audit doc once written

All inspect scripts share:

- **Three source modes** (mutually exclusive): `--raw-landing-path` (one R2 key, fetched + cached), `--local-path` (use an already-cached file; works offline), `--date YYYY-MM-DD` (DB-resolves via `extraction_runs` to one or more R2 keys, all cached)
- **Gitignored cache** at `data/exploratory/<source>/` (anchored from repo root via `Path(__file__).resolve().parents[3]`)
- **Cache hit/miss messages to stderr**; analysis output to stdout — easy to redirect to a file without contaminating with diagnostics
- **Per-field statistics via shared `_lib.summarize_records`**: N records, NULL count + %, distinct count, length stats for strings, full distribution for low-cardinality (≤20 distinct) fields, value samples otherwise

### Need 2 — probe what's *not yet captured*

R2 is silent on fields we never requested. To verify a proposed-add field actually populates, issue a live API call with an expanded request shape (FDA: `displaycolumns`; CPSC: equivalent; etc.) and inspect the response. Does NOT land to R2, does NOT load to bronze.

- FDA: `scripts/fda/audit/probe_displaycolumns.py`
- Per-source equivalents created when each source is audited

Probe responses save to `data/exploratory/<source>/probes/probe_<UTC-timestamp>.json` by default so re-runs that want different aggregations don't burn API calls. `--no-save` for truly throwaway probes.

## Cross-source artifacts

After all six sources have per-source audit docs:

1. **Consolidation table** — a single table in `documentation/audit/cross_source_consolidation.md` (created at consolidation time, not before) showing each semantic concept (product description, hazard/recall reason, distribution area, classification/severity, lifecycle dates, firm address, etc.) mapped to each source's contributing field, with the proposed silver column name. Drives the (a) silver-remap PR.
2. **`documentation/audit/capture_expansion_backlog.md`** — the running parking lot for (b) PR items, grown source-by-source as each audit runs.

The consolidation step is also when we decide whether existing silver column names need *renaming* (e.g., `recall_event.description` → `recall_event.recall_reason` if all sources turn out to populate it with defect narrative rather than free-text description). Renames don't happen in per-source audits — they happen once, against the union.

## Cache hygiene

`data/exploratory/<source>/` is gitignored at `.gitignore:64`. Inspection sessions accumulate (hundreds of MB across sources is normal after a few weeks of audit work). Probes under `data/exploratory/<source>/probes/` are particularly easy to forget. Periodically prune.

## References

- `project_scope/branch_sequencing_strategy.md` — Phase 6a is the foundation-audit phase that consumes these docs
- `project_scope/implementation_plan.md` — master phase plan
- `project_scope/phase-6-execution-plan.md` — Phase 6 execution sequencing constraints
- `prompts/phase_6_deliverable_plan.md` — the deliverable plan listing the cross-source field-association workstream as Phase 6 add-on #2 (the trigger for this methodology)
