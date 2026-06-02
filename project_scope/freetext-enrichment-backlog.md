# Free-text → structured enrichment — backlog

- **Status:** Design (deferred workstream — not started; Phase 6/7). No branch yet.
- **Owns:** the cross-source silver-enrichment items that parse **already-captured** free-text bronze fields into structured columns (numeric quantities, state/country arrays, embedded identifiers, contact parts). These are deferred **out of** the (a) `silver-field-remap` branch (which does Tier-0 cleanup + Tier-1 deterministic derives only), and they are **not** (b) capture-expansion (the source fields are already in bronze).
- **Why a separate workstream:** structured extraction from free text has a messy long tail, is materially **cross-source** (one parser serves several sources' analogous fields), and carries silent-corruption risk if rushed. Keeping it off the foundation-audit branches lets (a) land the high-value/low-risk normalization now and defer the parsing to a focused pass.
- **Points at:** `documentation/audit/bronze_corpus_profile.md` §7 (the coverage evidence); `project_scope/silver-field-remap-plan.md` (the Tier-0/1 work that precedes this); `documentation/audit/capture_expansion_backlog.md` (the sibling **(b)** bronze-capture parking lot — the USDA enrichment items below are tracked there, not duplicated here).

## The normalization tier model (shared vocabulary)

| Tier | What | Layer | Done in |
|---|---|---|---|
| 0 — Cleanup | sentinel→NULL, trim, whitespace/CR collapse | staging | (a) `silver-field-remap` |
| 1 — Deterministic derive | enum/flag from cleaned text via unambiguous rules | silver | (a) `silver-field-remap` |
| **2 — Structured extraction** | **parse free text → structured values** | silver enrichment | **this backlog** |

## Items

### FDA — distributed-quantity → `quantity_value` + `quantity_unit`
- **Source:** `product_distributed_quantity` (bronze, captured). (a) keeps it as cleaned TEXT in `recall_product.number_of_units`.
- **Evidence** (2026-06-02, `scripts/sql/fda/bronze/profile_freetext_normalization.sql` Q1–Q3, 134,461 rows): **66% cleanly parseable** — 9.4% pure integer + 56.6% integer+unit (`N units/cases/bottles`). Messy tail 20.9%: weights (`919,616.31 total pounds, for all products`), multi-figure totals (`7,400,000 (globally); 260,395 (US)`), cross-product sums (`Total of all products (Listed #1 thru 101) = 304735 units`). 8.1% empty, 2.4% sentinel; 56,967 distinct normalized forms.
- **Cross-source synergy:** the weight/total tail is the **same shape as USDA `qty_recovered`** (`X lbs`) — one parser + one canonical `quantity_unit` taxonomy (each / case / bottle / pound / kg / …) should serve FDA + USDA + NHTSA `potaff` (integer) + USCG `units`.
- **v1 sketch:** regex leading `[0-9,]+` + trailing unit token → `quantity_value` numeric + `quantity_unit` canonical; NULL both where ambiguous (cleaned text preserved). ~66% immediate coverage; a `quantity_basis` flag (per-unit vs total/weight) for the totals.

### FDA — distribution-area → `distribution_states[]` + `distribution_countries[]`
- **Source:** `distribution_area_summary_txt` (bronze, captured). (a) ships the Tier-1 `distribution_scope` enum + the cleaned text; this is the **residual** structured extraction.
- **Evidence** (Q4): **~33% carry parseable state content** — 17.4% single short region + 15.4% comma state-code lists (`AR, GA, IL, LA, MS, MO, OK, and TX`); 8.6% narrative-long; 11.2% mid. (47% are Nationwide/Worldwide, handled by the Tier-1 scope enum.)
- **Cross-source synergy:** USDA recalls carry a more-structured `field_states` (already a firm-disambiguation signal) → a shared state-normalizer (name / 2-letter code / list → canonical array) serves FDA + USDA.
- **v1 sketch:** for the Regional bucket, tokenize on `,`/`and`, map state names + 2-letter codes → canonical `distribution_states[]`; international tokens → `distribution_countries[]`.

## Related — already tracked elsewhere (do not duplicate)
- **USDA** `field_product_items` structured parse (embedded UPCs, lot codes, FSIS establishment numbers, dates) + `field_company_media_contact` parse — tracked in `documentation/audit/capture_expansion_backlog.md` USDA "deferred to Phase 6/7" table. The establishment-number extraction also feeds **Phase 6b** USDA recall→establishment disambiguation (Signal 1, `phase-6-execution-plan.md`).
- The canonical `quantity_unit` + `distribution_states`/`distribution_countries` taxonomies should be designed **once here** and consumed by all sources.

## Sequencing
No hard dependency beyond "captured bronze + Tier-0-cleaned staging text exist." Best picked up after the silver foundation is stable (post-(a) remap; alongside or after Phase 6b/6c). Promote to an `Active` plan + branch when started.
