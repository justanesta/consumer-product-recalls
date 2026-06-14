# 0042 — Gold serving marts are a published read contract

**Status:** Accepted
**Date:** 2026-06-13

## Context

ADR 0038 decided gold's *shape* (denormalized `mart_*` serving tables + `fct_*` aggregates, first-principles indexing). It did **not** say anything about gold's *stability obligations* to a downstream consumer, because at the time there was none.

That changes with the `recalls-api` serving layer (ADR 0024 / 0025): a separate, open, no-auth, read-only FastAPI repo whose only contract with this pipeline is "it reads the gold marts." It does **not** own schema, migrations, or dbt — it reads. Several properties of the three serving marts (`mart_recall_summary`, `mart_product_search`, `mart_firm_profile`) and the one-row `gold_meta` table are now **load-bearing for the API at runtime**, and a few of them are *not* obvious from the model SQL alone:

- The API computes `recall_event_id = md5(source || '|' || source_recall_id)` from URL path params to hit `UNIQUE(recall_event_id)` — so the md5 recipe *and* each source's `source_recall_id` business key are a wire contract, not an internal detail (the recipe is single-homed in `dbt/models/silver/recall_event.sql`).
- `recall_product_id` is the API's opaque keyset-cursor anchor — it was migrated to the stable `(event, ordinal)` key on this branch precisely so it survives nightly rebuilds.
- The API uppercases `source` path params before the md5, so the `source` enum being **closed + UPPERCASE** (`CPSC FDA USDA NHTSA USCG`) is load-bearing.
- The serving-mart column set and the firm sidecar output names (`firm_{usda,uscg,fda}_attributes` after the R5 rename, ADR 0036) are API-breaking once the API freezes its `openapi.json`.
- Several columns are **deliberately not normalized**, and the API models its response schemas around the raw shapes — "tidying" them silently would break it: `classification` is **source-native** (FDA/USDA `Class I/II/III`, USCG `H/L/M/S`, CPSC/NHTSA NULL), not a unified enum; `risk_level` is USDA-only; `is_active` is **tri-state** (NULL for CPSC/NHTSA); `announced_at` is nullable by design; `distribution_states` (scalar text) is distinct from `distribution_state_codes` (`text[]`).

Without a record, a future pipeline change — renaming a column for tidiness, "conforming" `classification`, re-keying a surrogate, lower-casing `source` — looks locally harmless but silently breaks a deployed public API. The per-mart authoritative schema lives in the API repo's `project_scope/build/01-ground-truth-gold-marts.md`; this ADR is the **pipeline-side** record that these marts are a contract, not free-to-refactor internals.

## Decision

1. **The gold serving surface is a published, versioned read contract.** It comprises `mart_recall_summary`, `mart_product_search`, `mart_firm_profile`, and `gold_meta`. Their column names, types, surrogate-key recipes, enum domains, and the deliberately-un-normalized shapes listed above are **interface**, not implementation.

2. **The load-bearing invariants** (the bullets in Context) **must not change silently.** A change to any of them is a *breaking* change and requires, in order: (a) coordinate with the `recalls-api` repo and re-freeze its `openapi.json`; (b) bump `gold_meta.schema_version` (the `--vars '{gold_schema_version: "N"}'` knob, R4) so a consumer can detect the break; (c) record the change here or in the cited single-home doc.

3. **`gold_meta` is the contract's version + freshness signal.** `rebuilt_at` (the dbt `run_started_at`, identical across one build) drives the API's ETag/Last-Modified; `schema_version` is the manual contract version. Adding columns to a mart is backward-compatible (consumers ignore unknowns) and does **not** require a `schema_version` bump; renaming/removing/retyping/re-keying does.

4. **This ADR adds no new constraint to the modeling itself** — it *names* obligations that ADR 0038's shapes and ADR 0036's naming already imply, and points the maintainer at the API-side single-home for the per-column detail. It does not restate that detail here (documentation_model single-home rule).

## Consequences

- A maintainer refactoring gold now has a tripwire: "is this column/key/enum in the 0042 contract surface?" If yes, it is coordinated + version-bumped, not silently shipped.
- The API can rely on the four objects' stability across nightly rebuilds and treat a `schema_version` change as its cue to re-freeze `openapi.json`.
- The cost is discipline: cosmetic gold cleanups that touch the contract surface are now gated on API coordination. The R5 sidecar rename is the canonical example — cheap *now* (API not yet frozen), a breaking change *after*, which is exactly why it is being done pre-go-live.
- `fct_*` aggregate marts are **not** part of this contract surface (they back deferred `/stats/*` endpoints, ADR 0024); when a `/stats/*` endpoint ships, the relevant `fct_*` joins this contract by the same rule.
- No code change. The serving-layer-gold-readiness plan (`project_scope/serving-layer-gold-readiness-plan.md`) is the execution home for the gold changes that satisfy this contract; this ADR is the *why-it's-a-contract* single-home it points at.
