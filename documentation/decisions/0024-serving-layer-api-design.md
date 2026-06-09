# 0024 — Serving-layer API design

**Status:** Accepted (2026-06-09)
**Date:** 2026-06-09

> **Build location:** the serving API is built as a **separate repository** after this pipeline's
> production cron go-live — there is no in-repo "Phase 8" (decided 2026-06-09). This ADR records the
> design decision that repo follows; the Claude-Code-ready execution plan is
> `project_scope/future-repos/fastapi-serving-layer-plan.md`. The decision is recorded here because the
> API↔gold relationship (§5 below) is a decision about *this* project's gold layer and its consumers
> (the trigger ADR 0038 §1 / 0024 named).

## Context

The dbt gold layer (ADR 0038) is the consumer-shaped serving layer: denormalized `mart_*` serving
tables and `fct_*` aggregate views. Its first consumer is a public read-only API answering "what was
recalled?", "is my product recalled?", and "what's this firm's recall history?". ADR 0038 §1 deferred
the gold star-schema decision to "Phase 8 framing (this ADR)"; the website's chart inventory (the
gating fork) is now enumerated in `project_scope/future-repos/website-frontend-plan.md` §5.5.

## Decision

1. **Stack.** FastAPI + Pydantic v2 (request/response validation + OpenAPI generated from the same
   types), SQLAlchemy Core **async** over **asyncpg**, read-only to Neon `main`. Gold is wide
   denormalized tables read one-row-at-a-time, so we use SQLAlchemy Core (parameterized `select()` /
   `text()`), **not** the ORM. Rationale + rejected alternatives (Flask, full ORM): future-repo plan §1.
2. **Four core endpoints, mapped 1:1 to gold marts:**
   - `GET /recalls` — list + filters (`source`, `classification`, date range, `firm`) → `mart_recall_summary`.
   - `GET /recalls/{source}/{recall_id}` — detail (products, firms, history, lifecycle) → `mart_recall_summary`.
     The lookup **computes the silver surrogate** `recall_event_id = md5(source || '|' || source_recall_id)`
     from the path params and hits the **existing unique index** on `mart_recall_summary.recall_event_id`
     — no new composite index required.
   - `GET /products/search` — `mart_product_search` Postgres FTS (`tsvector` + GIN) for keyword search;
     exact-identifier lookups (HIN, model) via btree. **UPC caveat:** product-grain `upc` is NULL for all
     sources today (recall-level UPCs only, via the `recall_product_upcs` jsonb containment filter) — the
     API must not advertise a product-grain UPC search that returns nothing.
   - `GET /firms/{id}` — cross-source firm rollup → `mart_firm_profile`.
3. **Pagination.** Keyset (seek) pagination, not `OFFSET` (stable under concurrent writes, index-friendly).
4. **OpenAPI.** FastAPI's built-in generator is the source of truth at `/openapi.json`; a committed
   `openapi.json` snapshot serves as a contract-test fixture (drift = test failure).
5. **API↔gold relationship — no star schema (yet).** For an API-fed **fixed** chart set, the existing
   `fct_*` aggregate marts already *are* the dashboard layer (one query each) — a Kimball star buys
   nothing and would duplicate silver (ADR 0038 §1). So: **no star.** A `/stats/*` dashboard-endpoint
   family (by-month, by-classification, monthly-trend, by-geography, firm-leaderboard, overview) backed
   by the `fct_*` marts is **deferred out of API-v1** and **gated on the website's concrete chart
   inventory** (website plan §5.5); that inventory is the trigger that un-defers it. Only a BI/semantic
   layer or user-driven cross-dimensional slicing would justify revisiting the star.
6. **Auth posture.** Public read-only, no auth tier in v1 (project vision); abuse control is
   platform/rate-limit level, not application auth.

## Consequences

- The API reads `mart_*` directly without re-joining silver; gold's `dim_/mart_/fct_` shapes (ADR 0038)
  are the contract.
- Computing the `md5` surrogate in the API layer keeps the path-based detail lookup index-backed with
  zero upstream schema change; the only cost is the API knows the surrogate recipe (documented here +
  in `silver_design_notes.md` §4).
- Deferring `/stats/*` keeps API-v1 to four endpoints; the website inventory decides when to add them.
- No star means the website cannot do arbitrary cross-dimensional pivots without a new `fct_*` model or
  endpoint — accepted at this corpus size and chart set.
- Building in a separate repo (not an in-repo phase) means this pipeline repo stays the data layer; the
  API repo depends on it only through the gold schema contract + a read-only Neon role.
