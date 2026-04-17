# 0009 — NHTSA: preserve line-level granularity into silver

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

ADR 0008 established that NHTSA bronze ingests the flat file at its native granularity: one row per `(CAMPNO × MAKETXT × MODELTXT × YEARTXT × COMPNAME)` — typically 5–10 rows per recall campaign, ~300–500K total rows across the historical corpus.

The silver-layer materialization (per the header/line/firm model in ADR 0002) has two viable shapes:

- **Roll up to campaign level.** One `recall_event` per CAMPNO; the per-vehicle/component breakdown collapsed into a JSONB array on the event (e.g., `affected_vehicles: [{make, model, year, component}, ...]`) with no `recall_product` rows for NHTSA. Smaller silver footprint (~50–70K events instead of ~300–500K product rows).
- **Preserve line-level.** One `recall_event` per CAMPNO; each (vehicle/component) line from the flat file becomes a separate `recall_product` row, linked to the campaign. Larger row count, simpler queries.

The deciding factor is the canonical NHTSA query: **"Is my 2018 Honda Civic recalled?"** This is the question users actually ask of NHTSA recall data, and it should be a simple `WHERE` clause — not a JSONB unpacking exercise. Other consumer queries (recalls by component, recalls by manufacturer, recalls by model year) follow the same pattern and are similarly served by line-level structure.

The volume tradeoff is real but not load-bearing: ~300–500K product rows fits comfortably in Postgres, and ADR 0005's storage projection already accounts for this granularity.

## Decision

NHTSA data is materialized into silver at line-level granularity:

- One `recall_event` row per `CAMPNO`.
- One `recall_product` row per (CAMPNO × make × model × year × component) line from bronze, linked to its event via `recall_event_id`.
- `recall_product.identifier_type = 'MAKE_MODEL_YEAR'` for NHTSA lines.
- `brand` ← `MAKETXT`; `model` ← `MODELTXT`; `model_year` ← `YEARTXT` (parsed to SMALLINT, with `9999` per the source's "unknown" sentinel mapped to NULL).
- Component description (`COMPNAME`) and manufacturer-component metadata (`MFR_COMP_NAME`, `MFR_COMP_DESC`, `MFR_COMP_PTNO`, `RCL_CMPT_ID`) go into `source_specific_attrs` JSONB.
- `units_affected` ← `POTAFF` (potential units affected) at the line level; if needed at event level it can be summed in a gold view.

## Consequences

- "Is my Honda Civic recalled?" is a one-liner: `SELECT * FROM recall_product WHERE brand = 'HONDA' AND model = 'CIVIC' AND model_year = 2018`.
- Aggregations like "all airbag recalls" or "all 2018 model year recalls" are SQL-native — no JSONB extraction.
- Silver `recall_product` is dominated by NHTSA rows (~300–500K of ~430–760K total across all sources). Postgres handles this scale trivially; standard B-tree indexes on `(brand, model, model_year)` and `(recall_event_id)` cover the dominant query patterns.
- Storage projection in ADR 0005 already assumed line-level inclusion — no revision needed.
- Annual growth bounded by NHTSA's historical rate: ~700–1000 campaigns × ~5–10 lines = ~5–10K new product rows/year.
- The `recall_event` / `recall_product` schemas stay consistent across sources — no NHTSA-specific exceptions in the model. This is what ADR 0002 set up the structure to enable.
