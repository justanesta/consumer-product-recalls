# 0002 — Unit of analysis: header / line / firm

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

Every in-scope source publishes recalls with a 1-to-many relationship between a "recall event" and "products affected":

- CPSC returns event records with nested `Products`, `Manufacturers`, `Retailers`, `Importers`, `Distributors` collections.
- FDA's iRES API exposes rows at *product* granularity grouped by `recalleventid`.
- USDA returns one record per recall but lists multiple product items as unstructured text.
- NHTSA returns one row per (campaign × make × model × year × component) — a single campaign can have dozens of rows.
- USCG covers multiple model years / hull ID ranges per defect notice.

Two pure options were considered and rejected:

- **Pure event-level (one row per recall):** simple ingestion and `COUNT(*)` for dashboards, but "is my UPC recalled?" requires unpacking JSON inside a row, and fights FDA's natively product-level API shape.
- **Pure product-level (one row per affected product):** trivial point lookups, but recall-level metadata (hazard, dates, classification) gets denormalized onto every row — painful when an agency revises a recall and N rows must be updated. Worse, "product" granularity is inconsistent across sources (UPC vs VIN range vs lot code vs hull ID), so the same row type would conflate apples and oranges.

A separate concern: cross-source firm rollups (e.g. "all Honda recalls across NHTSA + CPSC + USDA") are a high-value consumer query that neither pure model handles well, since firm names appear inside the recall payload as variant strings.

## Decision

Adopt header / line / firm normalization in the silver layer:

- `recall_event` — one row per agency-published recall (the "header"). Carries hazard, classification, dates, status, source identifiers, and a `source_payload_raw` JSONB column.
- `recall_product` — one row per affected product instance (the "line"), with a `source_specific_attrs` JSONB column for the long tail of per-agency fields. 1:N from `recall_event`.
- `firm` — manufacturers, distributors, importers, and retailers as a separate dimension with canonical names and observed name variants.
- `recall_event_firm` — many-to-many between events and firms with a `role` column distinguishing manufacturer / distributor / importer / retailer / recalling-firm relationships.

## Consequences

- Unit of analysis is chosen by the query, not the schema: dashboards count events, consumer "is my product recalled?" lookups hit products, brand rollups hit firm.
- Source-specific weirdness lives in JSONB on the line, avoiding wide sparse-column tables.
- Cross-source firm reconciliation becomes a real entity-resolution problem. FDA's `firmfeinum` (FDA Establishment Identifier) is a stable cross-recall key within FDA; cross-agency resolution requires fuzzy name matching. This is embraced as a portfolio-worthy skill surface rather than avoided.
- USDA's `field_product_items` is unstructured text. Parsing it into normalized `recall_product` rows is non-trivial. v1 ships USDA at event-only granularity with no product rows; product extraction is deferred to a v2 milestone.
- Updates to a recall event only touch one row in `recall_event`, not N rows in a denormalized table.
