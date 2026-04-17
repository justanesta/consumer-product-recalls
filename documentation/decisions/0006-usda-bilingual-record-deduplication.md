# 0006 — USDA dedup of English/Spanish bilingual recall records

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

USDA FSIS publishes each recall as **two separate API records** — one English and one Spanish — distinguished by `langcode` and `field_has_spanish`. Both share the same `field_recall_number` (e.g. `040-2022`). Naive ingestion would double-count USDA recalls in every aggregation: total counts, classification breakdowns, reason histograms — all inflated by 2×.

Three options considered:

- **Drop Spanish records entirely.** Simplest, but loses Spanish-language recall summaries that are useful for the consumer-facing app's accessibility goals.
- **Keep both as separate rows with a `langcode` discriminator.** Preserves all data but every downstream aggregation query must remember to filter to one language. High risk of forgotten filters in views written months later.
- **Collapse to one event row per recall, English as primary, Spanish summary attached as a secondary field.** More work at the silver-layer transformation but yields semantically correct counts by default.

## Decision

- Collapse to one row per (`source = 'USDA'`, `source_recall_id = field_recall_number`) in `recall_event`.
- The English record is the primary source for all standard `recall_event` columns (title, description, hazard, classification, dates, status, etc.).
- Spanish summary is attached on the same event row in a `summary_alt_lang` JSONB column, structured as `{"es": "<Spanish summary text>"}`. Using JSONB rather than a `summary_es` column keeps the schema extensible if any source adds further languages without proliferating columns.
- Both raw records (English and Spanish) are preserved in the bronze layer (`usda_recalls_bronze`) for audit. Deduplication happens only when materializing silver from bronze.

## Consequences

- Aggregations and counts in dashboards match consumer expectations: one recall event per published recall.
- Spanish-language presentation remains supported in the consumer-facing app via `summary_alt_lang->>'es'`.
- Audit clarity at silver decreases slightly — to compare English vs Spanish text (e.g. spotting translation drift or source-data errors), an analyst must drop back to the bronze layer.
- **Edge case to handle in the silver builder:** if a recall publishes a Spanish record without an English counterpart (rare but possible during initial publication, before USDA back-fills the English version), the transformation must emit an alert and fall back to the Spanish record as primary, rather than silently dropping the recall. This becomes a validation rule.
- The `summary_alt_lang` JSONB shape generalizes if USDA or any other source adds further languages later — no further schema changes needed.
