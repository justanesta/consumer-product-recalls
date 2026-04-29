# Bronze-to-Silver Architecture Overview

This document covers the design decisions made when extending the CPSC-only silver layer (Phase 4) to include FDA data (Phase 6). It serves as the canonical reference for how the two sources are unified into the shared silver schema.

---

## Column Mapping: CPSC vs. FDA

| Silver column | CPSC mapping | FDA mapping |
|---|---|---|
| `recall_event.source_recall_id` | RecallNumber (e.g. "24-158") | RECALLEVENTID::text (e.g. "98724") |
| `recall_event.recall_event_id` (surrogate) | `MD5('CPSC' \| '\|' \| source_recall_id)` | `MD5('FDA' \| '\|' \| recall_event_id::text)` |
| `recall_event.announced_at` | `recall_date` | `recall_initiation_dt` |
| `recall_event.published_at` | `last_publish_date` | `event_lmd` |
| `recall_event.title` | `title` (API field) | `coalesce(recall_num, center_cd\|'-'\|recall_event_id::text) \|\| ' — ' \|\| firm_legal_nam` |
| `recall_event.description` | `description` (API field) | `distribution_area_summary_txt` |
| `recall_event.url` | `url` (API field) | `NULL` — not returned by iRES API |
| `recall_event.classification` | `NULL` — CPSC does not publish | `center_classification_type_txt` ('1', '2', '3', 'NC') |
| `recall_event.status` | `NULL` — CPSC does not publish | `phase_txt` ('Ongoing', 'Terminated', 'Completed') |
| `recall_event.hazards` | JSONB array from source | `NULL` — reason lives in `recall_product.product_description` |
| `recall_product` rows | One per element in `Products[]` JSONB array | One per bronze row — each row IS a product |
| `recall_product.product_name` | `products[].name` | `product_description_txt` |
| `recall_product.product_description` | `products[].description` | `product_short_reason_txt` |
| `recall_product.type` | `products[].type` | `product_type_short` |
| `recall_product.number_of_units` | `products[].number_of_units` | `product_distributed_quantity` (free text) |
| `firm` source | JSONB arrays: manufacturers, retailers, importers, distributors | Scalar `firm_legal_nam` + `firm_fei_num`; role = 'manufacturer' |
| `recall_event_firm.role` | manufacturer / retailer / importer / distributor | manufacturer only |

---

## Major Design Decisions

### 1. Two-Level FDA Dedup: Staging Deduplicates Products, Silver Aggregates to Events

FDA's iRES API returns one row per product (`PRODUCTID`), not one row per recall event. A single recall event can cover dozens of products (e.g., 86 products under `RECALLEVENTID` 96869). This means FDA bronze has a fundamentally different granularity from CPSC bronze.

The silver pipeline handles this in two steps:

1. **Staging (`stg_fda_recalls.sql`)** deduplicates at the product level using `ROW_NUMBER() OVER (PARTITION BY source_recall_id ORDER BY extraction_timestamp DESC)`. This resolves edit history — a product that was re-extracted with updated content will have multiple bronze rows with different content hashes; staging keeps only the latest.

2. **`recall_event.sql`** further aggregates product rows up to event-level headers using `DISTINCT ON (recall_event_id) ORDER BY recall_event_id, extraction_timestamp DESC`. This works correctly because event-level fields (`recall_num`, `firm_legal_nam`, `phase_txt`, `center_classification_type_txt`) are identical across all products in the same recall event — any representative product row yields the correct event header.

### 2. Flat vs. Exploded Product Rows

CPSC bronze stores products as a JSONB array on each event row. The `recall_product.sql` model explodes that array using `LATERAL JSONB_ARRAY_ELEMENTS() WITH ORDINALITY` to get one row per product. Ordinal position is included in the product surrogate key to distinguish products with identical names within the same recall.

FDA bronze is already flat — each bronze row IS a product, with `PRODUCTID` as the unique identifier. The FDA branch of `recall_product.sql` reads staging directly without any array explosion. The surrogate key is simply `MD5('FDA' | '|' | source_recall_id)` since PRODUCTID is a globally unique API sequence.

### 3. Scalar Firm vs. JSONB Firm Arrays

CPSC encodes firms as four separate JSONB arrays per event row (`manufacturers`, `retailers`, `importers`, `distributors`), each containing structured `{name, company_id}` objects. The firm models explode these arrays and track all four roles.

FDA encodes a single firm as two scalar columns (`firm_legal_nam`, `firm_fei_num`) on each product row, always in the 'manufacturer' role. There is no equivalent to CPSC's retailer/importer/distributor roles in the FDA data.

Both `firm.sql` and `recall_event_firm.sql` handle this via UNION ALL: the CPSC branch explodes JSON arrays; the FDA branch uses `DISTINCT` on `(recall_event_id, firm_legal_nam)` to avoid duplicating the same firm across multiple products in the same event.

### 4. Surrogate Key Strategy

All silver surrogate keys use deterministic MD5 hashing, prefixed by source name to guarantee global uniqueness across sources:

- `recall_event_id` → `MD5('<SOURCE>' || '|' || <event_business_key>)`
- `firm_id` → `MD5(UPPER(TRIM(firm_name)))` — deliberately source-agnostic, enabling implicit cross-source firm deduplication by normalized name
- `recall_product_id` → `MD5('<SOURCE>' || '|' || <product_business_key>)`

**CPSC event business key:** RecallNumber string (e.g., "24-158"). This is the stable CPSC identifier that persists across republications.

**FDA event business key:** RECALLEVENTID integer cast to text (e.g., "98724"). This is FDA's internal recall event ID, which groups all product lines belonging to the same recall.

**Product surrogate key difference:** CPSC product keys include ordinal position (`MD5(recall_event_id || '|' || product_name || '|' || model || '|' || ordinal)`) because product names are not guaranteed unique within a recall. FDA product keys use only PRODUCTID (`MD5('FDA' || '|' || source_recall_id)`) since PRODUCTID is already globally unique as an API-assigned sequence.

**Firm key:** `MD5(UPPER(TRIM(firm_name)))` is source-agnostic. A firm that appears in both CPSC and FDA data with the same normalized name will collapse to a single `firm` row with both sources' company IDs collected in `observed_company_ids`. This is the basis for the cross-source firm resolution planned for later phases.

### 5. Null-Filling for Schema Parity

The two sources don't map symmetrically to every silver column. Rather than restructuring the schema per-source, each source fills what it has and uses `NULL` for fields it doesn't expose:

- `recall_event.classification` / `.status`: `NULL` for CPSC (CPSC does not publish classification or status in their recall data); populated from `center_classification_type_txt` / `phase_txt` for FDA.
- `recall_event.hazards`: populated JSONB array for CPSC; `NULL` for FDA (FDA's hazard reason lives in `recall_product.product_description` via `product_short_reason_txt`).
- `recall_event.url`: populated for CPSC (direct API field); `NULL` for FDA (not returned by the iRES bulk POST endpoint).
- `recall_product.upc`: `NULL` for both sources at this stage. CPSC UPCs are recall-level (stored in `recall_event.source_payload_raw`) and not associated with specific products. FDA does not expose UPCs via the bulk POST endpoint.

This approach keeps the silver schema stable and source-agnostic while allowing downstream consumers to filter or coalesce NULLs as needed.

### 6. Source Freshness Thresholds

CPSC freshness warning is set at **48 hours** — CPSC publishes on all days, so a two-day gap indicates a genuine extraction problem.

FDA freshness warning is set at **72 hours** — FDA publishes Monday–Friday only (no weekend activity observed across 90 days of bronze data, except US federal holidays). A weekend pause will naturally reach ~63 hours without any pipeline problem. The 72-hour threshold prevents false-positive alerts while still catching a missed Monday run. Both sources error at **7 days**, which indicates a genuine outage regardless of publication cadence.
