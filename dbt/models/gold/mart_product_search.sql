{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_product_id'], 'unique': True},
      {'columns': ['recall_event_id']},
      {'columns': ['hin']},
      {'columns': ['model']},
      {'columns': ['search_vector'], 'type': 'gin'},
      {'columns': ['recall_product_upcs'], 'type': 'gin'},
    ],
    post_hook="analyze {{ this }}"
) }}

-- R3: the `recall_product_upcs` GIN above serves recall-level UPC containment (`@> :upc`) — the real
-- UPC search path (the per-product `upc` column is NULL for every row today). O2/G5: the all-NULL `upc`
-- btree was DROPPED 2026-06-15 (gold-audit confirmed `upc` 0% populated — an empty index rebuilt nightly
-- for no benefit); the `upc` COLUMN stays as a forward-looking placeholder for structured per-product UPCs.

-- mart_product_search — one row per recall_product, denormalized for "is my product recalled?"
-- (Phase 6e, ADR 0038). Feeds GET /products/search and the app's keyword search. Two access
-- paths: (1) exact-identifier lookup on hin (USCG) / model (NHTSA, CPSC) — btree-indexed (the all-NULL
-- per-product `upc` btree was dropped, G5);
-- (2) free-text keyword search over a stored tsvector — GIN-indexed (Postgres built-in FTS; no
-- pg_trgm per ADR 0037).
--
-- Caveats carried honestly: product-grain `upc` is NULL for every source today (CPSC UPCs are
-- recall-level, FDA returns none via the bulk endpoint) — recall-level UPCs are surfaced as
-- recall_product_upcs (jsonb) from mart_recall_summary for a containment filter; structured
-- per-product UPC is a future enrichment. Recall context (title, classification, firm) is pulled
-- from mart_recall_summary to avoid re-deriving the firm-priority logic.

with recall_ctx as (
    select
        recall_event_id,
        title,
        classification,
        risk_level,
        published_at,
        url,
        is_active,
        primary_firm_name,
        product_upcs
    from {{ ref('mart_recall_summary') }}
)

select
    rp.recall_product_id,
    rp.recall_event_id,
    rp.source,
    rp.source_recall_id,
    rp.product_name,
    rp.product_description,
    rp.model,
    rp.type,
    rp.model_year,
    rp.hin,
    rp.upc,
    rc.title             as recall_title,
    rc.classification,
    rc.risk_level,
    rc.published_at,
    rc.url,
    rc.is_active,
    rc.primary_firm_name as firm_name,
    rc.product_upcs      as recall_product_upcs,
    to_tsvector(
        'english',
        coalesce(rp.product_name, '') || ' ' ||
        coalesce(rp.product_description, '') || ' ' ||
        coalesce(rc.title, '') || ' ' ||
        coalesce(rc.primary_firm_name, '')
    ) as search_vector
from {{ ref('recall_product') }} rp
join recall_ctx rc on rc.recall_event_id = rp.recall_event_id
