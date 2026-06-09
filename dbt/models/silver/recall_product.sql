{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_product_id'], 'unique': True},
      {'columns': ['recall_event_id']},
      {'columns': ['upc']},
      {'columns': ['hin']},
    ]
) }}

-- Line-level recall products (ADR 0002). One row per affected product instance.
-- CPSC: explodes the Products[] JSONB array — one row per array element with
--   ordinal-based surrogate key to distinguish identical product names.
-- FDA: each bronze row IS a product (PRODUCTID = source_recall_id), so no array
--   explosion needed — staging feeds directly into the product table.
-- USDA: product_items is a free-text blob; ADR 0002 defers structured parsing.
--   Emit one product row per recall event (recall_product_id = recall_event_id)
--   so referential integrity holds and downstream queries don't silently skip USDA.
-- NHTSA: consumes the v1.5 SCD-2 snapshot's CURRENT view (Phase 6c.7 cutover, ADR 0034).
--   recall_product_id = md5(7-tuple) — (campno, normalize_maketxt(maketxt), modeltxt, yeartxt,
--   compname, rcl_cmpt_id, mfr_comp_ptno), single-homed in stg_nhtsa_recalls_current. The
--   drift-prone fields (mfr_comp_desc/name, bgman, endman, + widened business fields) are demoted
--   to snapshot attributes (Type-1 current + Type-2 history via nhtsa_recall_product_snapshot) so
--   an editorial edit versions instead of fragmenting (Pierce 26V217000); the structural part
--   dimension mfr_comp_ptno stays in the key (ADR 0033 7-tuple amendment 2026-06-06, after the
--   full-corpus 6-tuple over-collapse finding). Product-grain history peer: recall_product_history;
--   event-grain history stays recall_event_history (LAG, ADR 0022).
-- USCG: one row per recall in bronze; mirror USDA's one-product-per-recall
--   grain (recall_product_id = recall_event_id). USCG has no separate product
--   array — the recall row itself names a single boat model + manufacturer.
--   Null-announced_at rows are filtered to mirror the recall_event filter
--   so the FK relationship test on recall_product.recall_event_id holds.
-- Neither CPSC nor FDA associates UPCs with specific products (CPSC UPCs are recall-level;
-- FDA does not return them via the bulk POST endpoint), so upc is NULL for both.

with cpsc_exploded as (
    select
        s.source_recall_id,
        md5('CPSC' || '|' || s.source_recall_id) as recall_event_id,
        (prod.value ->> 'name')               as product_name,
        (prod.value ->> 'description')        as product_description,
        (prod.value ->> 'model')              as model,
        (prod.value ->> 'type')               as type,
        (prod.value ->> 'category_id')        as category_id,
        (prod.value ->> 'number_of_units')    as number_of_units,
        prod.value                            as source_specific_attrs,
        prod.ordinality                       as product_ordinal
    from {{ ref('stg_cpsc_recalls') }} s,
         lateral jsonb_array_elements(coalesce(s.products, '[]'::jsonb))
             with ordinality as prod(value, ordinality)
),

cpsc_products as (
    select
        md5(recall_event_id || '|' || coalesce(product_name, '') || '|'
            || coalesce(model, '') || '|' || product_ordinal::text) as recall_product_id,
        recall_event_id,
        'CPSC'                 as source,
        source_recall_id,
        product_name,
        product_description,
        model,
        type,
        category_id,
        number_of_units,
        cast(null as integer)  as unit_count,
        cast(null as text)     as model_year,
        cast(null as text)     as hin,
        cast(null as text)     as label_artifact_name,
        cast(null as text)     as distribution_list_artifact_name,
        cast(null as text)     as upc,
        source_specific_attrs
    from cpsc_exploded
),

fda_products as (
    select
        md5('FDA' || '|' || source_recall_id)         as recall_product_id,
        md5('FDA' || '|' || recall_event_id::text)    as recall_event_id,
        'FDA'                                         as source,
        source_recall_id,
        product_description_txt                       as product_name,
        -- Bug 3 fix: product_description is the product description text, not the
        -- short reason (that's the event-level recall_reason). product_name maps
        -- to the same field until Bug 2 lifts productdescriptionshort in (b).
        product_description_txt                       as product_description,
        cast(null as text)                            as model,
        product_type_short                            as type,
        cast(null as text)                            as category_id,
        product_distributed_quantity                  as number_of_units,
        cast(null as integer)                         as unit_count,
        cast(null as text)                            as model_year,
        cast(null as text)                            as hin,
        cast(null as text)                            as label_artifact_name,
        cast(null as text)                            as distribution_list_artifact_name,
        cast(null as text)                            as upc,
        jsonb_build_object(
            'rid',                            rid,
            'center_cd',                      center_cd,
            'recall_num',                     recall_num,
            'center_classification_type_txt', center_classification_type_txt
        )                                             as source_specific_attrs
    from {{ ref('stg_fda_recalls') }}
),

usda_products as (
    select
        md5('USDA' || '|' || source_recall_id)        as recall_product_id,
        md5('USDA' || '|' || source_recall_id)        as recall_event_id,
        'USDA'                                        as source,
        source_recall_id,
        title                                         as product_name,
        product_items                                 as product_description,
        cast(null as text)                            as model,
        -- Bug 1 fix: type is the processing category, not the recall lifecycle
        -- type (which is recall_event.lifecycle_status). Multi-value comma-joined.
        processing                                    as type,
        cast(null as text)                            as category_id,
        qty_recovered                                 as number_of_units,
        cast(null as integer)                         as unit_count,
        cast(null as text)                            as model_year,
        cast(null as text)                            as hin,
        labels                                        as label_artifact_name,
        distro_list                                   as distribution_list_artifact_name,
        cast(null as text)                            as upc,
        -- C-slim: labels → label_artifact_name and processing → type are now
        -- first-class columns. product_items_raw is kept — product_description
        -- holds the same unparsed blob and ADR 0002 defers its structured
        -- parsing, so the raw stays available under a distinct key.
        jsonb_build_object(
            'product_items_raw', product_items
        )                                             as source_specific_attrs
    from {{ ref('stg_usda_fsis_recalls') }}
),

nhtsa_products as (
    -- Phase 6c.7 cutover (ADR 0034): selects the v1.5 snapshot's CURRENT rows (dbt_valid_to is null)
    -- — the former recall_product_v15 SELECT, inlined here. recall_product_id is the 7-tuple md5
    -- (single-homed in stg_nhtsa_recalls_current); the snapshot already canonicalized maketxt via
    -- normalize_maketxt for the anchor and carries the latest-wins attribute values. Column shape is
    -- byte-identical to the pre-cutover CTE, so UNION parity with the other four branches holds.
    select
        recall_product_id,
        md5('NHTSA' || '|' || campno)                 as recall_event_id,
        'NHTSA'                                        as source,
        campno                                        as source_recall_id,
        compname                                       as product_name,
        mfr_comp_desc                                  as product_description,
        modeltxt                                       as model,
        -- Bug 1 fix: type is the recall-type code (rcltype), previously NULL.
        rcltype                                        as type,
        cast(null as text)                             as category_id,
        potaff                                         as number_of_units,
        case when potaff ~ '^[0-9]+$' then potaff::integer end as unit_count,
        model_year                                     as model_year,
        cast(null as text)                             as hin,
        cast(null as text)                             as label_artifact_name,
        cast(null as text)                             as distribution_list_artifact_name,
        cast(null as text)                             as upc,
        jsonb_build_object(
            'maketxt',       maketxt,
            'yeartxt',       yeartxt,
            'mfgname',       mfgname,
            'mfgtxt',        mfgtxt,
            'rcl_cmpt_id',   rcl_cmpt_id,
            'mfr_comp_ptno', mfr_comp_ptno,
            'mfr_comp_name', mfr_comp_name,
            'bgman',         bgman,
            'endman',        endman,
            'fmvss',         fmvss
        )                                              as source_specific_attrs
    from {{ ref('nhtsa_recall_product_snapshot') }}
    where dbt_valid_to is null
),

uscg_products as (
    select
        md5('USCG' || '|' || source_recall_id)        as recall_product_id,
        md5('USCG' || '|' || source_recall_id)        as recall_event_id,
        'USCG'                                        as source,
        source_recall_id,
        model_name                                    as product_name,
        coalesce(problem_1, problem_2)                as product_description,
        -- Bug 1 fix: model duplicated product_name (model_name); USCG has no
        -- distinct model field, so model is NULL. boat_type → type below.
        cast(null as text)                            as model,
        boat_type                                     as type,
        cast(null as text)                            as category_id,
        units::text                                   as number_of_units,
        units                                         as unit_count,
        model_year                                    as model_year,
        hin                                           as hin,
        cast(null as text)                            as label_artifact_name,
        cast(null as text)                            as distribution_list_artifact_name,
        cast(null as text)                            as upc,
        -- C-slim: model_year, hin, and units are now first-class columns
        -- (model_year / hin / unit_count). The rest are residual: mic /
        -- company_name / company_official are firm-grain, problem_1/problem_2
        -- are the pre-coalesce originals of product_description, and severity /
        -- disposition / campaign dates are event-level USCG context.
        jsonb_build_object(
            'mic',                  mic,
            'company_name',         company_name,
            'company_official',     company_official,
            'problem_1',            problem_1,
            'problem_2',            problem_2,
            'severity',             severity,
            'disposition',          disposition,
            'campaign_open_date',   campaign_open_date,
            'campaign_close_date',  campaign_close_date
        )                                             as source_specific_attrs
    from {{ ref('stg_uscg_recalls') }}
    where announced_at is not null
),

all_products as (
    select * from cpsc_products
    union all
    select * from fda_products
    union all
    select * from usda_products
    union all
    select * from nhtsa_products
    union all
    select * from uscg_products
)

-- C14: clean processing-category token array (jsonb), derived ONCE from the unioned `type` column
-- but ONLY for USDA (USDA's `type` is the multi-value `processing` field; the other sources' `type`
-- is a single-value product-type code, so it is left scalar and gets NULL tokens). Same comma-split
-- rationale as recall_event.reason_category_tokens (the 2026-06 jsonb arrays wrap, not split —
-- Finding S; the 10 processing tokens have no internal commas, guarded by the membership test).
-- jsonb (not text[]): bronze-consistent, containment-filterable, and dodges the dbt-postgres
-- unit-test ARRAY-cast limitation. Enables single-category filtering.
select
    ap.*,
    case
        when ap.source = 'USDA' and ap.type is not null
        then (
            select jsonb_agg(trim(t))
            from unnest(string_to_array(ap.type, ',')) as t
            where trim(t) <> ''
        )
    end as processing_categories,
    -- C13: structured quantity parsed from the raw number_of_units via quantity_crosswalk
    -- (`recalls parse-quantities`). FDA + USDA populate it; other sources' number_of_units mostly
    -- miss the crosswalk (NULL). quantity_basis separates a per-product quantity from a recall-wide
    -- total (the same total repeats on every product row — fct consumers must not sum those). The raw
    -- number_of_units is preserved alongside. LEFT JOIN to a unique PK → no fan-out.
    xq.quantity_value,
    xq.quantity_unit,
    xq.quantity_category,
    xq.quantity_basis
from all_products ap
left join {{ source('enrichment', 'quantity_crosswalk') }} xq
    on xq.raw_quantity = ap.number_of_units
