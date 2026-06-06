{{ config(materialized='table') }}

-- Line-level recall products (ADR 0002). One row per affected product instance.
-- CPSC: explodes the Products[] JSONB array — one row per array element with
--   ordinal-based surrogate key to distinguish identical product names.
-- FDA: each bronze row IS a product (PRODUCTID = source_recall_id), so no array
--   explosion needed — staging feeds directly into the product table.
-- USDA: product_items is a free-text blob; ADR 0002 defers structured parsing.
--   Emit one product row per recall event (recall_product_id = recall_event_id)
--   so referential integrity holds and downstream queries don't silently skip USDA.
-- NHTSA: each bronze row IS a product instance at 11-tuple grain (vehicle ×
--   component × part × batch); recall_product_id = md5(11-tuple) per ADR 0031.
--   Mirrors CPSC's md5(parent || distinguishing_fields || disambiguator)
--   recipe structurally — bgman/endman serve as NHTSA's batch-level
--   disambiguator (analog to CPSC's product_ordinal). v1 fragmentation rate
--   ~0.0004%/day (AC DELCO maketxt normalization observed 2026-05-08); see
--   ADR 0031 + documentation/nhtsa/incremental_delta_findings.md Section G.
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
    select
        md5(
            'NHTSA' || '|' || campno
            -- ADR 0033 Normalization class (Phase 6b 6b.3): hash the normalize_maketxt-
            -- canonicalized make so 'AC DELCO' -> 'ACDELCO' (and any whitespace/case make
            -- drift) yields ONE recall_product_id. SAME macro as stg_nhtsa_recalls' identity
            -- partition (one row survives there; this keeps its surrogate stable). The
            -- DISPLAYED maketxt in the source_specific_attrs blob below stays RAW (critic C14).
            || '|' || {{ normalize_maketxt('maketxt') }}
            || '|' || coalesce(modeltxt, '')
            || '|' || coalesce(yeartxt, '')        || '|' || coalesce(compname, '')
            || '|' || coalesce(rcl_cmpt_id, '')    || '|' || coalesce(mfr_comp_ptno, '')
            || '|' || coalesce(mfr_comp_desc, '')  || '|' || coalesce(mfr_comp_name, '')
            || '|' || coalesce(bgman::text, '')    || '|' || coalesce(endman::text, '')
        )                                             as recall_product_id,
        md5('NHTSA' || '|' || campno)                 as recall_event_id,
        'NHTSA'                                       as source,
        campno                                        as source_recall_id,
        compname                                      as product_name,
        mfr_comp_desc                                 as product_description,
        modeltxt                                      as model,
        -- Bug 1 fix: type is the recall-type code (rcltype), previously NULL.
        rcltype                                       as type,
        cast(null as text)                            as category_id,
        potaff                                        as number_of_units,
        case when potaff ~ '^[0-9]+$' then potaff::integer end as unit_count,
        model_year                                    as model_year,
        cast(null as text)                            as hin,
        cast(null as text)                            as label_artifact_name,
        cast(null as text)                            as distribution_list_artifact_name,
        cast(null as text)                            as upc,
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
        )                                             as source_specific_attrs
    from {{ ref('stg_nhtsa_recalls') }}
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
)

select * from cpsc_products
union all
select * from fda_products
union all
select * from usda_products
union all
select * from nhtsa_products
union all
select * from uscg_products
