{{ config(
    materialized='table',
    indexes=[
      {'columns': ['firm_id'], 'unique': True},
      {'columns': ['normalized_name']},
    ]
) }}

-- mart_firm_profile — one row per canonical firm (Phase 6e, ADR 0038). Because firm.firm_id
-- is already the 6b cross-source cluster id, this IS the cross-source rollup (a Honda or Tyson
-- that appears under several sources collapses to one row). Feeds GET /firms/{id} and the firm
-- landing page: identity + aliases + recall statistics + the current per-source SCD-2 attributes
-- (address, succession, geography) pulled from the three sidecars.
--
-- Sidecar join: firm.observed_company_ids is a jsonb array of source structural ids. The id
-- namespaces are disjoint (FDA FEI = long numeric, USDA establishment_number = "M1234"/"M1+P1"
-- composites, USCG MIC = 3-char), so unnesting and left-joining each id to all three sidecars
-- matches each id to exactly its own sidecar. Attributes are returned as per-source jsonb blocks
-- (a canonical firm can span >1 source identity) rather than forcing one flattened address.

with firm_ids as (
    select
        f.firm_id,
        cid.company_id
    from {{ ref('firm') }} f
    left join lateral jsonb_array_elements_text(
        coalesce(f.observed_company_ids, '[]'::jsonb)
    ) as cid(company_id) on true
),

firm_attr_rows as (
    select
        fi.firm_id,
        ea.establishment_id,
        to_jsonb(ea) as est_json,
        ma.mic,
        to_jsonb(ma) as mfr_json,
        fa.firm_fei_num,
        to_jsonb(fa) as fda_json
    from firm_ids fi
    left join {{ ref('firm_establishment_attributes') }} ea on ea.establishment_id = fi.company_id
    left join {{ ref('firm_manufacturer_attributes') }} ma  on ma.mic = fi.company_id
    left join {{ ref('firm_fda_attributes') }} fa           on fa.firm_fei_num::text = fi.company_id
),

firm_attrs as (
    select
        firm_id,
        jsonb_agg(est_json order by establishment_id) filter (where establishment_id is not null) as establishment_attributes,
        jsonb_agg(mfr_json order by mic)              filter (where mic is not null)              as manufacturer_attributes,
        jsonb_agg(fda_json order by firm_fei_num)     filter (where firm_fei_num is not null)     as fda_attributes
    from firm_attr_rows
    group by firm_id
),

firm_event_stats as (
    select
        ref.firm_id,
        count(distinct ref.recall_event_id)                                  as total_recalls,
        count(distinct ref.recall_event_id) filter (where re.is_active)       as active_recalls,
        min(re.published_at)                                                  as first_recall_at,
        max(re.published_at)                                                  as last_recall_at,
        jsonb_agg(distinct ref.role)                                          as roles
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_event') }} re on re.recall_event_id = ref.recall_event_id
    group by ref.firm_id
),

firm_source_counts as (
    select
        ref.firm_id,
        re.source,
        count(distinct ref.recall_event_id) as cnt
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_event') }} re on re.recall_event_id = ref.recall_event_id
    group by ref.firm_id, re.source
),

firm_source_agg as (
    select firm_id, jsonb_object_agg(source, cnt) as recalls_by_source
    from firm_source_counts
    group by firm_id
),

firm_product_counts as (
    select
        ref.firm_id,
        count(distinct rp.recall_product_id) as distinct_products
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_product') }} rp on rp.recall_event_id = ref.recall_event_id
    group by ref.firm_id
)

select
    f.firm_id,
    f.canonical_name,
    f.normalized_name,
    f.observed_names,
    f.observed_company_ids,
    f.alternate_names,
    coalesce(fes.total_recalls, 0)   as total_recalls,
    coalesce(fes.active_recalls, 0)  as active_recalls,
    fes.first_recall_at,
    fes.last_recall_at,
    fes.roles,
    fsa.recalls_by_source,
    coalesce(fpc.distinct_products, 0) as distinct_products,
    fa.establishment_attributes,
    fa.manufacturer_attributes,
    fa.fda_attributes
from {{ ref('firm') }} f
left join firm_event_stats fes    on fes.firm_id = f.firm_id
left join firm_source_agg fsa     on fsa.firm_id = f.firm_id
left join firm_product_counts fpc on fpc.firm_id = f.firm_id
left join firm_attrs fa           on fa.firm_id = f.firm_id
