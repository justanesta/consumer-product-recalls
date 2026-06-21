{{ config(
    materialized='table',
    meta={'index_specs': [
      {'suffix': 'firm_id',         'cols': 'firm_id', 'unique': True},
      {'suffix': 'normalized_name', 'cols': 'normalized_name'},
    ]},
    post_hook="analyze {{ this }}"
) }}

-- Indexes declared in config(meta.index_specs), built by the folder-level rebuild_indexes() post_hook
-- (DROP-THEN-CREATE on the final {{ this }}), NOT config(indexes=[...]) — which oscillates indexes out
-- every other build under dbt 1.11.x (gold-audit 2026-W26; see macros/rebuild_indexes.sql). firm_id
-- (unique) backs GET /firms/{id}.

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
    left join {{ ref('firm_usda_attributes') }} ea on ea.establishment_id = fi.company_id
    left join {{ ref('firm_uscg_attributes') }} ma  on ma.mic = fi.company_id
    left join {{ ref('firm_fda_attributes') }} fa           on fa.firm_fei_num::text = fi.company_id
),

firm_attrs as (
    select
        firm_id,
        jsonb_agg(est_json order by establishment_id) filter (where establishment_id is not null) as firm_usda_attributes,
        jsonb_agg(mfr_json order by mic)              filter (where mic is not null)              as firm_uscg_attributes,
        jsonb_agg(fda_json order by firm_fei_num)     filter (where firm_fei_num is not null)     as firm_fda_attributes
    from firm_attr_rows
    group by firm_id
),

-- Single MATERIALIZED pass over the firm⋈recall-event bridge — previously scanned 2-3× (event stats,
-- source counts, and the product fan-out each re-joined recall_event_firm). One row per (firm, event,
-- role); count(distinct event) collapses the role duplication.
firm_recalls as materialized (
    select
        ref.firm_id,
        ref.recall_event_id,
        ref.role,
        re.source,
        re.is_active,
        -- first/last_recall_at reflect when the recall was ANNOUNCED (the truthful event date),
        -- not last-published. coalesce to the non-null published_at for the ~20 FDA events with no
        -- announce date (2026-W25, fix/announced-at-date-join; mirrors the fct_* time-series basis).
        coalesce(re.announced_at, re.published_at) as recall_date
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_event') }} re on re.recall_event_id = ref.recall_event_id
),

firm_event_stats as (
    select
        firm_id,
        count(distinct recall_event_id)                          as total_recalls,
        count(distinct recall_event_id) filter (where is_active) as active_recalls,
        min(recall_date)                                         as first_recall_at,
        max(recall_date)                                         as last_recall_at,
        jsonb_agg(distinct role)                                 as roles
    from firm_recalls
    group by firm_id
),

firm_source_agg as (
    select firm_id, jsonb_object_agg(source, cnt) as recalls_by_source
    from (
        select firm_id, source, count(distinct recall_event_id) as cnt
        from firm_recalls
        group by firm_id, source
    ) s
    group by firm_id
),

-- distinct_products per firm WITHOUT the firm×product fan-out that made this the ~180s bottleneck: a
-- multi-firm, multi-product NHTSA recall (e.g. a 139-component Takata campno) exploded ref×rp into
-- firms×products rows before count(distinct). Instead count products per EVENT once, then sum over the
-- firm's distinct events — a recall_product_id belongs to exactly one event, so the per-event distinct
-- counts sum to the firm's distinct-product total (no double-count).
event_products as (
    select recall_event_id, count(distinct recall_product_id) as n_products
    from {{ ref('recall_product') }}
    group by recall_event_id
),

firm_product_counts as (
    select fr.firm_id, sum(ep.n_products) as distinct_products
    from (select distinct firm_id, recall_event_id from firm_recalls) fr
    join event_products ep on ep.recall_event_id = fr.recall_event_id
    group by fr.firm_id
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
    fa.firm_usda_attributes,
    fa.firm_uscg_attributes,
    fa.firm_fda_attributes
from {{ ref('firm') }} f
left join firm_event_stats fes    on fes.firm_id = f.firm_id
left join firm_source_agg fsa     on fsa.firm_id = f.firm_id
left join firm_product_counts fpc on fpc.firm_id = f.firm_id
left join firm_attrs fa           on fa.firm_id = f.firm_id
