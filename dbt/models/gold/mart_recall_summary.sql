{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
      {'columns': ['source', 'published_at']},
      {'columns': ['is_active']},
      {'columns': ['classification']},
    ]
) }}

-- mart_recall_summary — denormalized one-row-per-recall serving table (Phase 6e, ADR 0038).
-- Feeds GET /recalls (list) and GET /recalls/{source}/{recall_id} (detail): the silver
-- recall_event header plus its firm rollup, product rollup, lifecycle summary, and
-- edit-history flags, so the API serves a recall from one keyed read. Surrogate keys are
-- reused from silver verbatim (ADR 0038) — never re-keyed. One row per recall_event; the
-- left joins are to pre-grouped rollups, so they never fan out.

with firm_rollup as (
    select
        ref.recall_event_id,
        count(distinct ref.firm_id) as firm_count,
        jsonb_agg(
            jsonb_build_object(
                'firm_id', ref.firm_id,
                'name', f.canonical_name,
                'role', ref.role,
                'match_confidence', ref.match_confidence
            )
            order by ref.role, f.canonical_name
        ) as firms,
        -- Primary display firm: prefer the producing entity (manufacturer / establishment)
        -- over the filer / importer / distributor roles for landing-page headers + sorting.
        (array_agg(
            f.canonical_name
            order by case ref.role
                when 'manufacturer'  then 1
                when 'establishment' then 2
                when 'filer'         then 3
                when 'importer'      then 4
                when 'distributor'   then 5
                else 6
            end, f.canonical_name
        ))[1] as primary_firm_name
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('firm') }} f on f.firm_id = ref.firm_id
    group by ref.recall_event_id
),

product_rollup as (
    select
        recall_event_id,
        count(*)                                                              as product_count,
        jsonb_agg(distinct product_name) filter (where product_name is not null) as product_names,
        jsonb_agg(distinct model) filter (where model is not null)              as models,
        jsonb_agg(distinct hin) filter (where hin is not null)                  as hins
    from {{ ref('recall_product') }}
    group by recall_event_id
),

-- recall_event_history is keyed on (source, source_recall_id, langcode, field_name,
-- changed_at). Collapse to one row per event identity for the has-been-edited flag +
-- a raw edit-row count (USDA counts EN+ES change rows; fine as an activity proxy).
history_rollup as (
    select
        source,
        source_recall_id,
        count(*) as edit_event_count
    from {{ ref('recall_event_history') }}
    group by source, source_recall_id
)

select
    re.recall_event_id,
    re.source,
    re.source_recall_id,
    re.title,
    re.recall_reason,
    re.url,
    re.announced_at,
    re.published_at,
    re.classification,
    re.risk_level,
    re.lifecycle_status,
    re.is_active,
    re.reason_category,
    re.distribution_scope,
    re.distribution_states,
    rda.distribution_state_codes,
    re.hazards,
    re.product_upcs,
    re.corrective_action,
    re.consequence_of_defect,
    -- firm rollup
    fr.primary_firm_name,
    coalesce(fr.firm_count, 0)        as firm_count,
    coalesce(fr.firms, '[]'::jsonb)   as firms,
    -- product rollup
    coalesce(pr.product_count, 0)     as product_count,
    pr.product_names,
    pr.models,
    pr.hins,
    -- lifecycle (recall_lifecycle, 1:1 with recall_event)
    rl.first_seen_at,
    rl.last_seen_at,
    rl.edit_count,
    rl.is_currently_active,
    rl.was_ever_retracted,
    -- edit-history flags (recall_event_history)
    coalesce(hr.edit_event_count, 0)  as edit_event_count,
    (hr.source_recall_id is not null) as has_been_edited
from {{ ref('recall_event') }} re
left join firm_rollup fr                    on fr.recall_event_id = re.recall_event_id
left join product_rollup pr                 on pr.recall_event_id = re.recall_event_id
left join {{ ref('recall_lifecycle') }} rl  on rl.recall_event_id = re.recall_event_id
left join history_rollup hr                 on hr.source = re.source
                                           and hr.source_recall_id = re.source_recall_id
left join {{ ref('recall_distribution_area') }} rda on rda.recall_event_id = re.recall_event_id
