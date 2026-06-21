{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
      {'columns': ['source', 'event_date']},
      {'columns': ['is_active']},
      {'columns': ['classification']},
      {'columns': ['distribution_state_codes'], 'type': 'gin'},
      {'columns': ['distribution_country_codes'], 'type': 'gin'},
      {'columns': ['search_vector'], 'type': 'gin'},
      {'columns': ['firms'], 'type': 'gin'},
    ],
    post_hook=[
      "drop index if exists {{ this.schema }}.{{ this.name }}_event_date_desc_evt",
      "create index {{ this.name }}_event_date_desc_evt
       on {{ this }} (event_date desc, recall_event_id)",
      "analyze {{ this }}",
    ]
) }}

-- R2 keyset index post_hook uses DROP-THEN-CREATE (not `create index if not exists`): Postgres index
-- names are unique per SCHEMA, so a fixed-name `if not exists` collides with the same-named index still
-- attached to dbt's `__dbt_backup` table mid-rebuild, no-ops, and is then dropped with the backup — so the
-- index OSCILLATED out every other build (gold-audit G0, root-caused 2026-06-15). drop-if-exists frees the
-- stale backup name first, so the index rebuilds deterministically every run.
--
-- The keyset index (and the (source, event_date) composite for the ?source= path) is built on EVENT_DATE
-- = coalesce(announced_at, published_at), the announce-recency feed sort key (ADR 0038 §2026-W26). It was
-- (published_at desc, recall_event_id) through 2026-W25; published_at stayed the keyset key while the
-- time-series facts had already moved to announce-bucketing, which surfaced long-dormant recalls that got
-- one minor agency edit at the top of the feed (a 2000 recall re-published days ago outranking genuinely
-- newer ones). event_date is non-null by construction (published_at is the NOT-NULL fallback for the ~20
-- FDA with no announce date), so the seek WHERE stays totally ordered — no NULLs-last hazard, no data loss.
--
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
        string_agg(distinct product_name, ' ') filter (where product_name is not null) as product_names_text,
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
    -- Non-null announce-recency feed sort key (ADR 0038 §2026-W26). Mirrors the fct_* time-series basis
    -- (coalesce(announced_at, published_at)): the TRUE event date where known, falling back to the
    -- NOT-NULL published_at for the ~20 FDA with no trustworthy announce date. Backs the (event_date desc,
    -- recall_event_id) keyset index above; the recalls-api paginates GET /recalls on it.
    coalesce(re.announced_at, re.published_at) as event_date,
    re.classification,
    re.risk_level,
    re.lifecycle_status,
    re.is_active,
    re.reason_category,
    re.distribution_scope,
    re.distribution_states,
    rda.distribution_state_codes,
    rda.distribution_country_codes,
    re.hazards,
    re.product_upcs,
    re.corrective_action,
    re.consequence_of_defect,
    -- firm rollup
    fr.primary_firm_name,
    coalesce(fr.firm_count, 0)        as firm_count,
    -- `firms` is GIN-indexed via config(indexes) above (hash-named, oscillation-immune) so the recalls-api's
    -- GET /recalls?firm_id={id} filter resolves as an index-backed jsonb containment match
    -- (`firms @> '[{"firm_id": :id}]'`) instead of a seq-scan of the corpus. jsonb_ops serves `@>`; a tighter
    -- jsonb_path_ops opclass is a Phase-7 re-profile option (needs a post_hook — config(indexes) can't set one).
    coalesce(fr.firms, '[]'::jsonb)   as firms,
    -- product rollup (O1: coalesce the array rollups to '[]' for serving-layer consistency with `firms`)
    coalesce(pr.product_count, 0)     as product_count,
    coalesce(pr.product_names, '[]'::jsonb) as product_names,
    coalesce(pr.models,        '[]'::jsonb) as models,
    coalesce(pr.hins,          '[]'::jsonb) as hins,
    -- lifecycle (recall_lifecycle, 1:1 with recall_event)
    rl.first_seen_at,
    rl.last_seen_at,
    rl.edit_count,
    rl.is_currently_active,
    rl.was_ever_retracted,
    -- edit-history flags (recall_event_history)
    coalesce(hr.edit_event_count, 0)  as edit_event_count,
    (hr.source_recall_id is not null) as has_been_edited,
    -- Option B (gold-audit 2026-06-15): recall-grain FTS vector for GET /recalls/search (v1.1, API-side).
    -- field->setweight-bucket is the gold contract; the API tunes the NUMERIC {D,C,B,A} weights at query
    -- time via ts_rank_cd (no rebuild). 4 buckets, 1:1, so a real brand/product match can outrank a
    -- narrative-only mention: A=title; B=what/who (product names + primary firm); C=why (recall_reason);
    -- D=harm tail (consequence_of_defect). corrective_action + hazards excluded (boilerplate / opaque jsonb).
    -- GIN via config(indexes) above (hash-named, immune to the post_hook oscillation noted at the top).
    (
        setweight(to_tsvector('english', coalesce(re.title, '')), 'A')
        || setweight(to_tsvector('english',
               coalesce(pr.product_names_text, '') || ' '
            || coalesce(fr.primary_firm_name, '')), 'B')
        || setweight(to_tsvector('english', coalesce(re.recall_reason, '')), 'C')
        || setweight(to_tsvector('english', coalesce(re.consequence_of_defect, '')), 'D')
    )                                 as search_vector
from {{ ref('recall_event') }} re
left join firm_rollup fr                    on fr.recall_event_id = re.recall_event_id
left join product_rollup pr                 on pr.recall_event_id = re.recall_event_id
left join {{ ref('recall_lifecycle') }} rl  on rl.recall_event_id = re.recall_event_id
left join history_rollup hr                 on hr.source = re.source
                                           and hr.source_recall_id = re.source_recall_id
left join {{ ref('recall_distribution_area') }} rda on rda.recall_event_id = re.recall_event_id
