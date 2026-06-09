{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
    ]
) }}

-- recall_lifecycle — per-recall lifecycle summary (ADR 0026 / Phase 6c.2). One row per
-- recall_event (driven from ref('recall_event'), so the grain + per-source identity match
-- exactly: FDA event, NHTSA campno, USDA English, USCG announced-not-null).
--
-- Two tiers of dimensions:
--   * first_seen_at / last_seen_at / edit_count — manifest-INDEPENDENT, all five sources,
--     from bronze (extraction_timestamp + content_hash). NOTE: post-6a.5-reseed these are
--     bounded by the reseed (bronze history was wiped), so first_seen_at is "first seen by
--     OUR pipeline since the reseed", NOT the recall's true age — use recall_event.announced_at
--     for age. Never uses last_modified_date (ADR 0026 Phase-5c addendum: unreliable per-edit).
--     edit_count = distinct content versions observed (1 = seen, never changed); for the
--     multi-row sources (FDA products, NHTSA 11-tuple lines) it counts version diversity
--     across the event's child rows — a proxy for event activity.
--   * is_currently_active / was_ever_retracted — need the presence manifest
--     (extraction_run_identities). USDA + NHTSA (the track_presence sources, C16); NULL for
--     CPSC/FDA/USCG. NHTSA presence is GATED to full-enumerating runs (change_type='historical_seed')
--     — routine runs' manifests are partial (bronze dedup hides unchanged campnos), so NHTSA
--     presence stays NULL until a deep-rescan/seed banks a complete manifest (WS-H). It is OBSERVED
--     feed presence, not an authoritative agency retraction (v2). See ADR 0026.
--
-- Presence dims key on the LATEST ENUMERATING run (a 304 succeeds but enumerates nothing, so
-- latest-success would read as empty — proven 2026-06-06) and trim() the manifest's
-- source_recall_id to silver's canonical form (Finding R; also folds the one pre-trim-fix run
-- into the canonical identity).

with cpsc_stats as (
    select
        'CPSC'                          as source,
        source_recall_id,
        min(extraction_timestamp)       as first_seen_at,
        max(extraction_timestamp)       as last_seen_at,
        count(distinct content_hash)    as edit_count
    from {{ source('cpsc', 'cpsc_recalls_bronze') }}
    group by source_recall_id
),

fda_stats as (
    select
        'FDA'                           as source,
        recall_event_id::text           as source_recall_id,
        min(extraction_timestamp)       as first_seen_at,
        max(extraction_timestamp)       as last_seen_at,
        count(distinct content_hash)    as edit_count
    from {{ source('fda', 'fda_recalls_bronze') }}
    group by recall_event_id
),

usda_stats as (
    -- English only (mirrors recall_event / staging) + trim (Finding R) to align with the
    -- recall_event identity.
    select
        'USDA'                          as source,
        trim(source_recall_id)          as source_recall_id,
        min(extraction_timestamp)       as first_seen_at,
        max(extraction_timestamp)       as last_seen_at,
        count(distinct content_hash)    as edit_count
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
    where langcode = 'English'
    group by trim(source_recall_id)
),

nhtsa_stats as (
    select
        'NHTSA'                         as source,
        campno                          as source_recall_id,
        min(extraction_timestamp)       as first_seen_at,
        max(extraction_timestamp)       as last_seen_at,
        count(distinct content_hash)    as edit_count
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    group by campno
),

uscg_stats as (
    select
        'USCG'                          as source,
        source_recall_id,
        min(extraction_timestamp)       as first_seen_at,
        max(extraction_timestamp)       as last_seen_at,
        count(distinct content_hash)    as edit_count
    from {{ source('uscg', 'uscg_recalls_bronze') }}
    group by source_recall_id
),

bronze_stats as (
    select * from cpsc_stats
    union all select * from fda_stats
    union all select * from usda_stats
    union all select * from nhtsa_stats
    union all select * from uscg_stats
),

-- USDA presence-manifest dims --------------------------------------------------------------

usda_enum_runs as (
    -- runs that actually wrote USDA manifest rows (enumerating runs), with their time.
    select
        eri.run_id,
        max(er.started_at) as started_at
    from {{ source('pipeline', 'extraction_run_identities') }} eri
    join {{ source('pipeline', 'extraction_runs') }} er on er.run_id = eri.run_id
    where eri.source = 'usda'
    group by eri.run_id
),

usda_latest_run as (
    select run_id from usda_enum_runs order by started_at desc limit 1
),

usda_presence as (
    select distinct
        trim(eri.source_recall_id)  as source_recall_id,
        eri.run_id,
        r.started_at
    from {{ source('pipeline', 'extraction_run_identities') }} eri
    join usda_enum_runs r on r.run_id = eri.run_id
    where eri.source = 'usda'
      and eri.langcode = 'English'
),

usda_presence_agg as (
    select
        source_recall_id,
        bool_or(run_id = (select run_id from usda_latest_run)) as is_currently_active,
        count(distinct run_id)                                 as present_runs,
        min(started_at)                                        as first_present
    from usda_presence
    group by source_recall_id
),

usda_lifecycle as (
    select
        source_recall_id,
        is_currently_active,
        -- present in fewer enum runs than exist since first appearance ⇒ absent in some run
        -- (a mid-lifespan toggle OR an end retraction).
        present_runs < (
            select count(*) from usda_enum_runs er where er.started_at >= a.first_present
        ) as was_ever_retracted
    from usda_presence_agg a
),

-- NHTSA presence-manifest dims (C16) --------------------------------------------------------
-- IDENTICAL shape to USDA, with ONE critical gate: only FULL-enumerating runs
-- (change_type='historical_seed' — the deep-rescan/seed) write a COMPLETE NHTSA manifest. Routine
-- cron runs are incremental, so only changed campnos reach _passing_records (the manifest source)
-- and their manifest is PARTIAL — counting them would falsely read most campnos as "absent". Gating
-- to historical_seed means: until a full NHTSA deep-rescan/seed runs (WS-H H-b), nhtsa_enum_runs is
-- EMPTY → every NHTSA presence dim is NULL, never wrong. (USDA needs no gate — it full-dumps each
-- run.) The signal is OBSERVED feed presence (campno dropped from / present in the source pull), NOT
-- an authoritative agency retraction — interpreting "withdrawn on date X" is v2 (ADR 0026).
nhtsa_enum_runs as (
    select eri.run_id, max(er.started_at) as started_at
    from {{ source('pipeline', 'extraction_run_identities') }} eri
    join {{ source('pipeline', 'extraction_runs') }} er on er.run_id = eri.run_id
    where eri.source = 'nhtsa' and er.change_type = 'historical_seed'
    group by eri.run_id
),

nhtsa_latest_run as (
    select run_id from nhtsa_enum_runs order by started_at desc limit 1
),

nhtsa_presence as (
    select distinct eri.source_recall_id, eri.run_id, r.started_at
    from {{ source('pipeline', 'extraction_run_identities') }} eri
    join nhtsa_enum_runs r on r.run_id = eri.run_id
    where eri.source = 'nhtsa'
),

nhtsa_presence_agg as (
    select
        source_recall_id,
        bool_or(run_id = (select run_id from nhtsa_latest_run)) as is_currently_active,
        count(distinct run_id)                                 as present_runs,
        min(started_at)                                        as first_present
    from nhtsa_presence
    group by source_recall_id
),

nhtsa_lifecycle as (
    select
        source_recall_id,
        is_currently_active,
        present_runs < (
            select count(*) from nhtsa_enum_runs er where er.started_at >= a.first_present
        ) as was_ever_retracted
    from nhtsa_presence_agg a
)

select
    re.recall_event_id,
    re.source,
    re.source_recall_id,
    bs.first_seen_at,
    bs.last_seen_at,
    bs.edit_count,
    coalesce(ul.is_currently_active, nl.is_currently_active) as is_currently_active,
    coalesce(ul.was_ever_retracted, nl.was_ever_retracted)  as was_ever_retracted
from {{ ref('recall_event') }} re
left join bronze_stats bs
    on bs.source = re.source
   and bs.source_recall_id = re.source_recall_id
left join usda_lifecycle ul
    on re.source = 'USDA'
   and ul.source_recall_id = re.source_recall_id
left join nhtsa_lifecycle nl
    on re.source = 'NHTSA'
   and nl.source_recall_id = re.source_recall_id
