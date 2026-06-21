{{ config(
    materialized='table',
    meta={'index_specs': [
      {'suffix': 'recall_event_id', 'cols': 'recall_event_id', 'unique': True},
    ]}
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
--     CPSC/FDA/USCG. NHTSA presence is GATED to the full-corpus deep-rescan runs — those written by
--     NhtsaDeepRescanLoader (writes_presence_manifest=True, both archives), NOT a change_type filter,
--     so the monthly scheduled deep-rescan refreshes presence even as change_type='routine'; the daily
--     incremental extract pulls POST_2010 only and writes no manifest. NHTSA presence stays NULL until
--     a deep-rescan/seed banks a complete manifest (WS-H). It is OBSERVED
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
-- IDENTICAL shape to USDA. The manifest is keyed on campno (recall-event grain) — NOT the bronze
-- source_recall_id (RECORD_ID, regen-unstable) — and is written ONLY by the deep-rescan loader
-- (NhtsaDeepRescanLoader.writes_presence_manifest=True), because only the deep-rescan enumerates the
-- FULL corpus (both PRE_2010 + POST_2010 archives). The daily incremental extract pulls POST_2010
-- only, so it is NOT a full-corpus snapshot and deliberately writes no manifest — otherwise pre-2010
-- campnos (absent from the daily pull) would read as false retractions. So nhtsa_enum_runs are the
-- deep-rescan runs (no change_type gate — the already-scheduled monthly deep-rescan refreshes
-- presence even as change_type='routine'). Until the first deep-rescan banks a manifest (WS-H H-b),
-- these CTEs are EMPTY → NHTSA presence dims are NULL, never wrong. The signal is OBSERVED feed
-- presence (campno present in / absent from the full-corpus pull), NOT an authoritative agency
-- retraction — interpreting "withdrawn on date X" is v2 (ADR 0026).
nhtsa_enum_runs as (
    -- runs that wrote NHTSA manifest rows = the deep-rescan full-corpus enumerations (the daily
    -- POST_2010 extract does NOT write the manifest — see the block comment above). No change_type
    -- filter: the monthly scheduled deep-rescan (change_type='routine') refreshes presence too.
    select eri.run_id, max(er.started_at) as started_at
    from {{ source('pipeline', 'extraction_run_identities') }} eri
    join {{ source('pipeline', 'extraction_runs') }} er on er.run_id = eri.run_id
    where eri.source = 'nhtsa'
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
