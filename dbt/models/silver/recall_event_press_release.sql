{{ config(materialized='table') }}

-- FDA press releases — event-grain child of recall_event, M:1 (an event can carry
-- several press releases). The repo's first per-record-enrichment child fact
-- (capture-expansion (b) PR, Part C). FDA-only today; the source is lookup-endpoint-only
-- (the bulk POST 406s it, Finding K0). Joined to recall_event via recall_event_id =
-- md5('FDA' || '|' || source_recall_id) — source_recall_id here is RECALLEVENTID, matching
-- recall_event's FDA branch (md5('FDA' || '|' || recall_event_id::text)).
--
-- Grain: one row per (event, press_release_url). recall_event_press_release_id is the
-- md5 surrogate over that pair (unique because stg_fda_press_releases is latest-per-pair).

select
    md5('FDA' || '|' || source_recall_id || '|' || press_release_url)
        as recall_event_press_release_id,
    md5('FDA' || '|' || source_recall_id)  as recall_event_id,
    press_release_url                      as url,
    press_release_type                     as release_type,
    press_release_issued_dt                as issued_at
from {{ ref('stg_fda_press_releases') }}
