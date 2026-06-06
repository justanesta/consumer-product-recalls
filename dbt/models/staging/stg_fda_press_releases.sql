{{ config(materialized='view') }}

-- Latest-per-press-release projection over the FDA Tier-3 press-release bronze.
-- Identity is (source_recall_id, press_release_url): source_recall_id = RECALLEVENTID
-- (the event), so press releases are M:1 to the recall event. Multiple bronze rows per
-- identity arise only when a press release's metadata changes between runs (content-hash
-- dedup prevents identical re-ingestion); silver consumes only the latest version.
--
-- nullif(col, '') per ADR 0027: bronze preserves the source's '' / null verbatim; silver
-- normalizes the empty string to null. press_release_issued_dt is already a proper
-- date/null in bronze (storage-forced), so no nullif needed.

with ranked as (
    select
        *,
        row_number() over (
            partition by source_recall_id, press_release_url
            order by extraction_timestamp desc
        ) as rn
    from {{ source('fda', 'fda_press_releases_bronze') }}
)

select
    source_recall_id,
    press_release_url,
    nullif(press_release_type, '')  as press_release_type,
    press_release_issued_dt,
    content_hash,
    extraction_timestamp,
    raw_landing_path
from ranked
where rn = 1
