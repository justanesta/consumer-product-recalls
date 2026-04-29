-- Fails if recall_event has fewer than 500 FDA rows.
-- First extraction (2026-04-29) produced 755 unique recall events from a 90-day
-- window; 500 leaves headroom for dedup variance while catching catastrophic failure.
select 'fda_event_count_below_floor' as failure
where (
    select count(*) from {{ ref('recall_event') }} where source = 'FDA'
) < 500
