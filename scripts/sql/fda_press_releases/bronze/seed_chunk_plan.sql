-- FDA press-release chunked-seed plan: every chunk's --resume-after-event-id, computed
-- straight from bronze (independent of any run's log). The distinct-event set is fixed
-- during the seed, so these boundaries are STABLE — re-run a failed chunk with its OWN
-- resume_after_event_id, and only advance to the next chunk once the current one SUCCEEDS
-- (loads). chunk_max_event_id is logged at each chunk's START, before the long fetch, so a
-- chunk that fails mid-fetch loaded nothing even though its end cursor was already printed.
--
-- Set :chunk to your --limit, then run:
--   set -a && . .env && set +a
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -v chunk=5000 -f scripts/sql/fda_press_releases/bronze/seed_chunk_plan.sql
--
-- For each row, run (omit --resume-after-event-id for chunk 1, where it is NULL):
--   recalls deep-rescan fda_press_releases \
--     --resume-after-event-id <resume_after_event_id> --limit <chunk> --change-type historical_seed

\set ON_ERROR_STOP on

with ev as (
    select distinct recall_event_id
    from fda_recalls_bronze
    where recall_event_id is not null
),
numbered as (
    select
        recall_event_id,
        row_number() over (order by recall_event_id) as rn
    from ev
),
chunks as (
    select
        (rn - 1) / :chunk    as chunk_index,
        max(recall_event_id) as last_id,
        count(*)             as events_in_chunk
    from numbered
    group by (rn - 1) / :chunk
)
select
    chunk_index + 1                           as chunk_number,
    lag(last_id) over (order by chunk_index)  as resume_after_event_id,  -- NULL for chunk 1
    events_in_chunk
from chunks
order by chunk_index;
