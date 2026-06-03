-- Validation queries for the FDA press-release (Tier-3) extractor + historical seed.
-- Capture-expansion (b) PR, Part C. Run after the deep-rescan seed and `dbt build`.
--
-- Spans the seed source (fda_recalls_bronze), the press-release bronze
-- (fda_press_releases_bronze), silver (recall_event_press_release), and the operational
-- tables (extraction_runs, source_watermarks). Run the whole file, or copy a single Q.
--
-- Usage (user runs):
--   set -a && . .env && set +a
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/fda_press_releases/bronze/validate_seed.sql

\set ON_ERROR_STOP on

-- Q1 — Event range + total (chunk planning: ids run min..max over ~N distinct events).
select
    'Q1 event range'                  as q,
    min(recall_event_id)              as min_id,
    max(recall_event_id)              as max_id,
    count(distinct recall_event_id)   as distinct_events
from fda_recalls_bronze
where recall_event_id is not null;

-- Q2 — Eyeball the most-recently-landed press releases (field-mapping sanity:
--      source_recall_id = the event id, a real URL, a type, a date or null).
select
    source_recall_id,
    press_release_url,
    press_release_type,
    press_release_issued_dt
from fda_press_releases_bronze
order by extraction_timestamp desc
limit 10;

-- Q3 — Prevalence: total press releases and the distinct events that carry at least one.
select
    'Q3 prevalence'                       as q,
    count(*)                              as press_releases,
    count(distinct source_recall_id)      as events_with_pr
from fda_press_releases_bronze;

-- Q4 — M:1 grain: events carrying multiple press releases (validates the child grain).
select
    source_recall_id,
    count(*) as n_press_releases
from fda_press_releases_bronze
group by source_recall_id
having count(*) > 1
order by n_press_releases desc
limit 10;

-- Q5 — change_type on the seed runs (the seed chunks should be `historical_seed`).
select
    change_type,
    count(*) as runs
from extraction_runs
where source = 'fda_press_releases'
group by change_type
order by runs desc;

-- Q6 — Watermark is NOT advanced by the deep-rescan (the incremental path owns it):
--      last_cursor stays NULL until the first incremental `recalls extract` run.
select
    source,
    last_cursor,
    last_successful_extract_at
from source_watermarks
where source = 'fda_press_releases';

-- Q7 — Silver child populated + FK integrity: every press release's event resolves to
--      recall_event (orphans must be 0 — the dbt relationships test enforces this too).
select
    'Q7 silver' as q,
    (select count(*) from recall_event_press_release) as silver_rows,
    (
        select count(*)
        from recall_event_press_release pr
        left join recall_event e on pr.recall_event_id = e.recall_event_id
        where e.recall_event_id is null
    ) as orphans;
