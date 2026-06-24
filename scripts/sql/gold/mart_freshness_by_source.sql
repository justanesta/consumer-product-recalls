-- Is the gold layer actually rebuilding daily, and is NHTSA UNIQUELY stale?
--
-- transform.yml runs `dbt build` + `dbt snapshot` daily at 03:00 UTC (guarded by
-- the CRON_ENABLED repo var), so mart_recall_summary should be rebuilt every day.
-- If gold_meta.rebuilt_at is days old, the transform cron is not firing (or is
-- failing) and EVERY source is frozen — which would mean the "NHTSA gap" is just
-- the most-noticed symptom of a stalled transform, not an NHTSA issue at all.
--
-- Q2 contrasts max(event_date) across all five sources: if only NHTSA's is ~7
-- days back while CPSC/FDA/USDA/USCG advanced within the last day or two, the
-- transform is healthy and the gap is genuinely NHTSA cadence. If all five are
-- stuck at the same old date, it is a transform-freshness problem.
--
-- event_date = coalesce(announced_at, published_at) (mart_recall_summary, the
-- announce-recency feed sort key, ADR 0038 §2026-W26).
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/gold/mart_freshness_by_source.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: gold rebuild stamp (is the transform cron running?) ==='
\echo 'age should be < ~24h on a healthy daily cron. Days-old = transform stalled'
\echo '(CRON_ENABLED off, or the daily Transform workflow failing) -> ALL sources'
\echo 'frozen, not just NHTSA.'

select
    rebuilt_at,
    now() - rebuilt_at        as age,
    schema_version
from gold_meta;

\echo
\echo '=== Q2: per-source freshness in mart_recall_summary ==='
\echo 'If only NHTSA days_since_max_event is ~7 and the others are 0-2, transform is'
\echo 'healthy and the NHTSA gap is upstream cadence. If every source is stuck at the'
\echo 'same date, the transform itself stopped advancing.'

select
    source,
    count(*)                                       as n_recalls,
    max(event_date)::date                          as max_event_date,
    current_date - max(event_date)::date           as days_since_max_event,
    max(announced_at)::date                        as max_announced,
    max(published_at)::date                        as max_published
from mart_recall_summary
group by source
order by source;
