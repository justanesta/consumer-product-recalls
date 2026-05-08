-- Finding H Q1 closure mechanism — NHTSA update cadence via extraction_runs.
--
-- Background: Finding H Q1 (`documentation/nhtsa/flat_file_observations.md:421-456`)
-- asks how often NHTSA actually publishes new content vs re-stamps idle data.
-- Per Finding H itself (line 432), the wrapper-level watermark probe CANNOT
-- answer this — ZIP wrapper bytes shift every day regardless of inner content
-- (Finding J's non-determinism mandate). Closure requires inner-content
-- hashing across days, which is exactly what `extraction_runs.response_inner_content_sha256`
-- (migration 0011) provides as a free side-effect of every successful flat-file
-- extraction.
--
-- This script is the FOCUSED closure tool. It complements
-- `spot_check_extraction_runs.sql` (which is a multi-purpose forensic check).
-- This one renders only the cadence verdict.
--
-- Run it after every `recalls extract nhtsa`. Verdict closure target: ~7
-- successful run-days, ideally bracketing one real upstream content update.
-- After the verdict closes, this script becomes a passive monitoring tool —
-- run it monthly (or alert-driven via a future Tier 2 / DQ-framework hook
-- per ADR 0031) to catch any change in NHTSA's publication cadence.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/inner_content_cadence.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: cadence summary — distinct inner-content snapshots vs run-days ==='
\echo 'change_rate is the share of consecutive same-day runs where the inner'
\echo 'content actually transitioned (i.e., NHTSA published new content).'
\echo 'Excludes the first run (no prior to compare to) and unsuccessful runs.'

with successful_runs as (
    select
        started_at,
        response_inner_content_sha256
    from extraction_runs
    where source = 'nhtsa'
      and status = 'success'
      and response_inner_content_sha256 is not null
    order by started_at
),
transitions as (
    select
        started_at,
        response_inner_content_sha256,
        lag(response_inner_content_sha256) over (order by started_at) as prior_inner_hash
    from successful_runs
)
select
    count(*)                                                                  as total_runs,
    count(distinct response_inner_content_sha256)                             as distinct_inner_snapshots,
    count(*) filter (where prior_inner_hash is null)                          as first_runs,
    count(*) filter (where prior_inner_hash = response_inner_content_sha256)  as idle_transitions,
    count(*) filter (where prior_inner_hash is not null
                       and prior_inner_hash <> response_inner_content_sha256) as content_change_transitions,
    case
        when count(*) filter (where prior_inner_hash is not null) = 0 then null
        else round(
            100.0 * count(*) filter (where prior_inner_hash is not null
                                       and prior_inner_hash <> response_inner_content_sha256)
            / nullif(count(*) filter (where prior_inner_hash is not null), 0),
            2
        )
    end                                                                       as content_change_rate_pct
from transitions;

\echo
\echo '=== Q2: per-run detail — inner-hash transition log ==='
\echo 'CHANGED rows are the events that prove content updates happen.'
\echo 'unchanged rows confirm idle days (NHTSA re-stamped wrapper without new content).'

select
    started_at,
    finished_at,
    change_type,
    records_inserted,
    left(response_inner_content_sha256, 16)                              as inner_hash,
    case
        when lag(response_inner_content_sha256) over (order by started_at)
             = response_inner_content_sha256                              then 'unchanged'
        when lag(response_inner_content_sha256) over (order by started_at) is null
                                                                          then 'first_run'
        else 'CHANGED'
    end                                                                  as inner_transition
from extraction_runs
where source = 'nhtsa'
  and status = 'success'
  and response_inner_content_sha256 is not null
order by started_at;

\echo
\echo '=== Q3: per-day aggregation — multi-run-per-day cases ==='
\echo 'If multiple runs land on the same UTC day, do they see the same inner hash?'
\echo 'Multi-hash days = NHTSA published mid-day OR our extractor caught both sides'
\echo 'of a regen window. Either way it is a real cadence signal.'

select
    started_at::date                                              as run_date,
    count(*)                                                      as runs_that_day,
    count(distinct response_inner_content_sha256)                 as distinct_inner_hashes_that_day,
    count(distinct response_inner_content_sha256) > 1             as content_changed_within_day
from extraction_runs
where source = 'nhtsa'
  and status = 'success'
  and response_inner_content_sha256 is not null
group by run_date
order by run_date;

\echo
\echo '=== Verdict guidance ==='
\echo 'Sufficient evidence to close Finding H Q1: ≥4-5 distinct run-days with'
\echo 'at least one content_change_transition. Update flat_file_observations.md'
\echo 'Finding H Q1 status from "Deferred" → "Confirmed [pattern] YYYY-MM-DD"'
\echo 'and document the operational implication for ADR 0010 cron cadence.'
\echo
\echo 'Insufficient evidence: 1-3 run-days. Add a "Preliminary observation"'
\echo 'subsection to Finding H Q1 with what data we have and revisit.'
