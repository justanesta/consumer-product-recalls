-- Sampling probe (step 1 of 2) — draw a STRATIFIED RANDOM sample of recall_event_id from
-- the work-list, stratified by recall_initiation_dt year-band, and emit it as a CSV the
-- paced sampler script (step 2) will read. Purpose: estimate the total press-release count
-- and the yield-by-stratum CHEAPLY (~1% of events, paced) BEFORE committing to the ~17h
-- full sweep. A stratified estimate tells us, per date band, what fraction of events carry
-- a PR — which directly validates (or kills) any date-floor bounding strategy.
--
-- WHY STRATIFY BY recall_initiation_dt (not id, not event_lmd):
--   * id-order is a known-bad recency proxy (probe_worklist_date_distribution.sql Q5).
--   * event_lmd has NULL/drift defects (Finding H / M).
--   * recall_initiation_dt is the real announcement date — the band a PR's recency tracks.
--   Events with NULL/typo initiation dates get their own 'unknown' stratum so the legacy-PR
--   cohort (where the seed's only PR lives) is sampled too, not silently excluded.
--
-- WHY A PROPORTIONAL ~1% SAMPLE PER STRATUM:
--   Proportional allocation keeps the overall estimate unbiased and lets us scale each
--   stratum's observed yield back up by that stratum's true size (carried in the CSV) to
--   get a total-PR point estimate + per-stratum confidence. Bump :sample_pct for tighter
--   intervals on rare-yield strata (early bands may need a higher rate to see any PRs).
--
-- DETERMINISM: setseed makes the draw reproducible (re-run gives the SAME sample, so a
-- crashed sampler can resume against an identical id list). Change :seed for a fresh draw.
--
-- Usage (user runs — writes the CSV the Python sampler reads):
--   set -a && . .env && set +a
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -v sample_pct=1.0 -v seed=0.42 \
--     -A -F',' --no-align --pset footer=off \
--     -f scripts/sql/fda_press_releases/bronze/sample_worklist_stratified.sql \
--     -o data/exploratory/fda_press_releases/pr_sample_event_ids.csv
--   # (data/exploratory/** is gitignored — per the dump-full-results convention)
--
-- CSV columns: recall_event_id, stratum, stratum_size, sample_pct
--   - stratum:      the date band the event was drawn from
--   - stratum_size: TRUE number of work-list events in that band (the scale-up weight)
--   - sample_pct:   the requested rate (echoed for the estimator's bookkeeping)

\set ON_ERROR_STOP on

-- Make the random draw reproducible for this session. Use \gset so setseed's result row is
-- captured into a throwaway psql var instead of being written to the -o CSV: a bare
-- `select setseed(...);` emits a spurious first row that corrupts the CSV header (the sampler's
-- csv.DictReader then reads "setseed" as the only column). \gset runs the query silently.
select setseed(:seed) as _seed_applied \gset

with ev as (
    select
        recall_event_id,
        max(recall_initiation_dt) as init
    from fda_recalls_bronze
    where recall_event_id is not null
    group by recall_event_id
),
stratified as (
    select
        recall_event_id,
        case
            when init is null                       then 'unknown'
            when init <  '1940-01-01'               then 'unknown'   -- dropped-century typos
            when init <  '2012-01-01'               then 'pre_2012'
            when init <  '2018-01-01'               then '2012_2017'
            when init <  '2022-10-25'               then '2018_pre_oct2022'
            else                                          'post_2022_10_25'
        end as stratum
    from ev
),
sized as (
    select
        recall_event_id,
        stratum,
        count(*) over (partition by stratum) as stratum_size
    from stratified
)
select
    recall_event_id,
    stratum,
    stratum_size,
    :sample_pct as sample_pct
from sized
where random() < (:sample_pct / 100.0)
order by stratum, recall_event_id;
