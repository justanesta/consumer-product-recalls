-- Phase 5d Step 3 — first-extraction survey of uscg_recalls_bronze.
--
-- Context: 2026-05-17 historical_seed run (run_id
-- b2444fa6-1b32-4b5f-99e9-d75662bc3e5c) extracted 1763, loaded 1512 to
-- bronze, quarantined 251 (33 validate + 218 invariants). 14.2% rejection
-- rate exceeded the 5% threshold so the run was marked aborted in
-- extraction_runs — BUT the bronze + rejected rows DID persist (the
-- ExtractionAbortedError fires AFTER load_bronze commits).
--
-- This script is the broad-strokes survey. For deeper failure-mode
-- analysis use ``diagnose_rejections.sql`` (companion script).
--
-- Run with no args; defaults to the most recent uscg extraction run.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: run summary from extraction_runs ==='
\echo 'Most recent uscg run, with rejection breakdown.'

select
    run_id,
    status,
    change_type,
    started_at,
    finished_at - started_at as duration,
    records_extracted,
    records_inserted,
    records_rejected,
    round((records_rejected::numeric / nullif(records_extracted, 0)) * 100, 2) as rejection_rate_pct,
    response_status_code,
    response_etag,
    response_last_modified,
    raw_landing_path
from extraction_runs
where source = 'uscg'
order by started_at desc
limit 5;

\echo
\echo '=== Q2: bronze row count vs extracted (sanity) ==='
\echo 'bronze_rows + rejected_rows should equal records_extracted from Q1.'

with bronze_count as (
    select count(*) as n from uscg_recalls_bronze
),
rejected_count as (
    select count(*) as n from uscg_recalls_rejected
)
select
    (select n from bronze_count) as bronze_rows,
    (select n from rejected_count) as rejected_rows,
    (select n from bronze_count) + (select n from rejected_count) as total_observed;

\echo
\echo '=== Q3: rejection breakdown by failure_stage ==='
\echo 'Two stages to expect: validate_records (Pydantic ValidationError) and'
\echo 'invariants (null_source_id / date_sanity / year_prefix). The split is'
\echo 'the first hint at whether parser-level or business-rule logic is failing.'

select
    failure_stage,
    count(*) as n,
    round(count(*) * 100.0 / sum(count(*)) over (), 2) as share_pct
from uscg_recalls_rejected
group by failure_stage
order by n desc;

\echo
\echo '=== Q4: top failure_reason patterns ==='
\echo 'Group by first 80 chars of failure_reason to cluster similar errors.'
\echo 'A long-tail-flat distribution = many distinct failures; a few dominant'
\echo 'reasons = one or two systemic issues.'

select
    failure_stage,
    left(failure_reason, 80) as reason_prefix,
    count(*) as n
from uscg_recalls_rejected
group by failure_stage, reason_prefix
order by n desc, failure_stage, reason_prefix
limit 20;

\echo
\echo '=== Q5: bronze field-population rates ==='
\echo 'Per-column NULL rate across the 1,512 bronze rows. Compare against the'
\echo 'Step 1 sample (Finding A) populated-rates table — divergence calibrates'
\echo 'expectations for Step 1.5 corpus probe.'

select
    count(*) as bronze_rows,
    count(*) filter (where opened_on is null) as null_opened_on,
    count(*) filter (where mic is null) as null_mic,
    count(*) filter (where model_name is null) as null_model_name,
    count(*) filter (where problem_1 is null) as null_problem_1,
    count(*) filter (where company_official is null) as null_company_official,
    count(*) filter (where model_year is null) as null_model_year,
    count(*) filter (where problem_2 is null) as null_problem_2,
    count(*) filter (where hin is null) as null_hin,
    count(*) filter (where case_open_date is null) as null_case_open_date,
    count(*) filter (where disposition is null) as null_disposition,
    count(*) filter (where case_close_date is null) as null_case_close_date,
    count(*) filter (where units is null) as null_units,
    count(*) filter (where boat_type is null) as null_boat_type,
    count(*) filter (where campaign_open_date is null) as null_campaign_open_date,
    count(*) filter (where campaign_close_date is null) as null_campaign_close_date,
    count(*) filter (where severity is null) as null_severity,
    count(*) filter (where last_date is null) as null_last_date
from uscg_recalls_bronze;

\echo
\echo '=== Q6: disposition distribution ==='
\echo 'Open vs Closed split. Useful for downstream silver "active recalls" filter.'

select
    coalesce(disposition, '<NULL>') as disposition,
    count(*) as n
from uscg_recalls_bronze
group by disposition
order by n desc;

\echo
\echo '=== Q7: top manufacturers by recall count ==='

select
    coalesce(mic, '<NULL>') as mic,
    coalesce(company_name, '<NULL>') as company_name,
    count(*) as n_recalls
from uscg_recalls_bronze
group by mic, company_name
order by n_recalls desc
limit 20;

\echo
\echo '=== Q8: opened_on year coverage ==='
\echo 'Bins by year. Reveals the historical depth of the corpus.'

select
    extract(year from opened_on)::int as year,
    count(*) as n
from uscg_recalls_bronze
where opened_on is not null
group by year
order by year desc
limit 30;

\echo
\echo '=== Q9: source_recall_id prefix distribution ==='
\echo 'First 2 chars of the recall number. The Step 1 working hypothesis was'
\echo 'that this encodes year (26MF0158 → opened 2026). Step 1.5 will confirm/'
\echo 'invalidate based on whether prefix matches opened_on year corpus-wide.'

select
    left(source_recall_id, 4) as prefix4,
    count(*) as n
from uscg_recalls_bronze
group by prefix4
order by n desc
limit 20;
