-- Phase 3 — CPSC bronze data exploration after first live extraction.
--
-- When to run: after `recalls extract cpsc` lands rows in cpsc_recalls_bronze.
-- Read top-down; output is the source-of-record for
-- documentation/cpsc/first_extraction_findings.md (sections A–I cite these
-- queries by number).
--
-- Each numbered \echo block is one of the 12 queries called out in the
-- findings doc. Output can be piped into the doc by running:
--   psql ... -f scripts/sql/cpsc/bronze/explore_bronze_shape.sql
-- and pasting the output back to the analysis.

\echo '=== Q1: overall row count and date range ==='
-- Establishes the extraction window and confirms the watermark field used.
select count(*), min(last_publish_date), max(last_publish_date)
from cpsc_recalls_bronze;

\echo ''
\echo '=== Q2: weekly cadence ==='
-- Publication rhythm week-by-week; reveals holiday gaps and seasonal patterns.
select
  date_trunc('week', last_publish_date)::date as week_start,
  count(*) as records,
  count(distinct last_publish_date::date) as active_days
from cpsc_recalls_bronze
group by date_trunc('week', last_publish_date)
order by week_start;

\echo ''
\echo '=== Q3: records per day ==='
-- Detailed daily view; used to identify spikes and confirm no weekend activity.
select last_publish_date::date as day, count(*) as records
from cpsc_recalls_bronze
group by last_publish_date::date
order by last_publish_date::date;

\echo ''
\echo '=== Q4: edit detection — recall_ids with multiple distinct content hashes ==='
-- A non-empty result indicates a record was re-fetched with changed content
-- (the content hash dedup mechanism captured an edit). Zero rows = no edits
-- detected during the extraction window.
select recall_id, count(distinct content_hash) as hash_versions, count(*) as total_rows
from cpsc_recalls_bronze
group by recall_id
having count(distinct content_hash) > 1
order by hash_versions desc;

\echo ''
\echo '=== Q5: total rows vs unique recall_ids (dedup summary) ==='
-- Confirms whether source_recall_id is truly unique in bronze or whether
-- multi-version rows exist from edit detection.
select
  count(*) as total_rows,
  count(distinct recall_id) as unique_recall_ids,
  count(*) - count(distinct recall_id) as apparent_duplicates
from cpsc_recalls_bronze;

\echo ''
\echo '=== Q6: null rates for scalar fields ==='
-- Identifies which fields are reliably populated vs. optional. Critical input
-- for deciding which silver columns can be NOT NULL.
select
  round(100.0 * sum(case when title is null then 1 else 0 end) / count(*), 1) as pct_null_title,
  round(100.0 * sum(case when recall_date is null then 1 else 0 end) / count(*), 1) as pct_null_recall_date,
  round(100.0 * sum(case when description is null then 1 else 0 end) / count(*), 1) as pct_null_description,
  round(100.0 * sum(case when url is null then 1 else 0 end) / count(*), 1) as pct_null_url,
  round(100.0 * sum(case when consumer_contact is null then 1 else 0 end) / count(*), 1) as pct_null_consumer_contact,
  round(100.0 * sum(case when sold_at_label is null then 1 else 0 end) / count(*), 1) as pct_null_sold_at_label,
  round(100.0 * sum(case when product_upcs is null then 1 else 0 end) / count(*), 1) as pct_null_upcs,
  round(100.0 * sum(case when injuries is null then 1 else 0 end) / count(*), 1) as pct_null_injuries
from cpsc_recalls_bronze;

\echo ''
\echo '=== Q7: JSONB field empty-array rates ==='
-- Checks whether JSONB arrays are populated, not just non-null. A non-null
-- but empty array (e.g. manufacturers = '[]') means the field exists
-- structurally but carries no data — different from a null field.
select
  round(100.0 * sum(case when products is null or products = '[]'::jsonb then 1 else 0 end) / count(*), 1) as pct_empty_products,
  round(100.0 * sum(case when hazards is null or hazards = '[]'::jsonb then 1 else 0 end) / count(*), 1) as pct_empty_hazards,
  round(100.0 * sum(case when remedies is null or remedies = '[]'::jsonb then 1 else 0 end) / count(*), 1) as pct_empty_remedies,
  round(100.0 * sum(case when manufacturers is null or manufacturers = '[]'::jsonb then 1 else 0 end) / count(*), 1) as pct_empty_manufacturers,
  round(100.0 * sum(case when retailers is null or retailers = '[]'::jsonb then 1 else 0 end) / count(*), 1) as pct_empty_retailers,
  round(100.0 * sum(case when manufacturer_countries is null or manufacturer_countries = '[]'::jsonb then 1 else 0 end) / count(*), 1) as pct_empty_mfr_countries
from cpsc_recalls_bronze;

\echo ''
\echo '=== Q8: products per recall (confirms 1:1 model) ==='
-- FDA has 1-to-many; this query confirms CPSC always has exactly 1 product
-- per row.
select
  jsonb_array_length(products) as product_count,
  count(*) as recalls
from cpsc_recalls_bronze
where products is not null
group by jsonb_array_length(products)
order by product_count;

\echo ''
\echo '=== Q9: HazardType population check ==='
-- Validates the finding that HazardType is always an empty string despite
-- hazards arrays being non-empty. Used to assess silver filter viability.
select
  count(*) as total_recalls,
  sum(case when hazards != '[]'::jsonb then 1 else 0 end) as recalls_with_hazards,
  sum(case when hazards != '[]'::jsonb and (hazards->0->>'HazardType') != '' then 1 else 0 end) as recalls_with_hazard_type
from cpsc_recalls_bronze;

\echo ''
\echo '=== Q10: top spike days ==='
-- Identifies the highest-volume single days; used to check for anomalies
-- vs. expected publication patterns.
select last_publish_date::date as day, count(*) as records
from cpsc_recalls_bronze
group by last_publish_date::date
order by records desc
limit 5;

\echo ''
\echo '=== Q11: weekday gap analysis ==='
-- Finds weekdays with zero CPSC activity. Expected gaps = US federal
-- holidays. Unexpected gaps may indicate pipeline failures or API outages.
with date_series as (
  select generate_series(min(last_publish_date)::date, max(last_publish_date)::date, '1 day'::interval)::date as day
  from cpsc_recalls_bronze
),
active_days as (
  select distinct last_publish_date::date as day from cpsc_recalls_bronze
)
select d.day, to_char(d.day, 'Day') as day_name
from date_series d
left join active_days a on d.day = a.day
where a.day is null and extract(dow from d.day) not in (0, 6)
order by d.day;

\echo ''
\echo '=== Q12: extraction run history (cross-table — extraction_runs) ==='
-- Confirms pipeline runs were recorded with correct counts and status. Note:
-- runs prior to the extraction_runs fix (feature/fda-first-extraction) will
-- not appear here.
select source, status, records_extracted, records_inserted, records_rejected,
  started_at, extract(epoch from (finished_at - started_at))::int as duration_seconds
from extraction_runs
where source = 'cpsc'
order by started_at;
