-- Phase 5a — FDA bronze data exploration after first live extraction.
--
-- When to run: after `recalls extract fda` lands rows in fda_recalls_bronze.
-- Read top-down; output is the source-of-record for
-- documentation/fda/first_extraction_findings.md (sections A–J cite these
-- queries by number).
--
-- Each numbered \echo block is one of the 16 queries called out in the
-- findings doc. Note: FDA bronze has 1-to-many shape (one recall_event_id
-- can have many products), so total_rows differs from unique_event_ids by
-- design (see Q5/Q6).
--
-- Run with: psql ... -f scripts/sql/fda/bronze/explore_bronze_shape.sql

\echo '=== Q1: overall row count and date range ==='
-- Establishes the extraction window and confirms the watermark field (event_lmd).
select count(*), min(event_lmd), max(event_lmd)
from fda_recalls_bronze;

\echo ''
\echo '=== Q2: records per day (cadence) ==='
-- Reveals publication rhythm, weekend gaps, and spike days. event_lmd is a
-- timestamp; cast to date to group by calendar day.
select event_lmd::date as day, count(*) as records
from fda_recalls_bronze
group by event_lmd::date
order by event_lmd::date;

\echo ''
\echo '=== Q3: weekly cadence summary ==='
-- Aggregates daily counts into weeks to show volume trends and confirm how
-- many days per week FDA publishes.
select
  date_trunc('week', event_lmd)::date as week_start,
  count(*) as records,
  count(distinct event_lmd::date) as active_days
from fda_recalls_bronze
group by date_trunc('week', event_lmd)
order by week_start;

\echo ''
\echo '=== Q4: edit detection — recall_event_ids with multiple distinct content hashes ==='
-- A hash_versions > 1 means the same recall_event_id was re-fetched with
-- changed content — the content hash dedup mechanism captured an edit.
-- hash_versions = total_rows confirms every row for that event has a unique hash.
select recall_event_id, count(distinct content_hash) as hash_versions, count(*) as total_rows
from fda_recalls_bronze
group by recall_event_id
having count(distinct content_hash) > 1
order by hash_versions desc
limit 20;

\echo ''
\echo '=== Q5: total rows vs unique recall identifiers (dedup summary) ==='
-- recall_event_id is NOT unique (one event = many products).
-- source_recall_id (PRODUCTID) is the true dedup key — but even that can have
-- 2 rows if the same product was edited (different content hashes).
select
  count(*) as total_rows,
  count(distinct recall_event_id) as unique_event_ids,
  count(*) - count(distinct recall_event_id) as apparent_duplicates
from fda_recalls_bronze;

\echo ''
\echo '=== Q6: multi-product event detail — rows, hash versions, distinct product IDs ==='
-- Confirms whether multiple rows per event_id are from different products
-- (distinct source_recall_ids) or from edits to the same product (same
-- source_recall_id, different content_hash).
select
  recall_event_id,
  count(*) as total_rows,
  count(distinct content_hash) as hash_versions,
  count(distinct source_recall_id) as distinct_product_ids,
  min(event_lmd::date) as event_lmd_date
from fda_recalls_bronze
group by recall_event_id
having count(*) > 1
order by total_rows desc
limit 10;

\echo ''
\echo '=== Q7: deep-dive into a specific recall event (substitute any recall_event_id) ==='
-- Used to confirm whether multiple rows are from different products or from
-- content edits — key evidence for validating dedup key design. The 98779
-- value below is a known-multi-row event from Phase 5a; substitute any ID.
select source_recall_id, recall_event_id, recall_num, firm_legal_nam,
  event_lmd::date, content_hash, extraction_timestamp::date
from fda_recalls_bronze
where recall_event_id = 98779
order by source_recall_id, extraction_timestamp;

\echo ''
\echo '=== Q8: center code distribution ==='
-- Shows which FDA centers (CDRH, CDER, CFSAN, etc.) contribute most records
-- and unique events. Important for silver partitioning and join strategy.
select center_cd, count(*) as rows, count(distinct recall_event_id) as unique_events
from fda_recalls_bronze
group by center_cd
order by rows desc;

\echo ''
\echo '=== Q9: product type distribution ==='
-- Cross-checks center_cd mapping (CDRH=Devices, HFP=Food, etc.) and gives a
-- product-level view for silver filtering.
select product_type_short, count(*) as rows
from fda_recalls_bronze
group by product_type_short
order by rows desc;

\echo ''
\echo '=== Q10: phase distribution ==='
-- Reveals the ratio of open vs. closed recalls. Ongoing phase drives
-- termination_dt nullability (finding F).
select phase_txt, count(*) as rows
from fda_recalls_bronze
group by phase_txt
order by rows desc;

\echo ''
\echo '=== Q11: null rates for all nullable fields ==='
-- Identifies which columns are reliably populated vs. sparsely filled.
-- Directly informs which silver columns can be NOT NULL vs. must allow NULL.
select
  round(100.0 * sum(case when recall_num is null then 1 else 0 end) / count(*), 1) as pct_null_recall_num,
  round(100.0 * sum(case when rid is null then 1 else 0 end) / count(*), 1) as pct_null_rid,
  round(100.0 * sum(case when firm_fei_num is null then 1 else 0 end) / count(*), 1) as pct_null_firm_fei_num,
  round(100.0 * sum(case when phase_txt is null then 1 else 0 end) / count(*), 1) as pct_null_phase_txt,
  round(100.0 * sum(case when center_classification_type_txt is null then 1 else 0 end) / count(*), 1) as pct_null_class_type,
  round(100.0 * sum(case when recall_initiation_dt is null then 1 else 0 end) / count(*), 1) as pct_null_initiation_dt,
  round(100.0 * sum(case when termination_dt is null then 1 else 0 end) / count(*), 1) as pct_null_termination_dt,
  round(100.0 * sum(case when product_description_txt is null then 1 else 0 end) / count(*), 1) as pct_null_product_desc,
  round(100.0 * sum(case when product_distributed_quantity is null then 1 else 0 end) / count(*), 1) as pct_null_quantity
from fda_recalls_bronze;

\echo ''
\echo '=== Q12: product_distributed_quantity value samples ==='
-- This is a free-text field with no enforced format. Sampling the most
-- common values reveals the range of formats that silver cleaning must handle.
select product_distributed_quantity, count(*) as occurrences
from fda_recalls_bronze
where product_distributed_quantity is not null
group by product_distributed_quantity
order by occurrences desc
limit 10;

\echo ''
\echo '=== Q13: null recall_num breakdown by center ==='
-- Identifies which centers are responsible for the 1% null recall_num rate.
-- Helps determine whether silver can require this field for specific centers.
select center_cd, count(*) as null_recall_num_rows
from fda_recalls_bronze
where recall_num is null
group by center_cd
order by null_recall_num_rows desc;

\echo ''
\echo '=== Q14: top spike days ==='
-- Identifies the highest-volume single days; used to distinguish normal
-- variation from large multi-product batch publications.
select event_lmd::date as day, count(*) as records
from fda_recalls_bronze
group by event_lmd::date
order by records desc
limit 5;

\echo ''
\echo '=== Q15: weekday gap analysis ==='
-- Finds weekdays with zero FDA activity. Expected = US federal holidays.
-- No-gap result in 90 days (except Presidents Day) confirms reliable daily
-- publication. Date window is hardcoded to the Phase 5a baseline; widen as
-- the dataset grows.
with date_series as (
  select generate_series(
    '2026-01-29'::date, '2026-04-27'::date, '1 day'::interval
  )::date as day
),
active_days as (
  select distinct event_lmd::date as day from fda_recalls_bronze
)
select d.day, to_char(d.day, 'Day') as day_name
from date_series d
left join active_days a on d.day = a.day
where a.day is null and extract(dow from d.day) not in (0, 6)
order by d.day;

\echo ''
\echo '=== Q16: extraction run history (cross-table — extraction_runs) ==='
-- Confirms pipeline runs were recorded with correct counts, status, and
-- timing.
select source, status, records_extracted, records_inserted, records_rejected,
  started_at, finished_at,
  extract(epoch from (finished_at - started_at))::int as duration_seconds
from extraction_runs
where source = 'fda'
order by started_at;
