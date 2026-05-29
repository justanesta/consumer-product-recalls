-- Phase 5c — NHTSA bronze data exploration after first live extraction.
--
-- When to run: after `recalls extract nhtsa` lands rows in nhtsa_recalls_bronze.
-- Output is the source-of-record for the eventual
-- documentation/nhtsa/first_extraction_findings.md (analogous to the CPSC
-- and FDA findings docs); each \echo block is one numbered query the doc
-- can cite.
--
-- Two NHTSA-specific shape facts shape every query below:
--
--   (a) 1-to-many at the campaign level. One CAMPNO (recall campaign)
--       expands into many rows — one per (make, model, year) tuple per
--       recall. source_recall_id (RECORD_ID) is the per-row natural key;
--       campno is the campaign-level grouping. Compare to FDA where
--       recall_event_id is the campaign analog.
--   (b) No per-record edit cursor. Unlike CPSC's LastPublishDate or FDA's
--       eventlmd, NHTSA's flat-file format carries no row-level "last
--       modified" — the watermark lives on the URL response headers
--       (probe_watermarks.sh tests its reliability). Edit detection in
--       bronze relies on the content-hash dedup mechanism alone (Q4).
--
-- Run with: psql ... -f scripts/sql/nhtsa/bronze/explore_bronze_shape.sql
--
-- Note on scope: bronze currently holds only the recent-years subset of
-- the NHTSA archive (loaded from RCL_FROM_2025_*.zip / FLAT_RCL_POST_2010
-- rather than the full PRE_2010 corpus). Yearly-distribution queries
-- (Q2, Q11) reflect that scope; widen the range when the historical seed
-- runs in Phase 7.

\echo '=== Q1: overall row count, rcdate range, distinct campaigns ==='
-- rcdate (Part 573 "received" date) is the cleanest publication-time
-- proxy per Finding H Q2. Nullable post-Finding-H schema change, so
-- min/max ignores the 5-record empty-rcdate cohort if it landed.
select
  count(*) as total_rows,
  count(distinct campno) as distinct_campaigns,
  count(distinct source_recall_id) as distinct_record_ids,
  min(rcdate) as min_rcdate,
  max(rcdate) as max_rcdate,
  count(*) filter (where rcdate is null) as null_rcdate_rows
from nhtsa_recalls_bronze;

\echo ''
\echo '=== Q2: yearly cadence by rcdate ==='
-- Reveals historical gaps (e.g., dead years, batch-load events like the
-- 1979 bulk pre-1979 backfill referenced in Finding H) and recent-years
-- volume. With recent-years-only scope, expect a tight band; the long
-- tail surfaces when PRE_2010 lands in Phase 7's historical seed.
select
  extract(year from rcdate)::int as year,
  count(*) as rows,
  count(distinct campno) as campaigns
from nhtsa_recalls_bronze
where rcdate is not null
group by extract(year from rcdate)
order by year;

\echo ''
\echo '=== Q3: monthly cadence within the loaded window ==='
-- Publication rhythm at month resolution. NHTSA's flat-file regenerates
-- daily (per probe_watermarks.sh hypothesis) but the rcdate field
-- reflects the underlying recall filing date, not the file regen date.
-- A flat monthly count would suggest steady industry recall activity;
-- spikes likely correspond to multi-make automotive recall waves.
select
  date_trunc('month', rcdate)::date as month,
  count(*) as rows,
  count(distinct campno) as campaigns
from nhtsa_recalls_bronze
where rcdate is not null
group by date_trunc('month', rcdate)
order by month;

\echo ''
\echo '=== Q4: edit detection — source_recall_ids with multiple content hashes ==='
-- NHTSA has no per-row edit timestamp, so the only edit signal in bronze
-- is multiple distinct content_hash values for the same source_recall_id.
-- Empty result = no edits captured during the extraction window. Once
-- the daily extractor runs over multiple file-regen events, non-empty
-- rows here are the corpus that proves "the file content actually
-- changed, not just got rebuilt with the same bytes."
select source_recall_id, count(distinct content_hash) as hash_versions, count(*) as total_rows
from nhtsa_recalls_bronze
group by source_recall_id
having count(distinct content_hash) > 1
order by hash_versions desc, total_rows desc
limit 20;

\echo ''
\echo '=== Q5: total rows vs distinct identities (1-to-many shape confirmation) ==='
-- Expect: total_rows ≈ distinct_record_ids (RECORD_ID is per-row),
-- distinct_campaigns much lower (one campaign expands to many rows),
-- ratio ≈ NHTSA's avg makes/models per recall (~3-5 historically).
select
  count(*) as total_rows,
  count(distinct source_recall_id) as distinct_record_ids,
  count(distinct campno) as distinct_campaigns,
  round(count(*)::numeric / nullif(count(distinct campno), 0), 2) as avg_rows_per_campaign
from nhtsa_recalls_bronze;

\echo ''
\echo '=== Q6: rows-per-campaign distribution ==='
-- Histograms the 1-to-many fan-out. A long tail (single campaign with
-- 100+ rows) typically corresponds to a multi-make industry-wide recall
-- (Takata airbag, etc.).
with per_campaign as (
  select campno, count(*) as rows from nhtsa_recalls_bronze group by campno
)
select
  case
    when rows = 1 then '1'
    when rows between 2 and 5 then '2-5'
    when rows between 6 and 20 then '6-20'
    when rows between 21 and 100 then '21-100'
    else '100+'
  end as bucket,
  count(*) as campaigns
from per_campaign
group by 1
order by min(rows);

\echo ''
\echo '=== Q7: top fan-out campaigns ==='
-- The biggest 1-to-many campaigns. Substitute any campno from the result
-- into a follow-up query to inspect a specific multi-row campaign.
select
  campno,
  count(*) as rows,
  count(distinct maketxt) as distinct_makes,
  count(distinct yeartxt) as distinct_years,
  min(rcdate) as rcdate
from nhtsa_recalls_bronze
group by campno
order by rows desc
limit 10;

\echo ''
\echo '=== Q8: nullable date sentinel and population rates ==='
-- Sentinel 1901-01-01 (per Finding H) parses cleanly into bronze on
-- odate; silver staging is responsible for mapping it to NULL. This
-- query quantifies how often each nullable date is empty (NULL) vs
-- populated vs sentinel-valued. odate is the Owner Notification Date —
-- empty means "manufacturer not yet notified owners"; sentinel means
-- "unknown date" per the convention NHTSA uses on historical records.
select
  count(*) as total,
  -- bgman / endman: manufacturing date range (vehicle scope)
  count(*) filter (where bgman is null) as null_bgman,
  count(*) filter (where endman is null) as null_endman,
  -- odate: owner notification date
  count(*) filter (where odate is null) as null_odate,
  count(*) filter (where odate = '1901-01-01 00:00:00+00') as sentinel_odate,
  -- datea: date received by NHTSA (different from rcdate per Finding H)
  count(*) filter (where datea is null) as null_datea,
  -- rcdate: Part 573 received date
  count(*) filter (where rcdate is null) as null_rcdate
from nhtsa_recalls_bronze;

\echo ''
\echo '=== Q9: drift-added field population by rcdate year ==='
-- Validates Finding F's drift cutoffs against loaded data: notes
-- (post-2007), rcl_cmpt_id (post-2008), mfr_comp_* (post-2020),
-- do_not_drive / park_outside (post-May-2025). With recent-years-only
-- scope, expect notes / rcl_cmpt_id ~100% populated, mfr_comp_* mostly
-- populated, do_not_drive sparse (only post-May-2025 rows).
select
  extract(year from rcdate)::int as year,
  count(*) as rows,
  round(100.0 * count(*) filter (where notes is not null) / count(*), 1) as pct_notes,
  round(100.0 * count(*) filter (where rcl_cmpt_id is not null) / count(*), 1) as pct_rcl_cmpt_id,
  round(100.0 * count(*) filter (where mfr_comp_name is not null) / count(*), 1) as pct_mfr_comp_name,
  round(100.0 * count(*) filter (where do_not_drive is not null) / count(*), 1) as pct_do_not_drive
from nhtsa_recalls_bronze
where rcdate is not null
group by extract(year from rcdate)
order by year;

\echo ''
\echo '=== Q10: rcltype distribution (vehicle / equipment / tire / child seat) ==='
-- Low-cardinality enum per RCL.txt: V (Vehicle), E (Equipment),
-- T (Tire), C (Child Restraint). Distribution informs silver
-- partitioning and downstream join shape.
select rcltype, count(*) as rows, count(distinct campno) as campaigns
from nhtsa_recalls_bronze
group by rcltype
order by rows desc;

\echo ''
\echo '=== Q11: yeartxt distribution (vehicle model year, not recall year) ==='
-- yeartxt is the model year of the affected vehicle, NOT the recall
-- year. Expect a long-tail histogram centered on recent years with
-- occasional ancient outliers (classic-car recalls). Loaded recent-year
-- bronze should still show model years going back decades.
select yeartxt, count(*) as rows
from nhtsa_recalls_bronze
group by yeartxt
order by rows desc
limit 25;

\echo ''
\echo '=== Q12: top makes by row count ==='
-- Sanity check: Ford / Chevrolet / Toyota / Honda / GM should dominate.
-- Surprises here (typo variants like "FORD MOTOR CO" vs "FORD") inform
-- silver firm-resolution mapping in Phase 6.
select maketxt, count(*) as rows, count(distinct campno) as campaigns
from nhtsa_recalls_bronze
group by maketxt
order by rows desc
limit 15;

\echo ''
\echo '=== Q13: FMVSS length and value distribution ==='
-- Validates Finding F's May 2025 width-reduction observation. Schema
-- enforces max_length=3 — anything longer would have been quarantined.
-- Distribution shows whether ≤3-char values are dominantly numeric
-- standard refs (e.g., "208" for occupant crash protection) or include
-- non-numeric outliers.
select
  length(fmvss) as fmvss_length,
  count(*) as rows,
  count(distinct fmvss) as distinct_values
from nhtsa_recalls_bronze
where fmvss is not null
group by length(fmvss)
order by fmvss_length;

\echo ''
\echo '=== Q14: rejected table — count and rejection-reason breakdown ==='
-- Empty result = first extraction parsed cleanly. Non-empty = treat each
-- distinct error class as a schema-bug to fix in src/schemas/nhtsa.py
-- (per the CPSC Phase 3 lesson: cassette/extraction failures are schema
-- bugs, not test failures to skip).
select
  failure_reason,
  count(*) as rows,
  min(rejected_at) as first_seen,
  max(rejected_at) as last_seen
from nhtsa_recalls_rejected
group by failure_reason
order by rows desc;

\echo ''
\echo '=== Q15: top spike days by rcdate ==='
-- Identifies the highest-volume single days; large spikes correspond to
-- multi-make industry-wide recall publications (Takata, Ford door
-- latch, etc.). Distinguishes normal cadence from batch-publication
-- events that the Q3 monthly view smears over.
select rcdate::date as day, count(*) as rows, count(distinct campno) as campaigns
from nhtsa_recalls_bronze
where rcdate is not null
group by rcdate::date
order by rows desc
limit 10;

\echo ''
\echo '=== Q16: extraction_runs history (cross-table) ==='
-- Confirms pipeline runs were recorded with correct counts, status, and
-- timing. response_inner_content_sha256 is flat-file-specific per
-- migration 0011 — a single distinct value across multiple runs means
-- the un-zipped TSV bytes were stable even when the ZIP wrapper changed
-- (hash-helper-rebaseline candidate).
select
  status,
  records_extracted,
  records_inserted,
  records_rejected,
  started_at,
  extract(epoch from (finished_at - started_at))::int as duration_seconds,
  response_status_code,
  response_etag,
  left(response_body_sha256, 12) as zip_sha_prefix,
  left(response_inner_content_sha256, 12) as inner_sha_prefix
from extraction_runs
where source = 'nhtsa'
order by started_at;
