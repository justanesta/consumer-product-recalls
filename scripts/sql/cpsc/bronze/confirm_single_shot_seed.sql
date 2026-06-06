-- Corpus gate G1c — confirm cpsc_recalls_bronze is the single-shot historical seed.
--
-- PURPOSE: the CPSC firm-name gates (G1 measure_comma_optional_of_strip.sql) and the
-- sibling inspect_firm_name_fragmentation.sql read cpsc_recalls_bronze RAW — they do NOT
-- apply the stg_cpsc_recalls latest-version projection (row_number() over (partition by
-- source_recall_id order by extraction_timestamp desc) where rn=1). That raw read is only
-- valid if bronze holds ~one row per source_recall_id (the single-shot 2026-05-31 seed,
-- ~9,828 records). If the daily incremental has banked edits, some ids carry multiple
-- content-hash versions, and reading raw double-counts an edited firm — inflating the strip
-- coverage figures. This gate quantifies version multiplicity so the strip gates know
-- whether they can trust a raw read or must add a latest-per-id dedup CTE.
--
-- EXPECTED SIGNAL: PROCEED (strip/fragmentation gates may read bronze raw) if Q2 shows all /
-- nearly all source_recall_ids at n_rows=1 and distinct_source_recall_id ~= 9,828. If a
-- material fraction carry n_rows>1, prepend a latest-per-id CTE (mirror stg_cpsc_recalls.sql)
-- to G1 + inspect_firm_name_fragmentation.sql before trusting their counts.
--
-- Feeds: project_scope/archive/phase-6b-execution-plan.md PR 6b.1 gate G1c.
-- Run with: psql ... -f scripts/sql/cpsc/bronze/confirm_single_shot_seed.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo '=== Q1: bronze row counts + identity cardinality (expect ~9,828 single-shot) ==='
-- total_bronze_rows == distinct_source_recall_id  =>  exactly one row per id (single-shot).
with per_id as (
  select source_recall_id, count(*) as n_rows
  from cpsc_recalls_bronze
  group by source_recall_id
)
select
  (select count(*) from cpsc_recalls_bronze)                  as total_bronze_rows,
  (select count(*) from per_id)                               as distinct_source_recall_id,
  (select count(distinct recall_id) from cpsc_recalls_bronze) as distinct_recall_id,
  (select max(n_rows) from per_id)                            as max_rows_per_id,
  (select round(avg(n_rows), 4) from per_id)                  as avg_rows_per_id;

\echo ''
\echo '=== Q2: rows-per-source_recall_id distribution (single-shot => one bucket: n_rows=1) ==='
-- A clean single-shot seed returns exactly one row here: (n_rows=1, ids_with_n_rows=~9828).
-- Any n_rows>1 bucket = edited records the incremental re-landed (raw read would double-count).
with per_id as (
  select source_recall_id, count(*) as n_rows
  from cpsc_recalls_bronze
  group by source_recall_id
)
select n_rows, count(*) as ids_with_n_rows
from per_id
group by n_rows
order by n_rows;

\echo ''
\echo '=== Q3: sample multi-version ids (empty result => clean single-shot seed) ==='
-- If non-empty, these ids have >1 bronze version; distinct_hashes>1 confirms real content
-- edits (not duplicate re-ingestion, which content-hash dedup prevents).
with per_id as (
  select source_recall_id,
         count(*)                     as n_rows,
         count(distinct content_hash) as distinct_hashes,
         min(extraction_timestamp)    as first_seen,
         max(extraction_timestamp)    as last_seen
  from cpsc_recalls_bronze
  group by source_recall_id
)
select source_recall_id, n_rows, distinct_hashes, first_seen, last_seen
from per_id
where n_rows > 1
order by n_rows desc, source_recall_id
limit 25;
