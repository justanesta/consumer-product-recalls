-- Leak test: does any NHTSA campno in bronze fail to reach silver/gold?
--
-- The recall_event NHTSA branch reads stg_nhtsa_recalls (a view over bronze)
-- directly via `distinct on (campno)` with NO date filter and NO snapshot
-- dependency (dbt/models/silver/recall_event.sql), and mart_recall_summary is
-- 1:1 over recall_event (left joins to pre-grouped rollups never drop a row;
-- assert_mart_recall_summary_no_event_loss enforces it). So EVERY distinct campno
-- in bronze must appear once in recall_event and once in mart_recall_summary.
-- This script proves that empirically — if it holds, no transform-logic bug is
-- hiding new recalls; the absence is upstream (NHTSA published nothing new) or a
-- freshness problem (see mart_freshness_by_source.sql), not a dropped row.
--
-- recall_event_id is md5('NHTSA' || '|' || campno) per recall_event.sql.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/_pipeline/verify_new_campaigns_reach_mart.sql

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: count parity bronze (distinct campno) -> recall_event -> mart ==='
\echo 'All three counts should be EQUAL; both deltas should be 0. A positive'
\echo 'bronze_minus_event = campnos in bronze not reaching silver (a real leak).'
\echo 'A positive event_minus_mart = the mart dropped events (no_event_loss broke).'

with bronze_campnos as (select count(distinct campno) as n from nhtsa_recalls_bronze),
     event_n        as (select count(*) as n from recall_event       where source = 'NHTSA'),
     mart_n         as (select count(*) as n from mart_recall_summary where source = 'NHTSA')
select
    (select n from bronze_campnos)                          as distinct_bronze_campnos,
    (select n from event_n)                                 as recall_event_nhtsa_rows,
    (select n from mart_n)                                  as mart_nhtsa_rows,
    (select n from bronze_campnos) - (select n from event_n) as bronze_minus_event,
    (select n from event_n) - (select n from mart_n)         as event_minus_mart;

\echo
\echo '=== Q2: recent bronze campnos vs their presence in recall_event and the mart ==='
\echo 'campnos first seen in bronze in the last 30 days, with in_recall_event / in_mart'
\echo 'flags. Every row should be t/t. Any f means that campno is stuck upstream of'
\echo 'the layer that shows f (the actual smoking gun if a leak exists).'

with recent as (
    select campno,
           min(extraction_timestamp)::date as first_seen,
           max(rcdate)::date               as rcdate
    from nhtsa_recalls_bronze
    group by campno
    having min(extraction_timestamp) >= current_date - 30
)
select
    r.campno,
    r.first_seen,
    r.rcdate,
    (e.recall_event_id is not null)  as in_recall_event,
    (m.recall_event_id is not null)  as in_mart
from recent r
left join recall_event       e on e.recall_event_id = md5('NHTSA' || '|' || r.campno)
left join mart_recall_summary m on m.recall_event_id = md5('NHTSA' || '|' || r.campno)
order by r.rcdate desc nulls last, r.first_seen desc;

\echo
\echo '=== Q3: explicit count of recent bronze campnos MISSING from the mart ==='
\echo 'Expected: 0. Nonzero = a genuine bronze->gold leak; capture these campnos and'
\echo 'trace which layer drops them (stg_nhtsa_recalls -> recall_event -> mart).'

with recent as (
    select distinct campno
    from nhtsa_recalls_bronze
    where extraction_timestamp >= current_date - 30
)
select count(*) as recent_campnos_missing_from_mart
from recent r
left join mart_recall_summary m on m.recall_event_id = md5('NHTSA' || '|' || r.campno)
where m.recall_event_id is null;
