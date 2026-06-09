\set ON_ERROR_STOP on
\pset null '<NULL>'
\echo '=== Why are ~426 USDA recalls firmless? (recall_event_firm drops USDA rows with a blank establishment) ==='
-- Hypothesis: most are Public Health Alerts (PHAs) — FSIS issues these when no single producing
-- establishment is identified (outbreak/import/unknown-source), so the establishment field is blank
-- and no firm bridge row is created. This breaks it down by recall_type + confirms the mechanism.
--
-- Run: psql -f scripts/sql/usda_recalls/silver/inspect_firmless_usda.sql

\echo ''
\echo '=== Q1: firmless USDA recalls by recall_type (lifecycle_status) ==='
select
    coalesce(re.lifecycle_status, '<null>') as recall_type,
    count(*)                                as firmless_recalls
from recall_event re
left join recall_event_firm ref using (recall_event_id)
where re.source = 'USDA'
  and ref.recall_event_id is null
group by re.lifecycle_status
order by firmless_recalls desc;

\echo ''
\echo '=== Q2: confirm the mechanism — is the establishment field blank for these? ==='
select
    count(*)                                                                              as firmless,
    count(*) filter (where nullif(trim(re.source_payload_raw->>'establishment'), '') is null) as establishment_blank
from recall_event re
left join recall_event_firm ref using (recall_event_id)
where re.source = 'USDA'
  and ref.recall_event_id is null;

\echo ''
\echo '=== Q3: context — ALL USDA recalls by recall_type, with firm coverage ==='
select
    coalesce(re.lifecycle_status, '<null>')                  as recall_type,
    count(*)                                                 as total,
    count(ref.recall_event_id)                               as bridge_rows,
    count(distinct re.recall_event_id) filter (
        where ref.recall_event_id is not null)               as recalls_with_firm
from recall_event re
left join recall_event_firm ref using (recall_event_id)
where re.source = 'USDA'
group by re.lifecycle_status
order by total desc;

\echo ''
\echo '=== Q4: 15 example firmless USDA recalls ==='
select re.source_recall_id,
       coalesce(re.lifecycle_status, '<null>') as recall_type,
       left(re.title, 70)                      as title
from recall_event re
left join recall_event_firm ref using (recall_event_id)
where re.source = 'USDA'
  and ref.recall_event_id is null
order by re.source_recall_id
limit 15;
