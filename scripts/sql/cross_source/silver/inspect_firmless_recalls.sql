\set ON_ERROR_STOP on
\pset null '<NULL>'
\echo '=== Firmless recalls: recall_events with ZERO recall_event_firm bridge rows ==='
-- Conceptually every recall has a responsible party, but the bridge is zero-or-many because:
--   (a) CPSC retailers are routed to recall_event.sales_channel_narrative (Option B — the field is
--       a sales-channel narrative, not a firm entity), so a CPSC recall naming ONLY retailers gets
--       0 firm rows; and
--   (b) USCG "Finding S" rows (no mic, no company_name, no directory match) appear in recall_event
--       but are filtered out of the bridge.
-- This quantifies the cohort and confirms the mechanism.
--
-- Run: psql -f scripts/sql/cross_source/silver/inspect_firmless_recalls.sql

\echo ''
\echo '=== Q1: zero-firm recalls by source (count + % of source) ==='
select
    re.source,
    count(*)                                                                  as total_recalls,
    count(*) filter (where nf.n_firms is null)                               as zero_firm_recalls,
    round(100.0 * count(*) filter (where nf.n_firms is null) / count(*), 3)  as pct_zero_firm
from recall_event re
left join (
    select recall_event_id, count(*) as n_firms
    from recall_event_firm
    group by recall_event_id
) nf using (recall_event_id)
group by re.source
order by zero_firm_recalls desc;

\echo ''
\echo '=== Q2: CPSC zero-firm recalls — do they carry a retailer sales-channel narrative? ==='
select
    count(*)                                                       as cpsc_zero_firm,
    count(*) filter (where re.sales_channel_narrative is not null) as with_retailer_narrative
from recall_event re
left join recall_event_firm ref using (recall_event_id)
where re.source = 'CPSC'
  and ref.recall_event_id is null;

\echo ''
\echo '=== Q3: 15 example zero-firm recalls (source, id, title) ==='
select re.source, re.source_recall_id, left(re.title, 70) as title
from recall_event re
left join recall_event_firm ref using (recall_event_id)
where ref.recall_event_id is null
order by re.source, re.source_recall_id
limit 15;
