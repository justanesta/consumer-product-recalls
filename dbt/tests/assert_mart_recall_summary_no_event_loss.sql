-- Singular test (Phase 6e gold reconciliation): the denormalized mart_recall_summary must preserve
-- every recall_event (it is 1:1 — left joins to grouped rollups never drop or duplicate a row).
-- Returns a row iff the counts diverge (severity=error).
with event_n as (select count(*) as n from {{ ref('recall_event') }}),
     mart_n  as (select count(*) as n from {{ ref('mart_recall_summary') }})
select event_n.n as recall_event_count, mart_n.n as mart_count
from event_n, mart_n
where event_n.n <> mart_n.n
