-- Floor check: fails if recall_event has fewer than 500 CPSC rows.
-- Bronze currently holds 1,193 CPSC rows (per
-- documentation/cpsc/last_publish_date_semantics.md), so 500 leaves generous
-- headroom for dedup variance while still catching catastrophic silent
-- failure (e.g., an extractor returning zero).
--
-- ADR 0015 prescribes a ±50%-of-baseline check for per-source counts. That
-- requires historical baseline data which does not yet exist; promote this
-- assertion to a proper baseline guard in Phase 6 once production data has
-- accumulated.

select 'cpsc_event_count_below_floor' as failure
where (
    select count(*) from {{ ref('recall_event') }} where source = 'CPSC'
) < 500
