-- Floor check: fails if recall_event has fewer than 1,500 USCG rows.
-- USCG bronze holds 1,763 rows after Phase 5d Step 3's clean extraction
-- (2026-05-17), with ~45 sentinel-date rows filtered from recall_event
-- per Finding O. Steady-state silver count ~1,718. 1,500 leaves headroom
-- for the Finding O filter + dedup variance while catching a catastrophic
-- silent failure (e.g., parser drift writing all rows to quarantine).
--
-- ADR 0015 prescribes a +/-50%-of-baseline check for per-source counts.
-- That requires historical baseline data which does not yet exist; promote
-- to a proper baseline guard in Phase 6 once production data has
-- accumulated.

select 'uscg_event_count_below_floor' as failure
where (
    select count(*) from {{ ref('recall_event') }} where source = 'USCG'
) < 1500
