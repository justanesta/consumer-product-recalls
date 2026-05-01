-- Floor check: fails if recall_event has fewer than 1,000 USDA rows.
-- USDA bronze holds ~2,001 rows after Phase 5b first extraction (2026-04-30),
-- with bilingual EN/ES siblings sharing source_recall_id. Silver consumes
-- English only via stg_usda_fsis_recalls. 1,000 leaves generous headroom for
-- both the EN/ES split and dedup variance while catching catastrophic silent
-- failure.
--
-- ADR 0015 prescribes a ±50%-of-baseline check for per-source counts. That
-- requires historical baseline data which does not yet exist; promote this
-- assertion to a proper baseline guard in Phase 6 once production data has
-- accumulated.

select 'usda_event_count_below_floor' as failure
where (
    select count(*) from {{ ref('recall_event') }} where source = 'USDA'
) < 1000
