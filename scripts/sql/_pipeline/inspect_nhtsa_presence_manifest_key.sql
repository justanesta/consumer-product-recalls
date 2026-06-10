-- C16 verification + pre-WS-H cleanup (2026-06-09): what does extraction_run_identities hold for
-- NHTSA, and is any of it PRE-FIX (RECORD_ID-keyed) data that must be cleared before the deep-rescan?
--
-- Before this fix, the daily NHTSA extract (incorrectly) wrote the manifest keyed on RECORD_ID
-- (bronze source_recall_id), as change_type='routine'. Post-fix ONLY NhtsaDeepRescanLoader writes it,
-- keyed on campno. recall_lifecycle now reads ALL nhtsa manifest runs (no change_type gate), so any
-- leftover RECORD_ID-keyed rows from a pre-fix manual daily extract are stale and should be deleted
-- before the first real deep-rescan (they won't join to recall_event's campno, but they pollute
-- enum-run accounting). campno is '##V######'-style; RECORD_ID is a plain integer sequence.

-- 1) What is there now, by change_type + key shape:
select
    er.change_type,
    count(*)                                                   as manifest_rows,
    count(*) filter (where eri.source_recall_id ~ '^[0-9]+$')  as record_id_shaped,
    count(*) filter (where eri.source_recall_id !~ '^[0-9]+$') as campno_shaped,
    min(er.started_at)                                         as first_run,
    max(er.started_at)                                         as last_run
from extraction_run_identities eri
join extraction_runs er on er.run_id = eri.run_id
where eri.source = 'nhtsa'
group by er.change_type
order by er.change_type;

-- 2) CLEANUP — run ONLY if step 1 shows pre-fix rows (record_id_shaped > 0, typically from a manual
--    NHTSA extract during development). Removes stale NHTSA manifest rows so the first deep-rescan
--    starts clean. Safe: recall_lifecycle derives NHTSA presence solely from this table.
-- delete from extraction_run_identities where source = 'nhtsa';
