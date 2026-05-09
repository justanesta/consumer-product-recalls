-- Singular test: FDA does not perform silent edits — for every
-- consecutive bronze pair per PRODUCTID where content_hash changed,
-- EVENTLMD must have advanced too. Returns rows for each silent-edit
-- transition (the alarming cell C from the rich diagnostic). The
-- archive-migration noise pattern (cell B: EVENTLMD changed without
-- content) is documented in ADR 0023 and intentionally NOT flagged
-- by this test — it would create permanent warning noise. Severity=
-- warn via dbt_project.yml. Full contingency table and samples at
-- scripts/sql/fda/bronze/assert_eventlmd_correlates_with_content_change.sql.
--
-- Rebaseline filter (matches the rich script): rows from
-- change_type IN ('schema_rebaseline', 'hash_helper_rebaseline') are
-- excluded before LAG so that per-ADR-0027 hash recomputations don't
-- look like edits. Verified necessary on 2026-05-08 — without the
-- filter, the 2026-05-01 architecture realignment generated 2,535
-- false positives. NULL change_type (pre-migration-0009) is kept as
-- routine. Phase 6 recall_event_history will use the same filter
-- (implementation_plan.md:611). Joins extraction_runs by raw table
-- name rather than via a dbt source declaration — keeps the test
-- self-contained; if Phase 6 needs the source declaration, add then.

with routine_only as (
    select b.*
    from {{ source('fda', 'fda_recalls_bronze') }} b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
),
ordered as (
    select
        source_recall_id,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(event_lmd)    over w as prev_event_lmd,
        lag(content_hash) over w as prev_content_hash
    from routine_only
    window w as (partition by source_recall_id order by extraction_timestamp)
)
select source_recall_id, extraction_timestamp, event_lmd, content_hash
from ordered
where prev_content_hash is not null
  and content_hash is distinct from prev_content_hash
  and event_lmd    is not distinct from prev_event_lmd
