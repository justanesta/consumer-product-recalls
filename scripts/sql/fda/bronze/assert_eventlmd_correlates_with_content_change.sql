-- Phase 5c follow-up — quantify how reliably FDA's EVENTLMD field
-- correlates with actual content changes.
--
-- Context: FDA's EVENTLMD ("event last modified date") is the
-- incremental-extraction watermark per `src/extractors/fda.py:269-270`.
-- The original assumption (per FDA's documented contract) is that
-- EVENTLMD advances only on real edits to the recall event. ADR 0023
-- documented the first violation: an active archive migration bumps
-- EVENTLMD on records dated 2002–2019 without changing content,
-- producing false-edit signals on the watermark. The mitigation is
-- the weekly deep-rescan workflow (ADR 0010); this assertion
-- characterizes the *rate* and *direction* of EVENTLMD/content
-- correlation drift to give Phase 6 something concrete to design
-- against (replaces the originally-planned PRODUCTLMD assertion,
-- which is structurally infeasible — PRODUCTLMD is not captured in
-- `fda_recalls_bronze`; see audit doc for the follow-up).
--
-- Two failure modes worth distinguishing:
--   (A) EVENTLMD changed but content_hash did NOT — the archive-
--       migration noise pattern documented in ADR 0023. Innocuous
--       for silver correctness (silver dedupes on content_hash) but
--       fills bronze with churn and inflates extraction-run row
--       counts. Quantifying this rate informs whether the count
--       guard at `tests/extractors/test_fda_extractor.py:127` needs
--       loosening or whether the migration's natural decay makes the
--       guard redundant.
--   (B) content_hash changed but EVENTLMD did NOT — silent edits.
--       This would be alarming: it means FDA changed a recall's data
--       without advancing the field that the incremental extractor
--       uses as a freshness signal. Incremental runs would miss the
--       edit; only the deep-rescan would catch it. If the rate is
--       non-zero, ADR 0010's deep-rescan cadence assumption is the
--       only thing preventing data loss in silver.
--
-- Strategy: for each PRODUCTID with multiple bronze rows, compare
-- consecutive (extraction_timestamp-ordered) rows and bucket them by
-- whether content_hash changed and whether event_lmd changed. Q1
-- prints the four-cell contingency. Q2 samples each interesting cell.
--
-- Cross-run filter: a PRODUCTID with only one bronze row contributes
-- nothing — no transition to observe. Implicit via the LAG predicate.
--
-- **Rebaseline filter**: bronze rows from `change_type IN
-- ('schema_rebaseline', 'hash_helper_rebaseline')` runs are excluded
-- before LAG. Per ADR 0027 + `operations/re_baseline_playbook.md`, a
-- rebaseline event re-stamps every existing bronze row's content_hash
-- when the canonical-dict shape changes — looks like an edit but
-- isn't. The 2026-05-08 verification (`diagnose_silent_edit_attribution.sql`)
-- found 100% of FDA's pre-filter "silent edits" were the 2026-05-01
-- architecture-realignment rebaseline; without this filter the
-- assertion produces ~2,500 false-positive Cell C transitions.
-- NULL change_type rows (predating migration 0009) are kept — they
-- are routine-extraction snapshots from before the column existed.
-- Mirrors the Phase 6 `recall_event_history` design at
-- `implementation_plan.md:611`.
--
-- Expected outcomes by cell:
--   * EVENTLMD changed + content_hash changed → real edits. Should be
--     the majority for non-archive-migration records.
--   * EVENTLMD changed + content_hash unchanged → archive-migration
--     noise (or other no-op EVENTLMD bumps). Per ADR 0023, expected
--     to be material; quantify and document.
--   * EVENTLMD unchanged + content_hash changed → silent edits.
--     Expected to be 0; non-zero is a Phase 6 deliverable trigger.
--   * EVENTLMD unchanged + content_hash unchanged → impossible by
--     content-hash dedup design (filter_new_records would have
--     skipped the row). Reported for invariant validation only.
--
-- Wire-up: also exercised via dbt singular test
-- `dbt/tests/source_assumptions/assert_fda_eventlmd_correlates_with_content_change.sql`
-- at severity=warn (test alerts on cell C — silent edits — only).

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: EVENTLMD vs. content_hash transition contingency ==='
\echo 'Counts of consecutive bronze-row pairs per PRODUCTID, bucketed by whether'
\echo 'each field changed across the transition. Cell C (EVENTLMD unchanged +'
\echo 'content changed) is the silent-edit failure mode and should be 0.'

with ordered as (
    select
        source_recall_id,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(event_lmd)     over w as prev_event_lmd,
        lag(content_hash)  over w as prev_content_hash
    from fda_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
    window w as (partition by source_recall_id order by extraction_timestamp)
),
transitions as (
    select
        case when event_lmd    is distinct from prev_event_lmd    then 'EVENTLMD changed'   else 'EVENTLMD same'   end as eventlmd_state,
        case when content_hash is distinct from prev_content_hash then 'content changed'    else 'content same'    end as content_state
    from ordered
    where prev_content_hash is not null  -- only consider real transitions, not first-row-per-PRODUCTID
)
select
    eventlmd_state,
    content_state,
    count(*) as transition_count
from transitions
group by eventlmd_state, content_state
order by eventlmd_state, content_state;

\echo
\echo '=== Q2a: silent-edit samples (cell C — alarming) ==='
\echo 'PRODUCTIDs where content_hash changed but EVENTLMD did not. Expected: 0.'

with ordered as (
    select
        source_recall_id,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(extraction_timestamp) over w as prev_extraction_timestamp,
        lag(event_lmd)            over w as prev_event_lmd,
        lag(content_hash)         over w as prev_content_hash
    from fda_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
    window w as (partition by source_recall_id order by extraction_timestamp)
)
select
    source_recall_id,
    prev_extraction_timestamp,
    extraction_timestamp,
    event_lmd as event_lmd_unchanged,
    prev_content_hash,
    content_hash
from ordered
where prev_content_hash is not null
  and event_lmd     is not distinct from prev_event_lmd
  and content_hash  is distinct from prev_content_hash
limit 5;

\echo
\echo '=== Q2b: archive-migration-noise samples (cell B — expected non-zero per ADR 0023) ==='
\echo 'PRODUCTIDs where EVENTLMD bumped but content_hash unchanged. Quantifying'
\echo 'cell B against cell A (real edits) is the noise-rate measurement.'

with ordered as (
    select
        source_recall_id,
        extraction_timestamp,
        event_lmd,
        content_hash,
        lag(extraction_timestamp) over w as prev_extraction_timestamp,
        lag(event_lmd)            over w as prev_event_lmd,
        lag(content_hash)         over w as prev_content_hash
    from fda_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
    window w as (partition by source_recall_id order by extraction_timestamp)
)
select
    source_recall_id,
    prev_extraction_timestamp,
    extraction_timestamp,
    prev_event_lmd,
    event_lmd,
    content_hash as content_unchanged
from ordered
where prev_content_hash is not null
  and event_lmd     is distinct from prev_event_lmd
  and content_hash  is not distinct from prev_content_hash
limit 5;
