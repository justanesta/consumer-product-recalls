-- Phase 5c follow-up — assert that USDA's `last_modified_date`
-- advances every time bronze content_hash changes (Finding E,
-- explicitly deferred per
-- `documentation/usda/recall_api_observations.md:140-151`).
--
-- Context: USDA FSIS publishes `field_last_modified_date` (renamed to
-- `last_modified_date` in bronze per `src/schemas/usda.py`) on every
-- recall record. Per
-- `documentation/usda/recall_api_observations.md:105-128`, this field
-- cannot be used as a server-side query filter (Finding D), so the
-- extractor uses a full-dump strategy with content-hash dedup
-- (`src/extractors/usda.py:143-163`). Whether `last_modified_date`
-- *itself* reliably advances when FSIS amends a record was deferred
-- ("Finding E") and never probed.
--
-- Why it matters: ADR 0022's silver `recall_event_history` model
-- (Phase 6 deliverable) needs a per-record edit-timestamp signal.
-- Currently the four non-FDA sources all fall back to bronze
-- `extraction_timestamp` for that signal — but `last_modified_date`
-- would be more accurate (it's the actual moment the source said the
-- record changed, not the moment we noticed). If `last_modified_date`
-- reliably advances on every content edit, Phase 6 can use it as the
-- canonical USDA edit timestamp. If it doesn't, we must use
-- `extraction_timestamp` and accept the granularity loss.
--
-- Strategy: for each `(source_recall_id, langcode)` with multiple
-- bronze rows ordered by `extraction_timestamp`, find consecutive
-- pairs where `content_hash` changed (genuine edit) and check whether
-- `last_modified_date` also advanced. Cases where content changed but
-- `last_modified_date` did not (or worse, regressed) are violations.
--
-- NULL handling: ~42% of USDA records have NULL `last_modified_date`
-- per `documentation/usda/recall_api_observations.md:105-128`
-- (Finding C). NULL → non-NULL is treated as "advance" (the field
-- finally populated); non-NULL → NULL is treated as "regress" and
-- counted as a violation; NULL → NULL with content change is a
-- separate cell (`last_modified_date_unusable`) reflecting records
-- where the field tells us nothing.
--
-- Bilingual scope: each `(source_recall_id, langcode)` is treated
-- independently. Bilingual atomicity is a separate concern handled
-- by `assert_bilingual_atomic_update.sql`.
--
-- **Rebaseline filter**: bronze rows from `change_type IN
-- ('schema_rebaseline', 'hash_helper_rebaseline')` runs are excluded
-- before LAG. Per ADR 0027 + `operations/re_baseline_playbook.md`, a
-- rebaseline event re-stamps every existing bronze row's content_hash
-- when the canonical-dict shape changes — looks like an edit but
-- isn't. Without this filter, every USDA rebaseline would synthesize
-- a wave of `unchanged_with_content_change` false positives (mirroring
-- what we observed for FDA on 2026-05-08 — see
-- `documentation/fda/productid_stability_findings.md` F2 section).
-- NULL change_type rows (predating migration 0009) are kept — they
-- are routine-extraction snapshots from before the column existed.
-- Mirrors the Phase 6 `recall_event_history` design at
-- `implementation_plan.md:611`.
--
-- Expected outcomes:
--   * advanced (non-NULL → newer non-NULL) → real edit, signal works
--   * regressed (non-NULL → older non-NULL) → broken
--   * advanced_from_null (NULL → non-NULL) → fix on FSIS side
--   * regressed_to_null (non-NULL → NULL) → broken
--   * unchanged_with_content_change → either FSIS bug, or bronze
--     captured a content change that doesn't reflect a real edit
--     (e.g., sort-order shuffle in JSONB-equivalent field). Either
--     way, last_modified_date is unreliable on this row.
--   * NULL → NULL with content change → field unusable for this
--     record entirely.
--
-- A "broken" or "unchanged_with_content_change" rate above ~5% means
-- Phase 6 cannot trust last_modified_date and must use
-- extraction_timestamp.
--
-- Wire-up: also exercised via dbt singular test
-- `dbt/tests/source_assumptions/assert_usda_field_last_modified_date_advances_on_edit.sql`
-- at severity=warn (test alerts on regressions and same-timestamp-with-
-- content-change only).

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: last_modified_date behavior on content edits ==='
\echo 'For each (source_recall_id, langcode) consecutive bronze pair where'
\echo 'content_hash changed, bucket by what last_modified_date did. Sum gives'
\echo 'total observed content-edit transitions; per-cell counts give the'
\echo 'reliability profile.'

with ordered as (
    select
        source_recall_id,
        langcode,
        extraction_timestamp,
        last_modified_date,
        content_hash,
        lag(last_modified_date) over w as prev_last_modified_date,
        lag(content_hash)       over w as prev_content_hash
    from usda_fsis_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
    window w as (partition by source_recall_id, langcode order by extraction_timestamp)
),
content_edits as (
    select *
    from ordered
    where prev_content_hash is not null
      and content_hash is distinct from prev_content_hash
)
select
    case
        when prev_last_modified_date is null and last_modified_date is null
            then 'NULL_to_NULL_with_content_change'
        when prev_last_modified_date is null and last_modified_date is not null
            then 'advanced_from_null'
        when prev_last_modified_date is not null and last_modified_date is null
            then 'regressed_to_null'
        when last_modified_date > prev_last_modified_date
            then 'advanced'
        when last_modified_date < prev_last_modified_date
            then 'regressed'
        else 'unchanged_with_content_change'
    end as transition_class,
    count(*) as transition_count
from content_edits
group by transition_class
order by transition_count desc;

\echo
\echo '=== Q2: sample regressions and same-date-with-content-change (alarming cells) ==='
\echo 'These are the cells where last_modified_date is actively unreliable: it'
\echo 'either went backwards or stayed put while content changed underneath.'

with ordered as (
    select
        source_recall_id,
        langcode,
        extraction_timestamp,
        last_modified_date,
        content_hash,
        lag(extraction_timestamp) over w as prev_extraction_timestamp,
        lag(last_modified_date)   over w as prev_last_modified_date,
        lag(content_hash)         over w as prev_content_hash
    from usda_fsis_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
    window w as (partition by source_recall_id, langcode order by extraction_timestamp)
)
select
    source_recall_id,
    langcode,
    prev_extraction_timestamp,
    extraction_timestamp,
    prev_last_modified_date,
    last_modified_date,
    case
        when last_modified_date is not distinct from prev_last_modified_date
            then 'unchanged'
        when last_modified_date < prev_last_modified_date
            then 'regressed'
    end as transition_class
from ordered
where prev_content_hash is not null
  and content_hash is distinct from prev_content_hash
  and prev_last_modified_date is not null
  and (
       last_modified_date is not distinct from prev_last_modified_date
       or last_modified_date < prev_last_modified_date
  )
limit 10;

\echo
\echo '=== Q3: corpus shape — total edits vs. opportunity (context) ==='
\echo 'How many bronze rows have at least one content-edit transition. Provides'
\echo 'denominator for interpreting Q1 reliability rates.'

with ordered as (
    select
        source_recall_id,
        langcode,
        content_hash,
        lag(content_hash) over w as prev_content_hash
    from usda_fsis_recalls_bronze b
    left join extraction_runs r on b.raw_landing_path = r.raw_landing_path
    where r.change_type is null
       or r.change_type not in ('schema_rebaseline', 'hash_helper_rebaseline')
    window w as (partition by source_recall_id, langcode order by extraction_timestamp)
)
select
    count(*)                                                                         as total_bronze_rows,
    count(*) filter (where prev_content_hash is not null)                            as transitions_observed,
    count(*) filter (where prev_content_hash is not null
                     and content_hash is distinct from prev_content_hash)            as content_edit_transitions,
    count(distinct (source_recall_id, langcode))                                     as distinct_lang_keyed_records
from ordered;
