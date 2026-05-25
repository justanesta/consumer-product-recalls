-- Diagnostic — across all bronze rows landed by a given USDA recalls run,
-- count which fields actually differ between the new bronze version and
-- the immediately-prior bronze version. The authoritative version of the
-- "is this wave driven by one field?" question — distinct from
-- diagnose_payload_drift_for_recall.sql which is single-recall.
--
-- Motivating context (2026-05-15): the 2026-05-15 run inserted 1235 rows
-- with 100% silent edits per assert_field_last_modified_date_advances_on_edit.sql.
-- A single payload diff on `021-2020` showed only `company_media_contact`
-- changed, and only in whitespace padding. This script lets us verify that
-- the same field is responsible for the entire 1235-row wave (or surfaces
-- a richer multi-field pattern).
--
-- Strategy:
--   1. Pair each "new" bronze row (extraction_timestamp >= run.started_at)
--      with the latest "old" bronze row for the same (source_recall_id,
--      langcode) where extraction_timestamp < run.started_at.
--   2. For each pair, compute a per-field diff on the JSONB payload
--      (using `to_jsonb(b) - <lineage cols>`).
--   3. Aggregate across all pairs to produce a field-driver histogram.
--
-- How to read the results:
--   Q1 — Field-level diff count. If one field dominates (n_diff ≈
--        total_pairs) and others are near zero, the wave is single-field
--        cosmetic churn (ADR-0032-analog candidate).
--        If multiple fields show high counts, the wave is multi-field
--        and may reflect substantive edits.
--   Q2 — "Fields changed per pair" distribution. Bucket showing how many
--        pairs differ on exactly 1 field vs 2 vs 3+ — confirms whether
--        the single-field hypothesis applies to MOST pairs or just some.
--   Q3 — Sample pairs that differ on the MOST fields. Surfaces outliers
--        that might reflect genuine recall amendments hiding inside an
--        otherwise cosmetic wave.
--
-- Pass the run_id via -v run_id='<uuid>'. Defaults to most recent
-- successful usda run.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\if :{?run_id}
\echo
\echo === Using passed run_id: :run_id ===
\else
select run_id as run_id
from extraction_runs
where source = 'usda' and status = 'success'
order by started_at desc
limit 1
\gset
\echo
\echo === Defaulted to most recent successful usda run: :run_id ===
\endif

select started_at as run_started_at
from extraction_runs
where run_id = :'run_id'
\gset

\echo Run started_at: :run_started_at
\echo

-- Build the (new, old) payload pairs once as a temp table so all three
-- queries below reference the same denominator without re-joining bronze.
drop table if exists wave_pairs;
create temp table wave_pairs as
with new_versions as (
    select
        source_recall_id, langcode,
        extraction_timestamp,
        to_jsonb(b)
            - 'id' - 'content_hash' - 'extraction_timestamp' - 'raw_landing_path'
            as payload
    from usda_fsis_recalls_bronze b
    where extraction_timestamp >= :'run_started_at'::timestamptz
),
old_versions as (
    select distinct on (source_recall_id, langcode)
        source_recall_id, langcode,
        extraction_timestamp,
        to_jsonb(b)
            - 'id' - 'content_hash' - 'extraction_timestamp' - 'raw_landing_path'
            as payload
    from usda_fsis_recalls_bronze b
    where extraction_timestamp < :'run_started_at'::timestamptz
    order by source_recall_id, langcode, extraction_timestamp desc
)
select
    n.source_recall_id,
    n.langcode,
    n.payload as new_payload,
    o.payload as old_payload
from new_versions n
join old_versions o using (source_recall_id, langcode);

\echo '=== Pairs constructed (denominator for the queries below) ==='
select count(*) as total_pairs from wave_pairs;

\echo
\echo '=== Q1: per-field diff count across all pairs in this run ==='
\echo 'For each JSONB key, count how many of the total_pairs have a diff'
\echo 'on that key. n_diff = pairs where the field changed; pct_pairs_changed'
\echo 'is the share of the wave attributable to that field.'

-- Use jsonb_each on the new payload to enumerate keys (same schema
-- across all pairs, so any one pair's key set is exhaustive).
with all_keys as (
    select distinct k
    from wave_pairs,
         lateral jsonb_object_keys(new_payload) as k
),
diff_counts as (
    select
        k,
        count(*) filter (
            where (new_payload -> k) is distinct from (old_payload -> k)
        ) as n_diff,
        count(*) as n_total
    from wave_pairs, all_keys
    group by k
)
select
    k                                              as field,
    n_diff,
    n_total                                        as total_pairs,
    round(100.0 * n_diff / nullif(n_total, 0), 1)  as pct_pairs_changed
from diff_counts
where n_diff > 0
order by n_diff desc;

\echo
\echo '=== Q2: distribution of "number of fields changed per pair" ==='
\echo 'Bucket pairs by how many fields differ between new and old.'
\echo 'If the wave is single-field cosmetic, most pairs sit in the 1-field bucket.'
\echo 'A long tail at 3+ fields would suggest substantive edits hiding in the wave.'

with pair_diffs as (
    select
        source_recall_id,
        langcode,
        count(*) filter (
            where (new_payload -> k) is distinct from (old_payload -> k)
        ) as n_fields_changed
    from wave_pairs,
         lateral jsonb_object_keys(new_payload) as k
    group by source_recall_id, langcode, new_payload, old_payload
)
select
    n_fields_changed,
    count(*) as n_pairs,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from pair_diffs
group by n_fields_changed
order by n_fields_changed;

\echo
\echo '=== Q3: top 5 pairs ranked by number of fields changed ==='
\echo 'These are the outliers most likely to reflect substantive recall'
\echo 'amendments rather than cosmetic churn. The `changed_fields` column'
\echo 'lists which keys diverged so you can spot-check a real edit pattern.'

with pair_diffs as (
    select
        source_recall_id,
        langcode,
        new_payload,
        old_payload,
        array_agg(k order by k) filter (
            where (new_payload -> k) is distinct from (old_payload -> k)
        ) as changed_fields,
        count(*) filter (
            where (new_payload -> k) is distinct from (old_payload -> k)
        ) as n_fields_changed
    from wave_pairs,
         lateral jsonb_object_keys(new_payload) as k
    group by source_recall_id, langcode, new_payload, old_payload
)
select
    source_recall_id,
    langcode,
    n_fields_changed,
    changed_fields
from pair_diffs
order by n_fields_changed desc, source_recall_id
limit 5;

drop table wave_pairs;
