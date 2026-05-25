-- Probe — confirm the exact storage shape of source_recall_id values in
-- usda_fsis_recalls_bronze. Wraps each value in brackets so hidden
-- leading / trailing whitespace shows up, and reports length to compare
-- against the visible-character count.
--
-- Motivating observation (2026-05-15): `diagnose_payload_drift_for_recall.sql`
-- with `\set recall_id '021-2020'` returned 0 rows despite the same
-- recall_id appearing in `assert_field_last_modified_date_advances_on_edit.sql`'s
-- Q2 sample. Column-width analysis suggested values like `021-2020` may
-- be stored as ` 021-2020` (with a leading space character) while
-- values like `001-2014` are stored cleanly.
--
-- If this probe confirms the hypothesis:
--   * Decide whether to (a) `strip_whitespace=True` on the Pydantic
--     `source_recall_id` field in `src/schemas/usda.py`, OR
--   * (b) Add a `trim()` wrapper to the diagnose/probe scripts as a
--     defensive read-side measure.
--   Option (a) is the right fix but requires a re-extract; option (b)
--   unblocks investigation now.
--
-- Pass the probe substring via -v probe='<substring>'. Defaults to
-- '021-2020' which is one of the candidates from the assert script
-- output where the leading-space hypothesis applies.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\if :{?probe}
\else
    \set probe '021-2020'
\endif

\echo
\echo === Probing source_recall_id storage for substring: :probe ===
\echo

\echo '=== Q1: distinct stored values matching the probe substring ==='
\echo 'Brackets reveal hidden whitespace; length tells you the true char count.'

select
    '[' || source_recall_id || ']'        as wrapped_id,
    length(source_recall_id)              as id_length,
    langcode,
    count(*)                              as snapshots,
    min(extraction_timestamp)             as first_seen,
    max(extraction_timestamp)             as last_seen
from usda_fsis_recalls_bronze
where source_recall_id like '%' || :'probe' || '%'
group by source_recall_id, langcode
order by source_recall_id, langcode;

\echo
\echo '=== Q2: whitespace contamination summary (corpus-wide) ==='
\echo 'How many distinct source_recall_id values have leading or trailing whitespace?'
\echo 'A non-zero count means the schema should strip whitespace at validation.'

with distinct_ids as (
    select distinct source_recall_id from usda_fsis_recalls_bronze
)
select
    count(*) filter (where source_recall_id <> trim(source_recall_id))   as ids_with_whitespace,
    count(*) filter (where source_recall_id <> ltrim(source_recall_id))  as ids_with_leading_ws,
    count(*) filter (where source_recall_id <> rtrim(source_recall_id))  as ids_with_trailing_ws,
    count(*)                                                              as total_distinct_ids
from distinct_ids;

\echo
\echo '=== Q3: sample contaminated values ==='
\echo 'Up to 10 distinct values whose trim() form differs from the stored form.'

select distinct
    '[' || source_recall_id || ']' as wrapped_id,
    length(source_recall_id)       as id_length,
    '[' || trim(source_recall_id) || ']' as trimmed_form,
    length(trim(source_recall_id)) as trimmed_length
from usda_fsis_recalls_bronze
where source_recall_id <> trim(source_recall_id)
order by wrapped_id
limit 10;
