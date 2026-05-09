-- Phase 5c follow-up — assert that USDA's bilingual EN/ES siblings
-- are atomically updated by FSIS.
--
-- Context: USDA FSIS publishes most recalls in two language versions
-- (English + Spanish), keyed in bronze by `(source_recall_id,
-- langcode)` per `src/extractors/usda.py:311-318` (ADR 0006). The
-- documented assumption (per
-- `documentation/usda/recall_api_observations.md:163-164`) is that
-- FSIS updates EN and ES atomically — both rows have identical
-- `last_modified_date` and content edits flow to both within the same
-- update window.
--
-- ADR 0026 (lifecycle tracking) references "the 13.3% bilingual
-- non-atomic-update rate" as motivation for treating lifecycle
-- transitions carefully, but that figure has not been re-computed
-- against current bronze. This assertion measures the actual rate.
--
-- Why it matters: ADR 0026's silver `recall_event_history` model
-- (Phase 6 deliverable) currently plans to track lifecycle per
-- `source_recall_id`. If EN/ES are non-atomic at a meaningful rate,
-- lifecycle events must be tracked per `(source_recall_id, langcode)`
-- — a structural change to the Phase 6 model. ADR 0006 is also
-- affected: bilingual-pairing invariants assume atomic updates when
-- detecting orphan Spanish records.
--
-- Strategy: take the latest row per `(source_recall_id, langcode)`,
-- pivot to one row per `source_recall_id` with EN+ES columns, and
-- count cases where:
--   (a) `last_modified_date` differs between EN and ES (timestamp
--       atomicity violation), OR
--   (b) `content_hash` differs in a way that wouldn't be expected
--       from translation alone (this script doesn't try to model
--       "expected differences from translation" — it just reports
--       atomicity rate on the timestamp signal, which is the proxy
--       FSIS itself documents as the synchronization mechanism).
--
-- The "latest row per (source_recall_id, langcode)" projection
-- mirrors the staging-model dedup pattern at
-- `dbt/models/staging/stg_cpsc_recalls.sql:8-15`.
--
-- Recall-only scope: this assertion considers only `source_recall_id`
-- values that have BOTH an EN row and an ES row (`has_spanish=true`
-- and an actual ES sibling present). Spanish-orphans and English-only
-- records are filtered out — they're handled by ADR 0006's
-- quarantine path, not this assertion.
--
-- Expected outcome: rate close to the 13.3% figure cited in ADR 0026.
-- A meaningfully higher rate (e.g., >25%) suggests the atomic-update
-- assumption is weaker than ADR 0026 documented and Phase 6 needs
-- to plan accordingly. A rate of 0% would mean FSIS has tightened
-- atomicity since the original observation.
--
-- Wire-up: also exercised via dbt singular test
-- `dbt/tests/source_assumptions/assert_usda_bilingual_atomic_update.sql`
-- at severity=warn.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: bilingual atomicity rate (last_modified_date axis) ==='
\echo 'For each recall with both EN and ES rows present in bronze, take the'
\echo 'latest version per language and check whether last_modified_date matches.'
\echo 'non_atomic_rate is the headline figure for ADR 0026 calibration.'

with latest as (
    select
        source_recall_id,
        langcode,
        last_modified_date,
        content_hash,
        row_number() over (
            partition by source_recall_id, langcode
            order by extraction_timestamp desc
        ) as rn
    from usda_fsis_recalls_bronze
),
latest_per_pair as (
    select source_recall_id, langcode, last_modified_date, content_hash
    from latest
    where rn = 1
),
pivoted as (
    select
        en.source_recall_id,
        en.last_modified_date as en_last_modified_date,
        es.last_modified_date as es_last_modified_date,
        en.content_hash       as en_content_hash,
        es.content_hash       as es_content_hash
    from latest_per_pair en
    join latest_per_pair es
        on en.source_recall_id = es.source_recall_id
       and en.langcode = 'English'
       and es.langcode = 'Spanish'
)
select
    count(*)                                                            as bilingual_pair_count,
    count(*) filter (where en_last_modified_date is distinct from es_last_modified_date) as non_atomic_pair_count,
    round(
        100.0 * count(*) filter (where en_last_modified_date is distinct from es_last_modified_date)
        / nullif(count(*), 0)::numeric, 2
    )                                                                   as non_atomic_rate_percent
from pivoted;

\echo
\echo '=== Q2: sample non-atomic pairs (up to 10) ==='
\echo 'Cases where EN and ES last_modified_date diverge. Inspect the date gap'
\echo 'to assess whether FSIS catches up within hours, days, or never.'

with latest as (
    select
        source_recall_id,
        langcode,
        last_modified_date,
        content_hash,
        row_number() over (
            partition by source_recall_id, langcode
            order by extraction_timestamp desc
        ) as rn
    from usda_fsis_recalls_bronze
),
latest_per_pair as (
    select source_recall_id, langcode, last_modified_date, content_hash
    from latest
    where rn = 1
)
select
    en.source_recall_id,
    en.last_modified_date as en_last_modified,
    es.last_modified_date as es_last_modified,
    en.last_modified_date - es.last_modified_date as en_minus_es_interval,
    case when en.content_hash = es.content_hash then 'same' else 'different' end as content_hash_relationship
from latest_per_pair en
join latest_per_pair es
    on en.source_recall_id = es.source_recall_id
   and en.langcode = 'English'
   and es.langcode = 'Spanish'
where en.last_modified_date is distinct from es.last_modified_date
order by abs(extract(epoch from (en.last_modified_date - es.last_modified_date))) desc nulls last
limit 10;

\echo
\echo '=== Q3: bilingual-corpus shape (context) ==='
\echo 'How many recalls have EN-only, ES-only, both. Used as denominator and'
\echo 'sanity-check for Q1 vs. ADR 0006 quarantine semantics.'

with latest as (
    select source_recall_id, langcode,
           row_number() over (partition by source_recall_id, langcode
                              order by extraction_timestamp desc) as rn
    from usda_fsis_recalls_bronze
),
languages_per_recall as (
    select source_recall_id,
           bool_or(langcode = 'English') as has_en,
           bool_or(langcode = 'Spanish') as has_es
    from latest
    where rn = 1
    group by source_recall_id
)
select
    count(*) filter (where has_en and has_es)         as both_en_and_es,
    count(*) filter (where has_en and not has_es)     as english_only,
    count(*) filter (where not has_en and has_es)     as spanish_only,
    count(*)                                          as total_recalls
from languages_per_recall;
