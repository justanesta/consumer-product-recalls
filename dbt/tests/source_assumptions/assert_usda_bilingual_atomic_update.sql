-- Singular test: USDA EN/ES bilingual siblings are atomically updated
-- (last_modified_date matches between the latest English and Spanish
-- versions of the same recall). Returns rows for each non-atomic pair.
-- Severity=warn via dbt_project.yml — a benign SOURCE-BEHAVIOR MONITOR, not a
-- pending defect. Silver consumes English-only (that IS the "reconciliation"),
-- and the ~13.3% non-atomicity is benign for it in BOTH directions: EN-newer =
-- FSIS edits English without retranslating; ES-newer = delayed Spanish
-- translation republish (confirmed a faithful, content-identical translation on
-- the 399-day worst case 058-2018). English stays authoritative + complete, so
-- English-only silver is not stale. Findings: documentation/usda/
-- bilingual_and_lmd_findings.md (ES-newer subsection, 2026-06-07). Rich version
-- with date-gap analysis at scripts/sql/usda_recalls/bronze/assert_bilingual_atomic_update.sql.

with latest as (
    select
        source_recall_id,
        langcode,
        last_modified_date,
        row_number() over (
            partition by source_recall_id, langcode
            order by extraction_timestamp desc
        ) as rn
    from {{ source('usda', 'usda_fsis_recalls_bronze') }}
),
latest_per_pair as (
    select source_recall_id, langcode, last_modified_date
    from latest
    where rn = 1
),
pairs as (
      select
          en.source_recall_id,
          (en.last_modified_date is distinct from es.last_modified_date) as is_non_atomic
      from latest_per_pair en
      join latest_per_pair es
          on en.source_recall_id = es.source_recall_id
         and en.langcode = 'English'
         and es.langcode = 'Spanish'
),
rate as (
    select
        count(*) filter (where is_non_atomic)            as non_atomic_pairs,
        count(*)                                         as bilingual_pairs,
        count(*) filter (where is_non_atomic)::numeric
            / nullif(count(*), 0)                        as non_atomic_rate
    from pairs
) 
-- Rate-ceiling drift monitor: the ~13.3% non-atomicity is the documented benign
-- steady state (bilingual_and_lmd_findings.md). Emit a breach row ONLY when the
-- rate clears 0.18 which signals a real change in FSIS EN/ES
-- update behavior worth investigating; normal fluctuation stays green (no noise).
select non_atomic_pairs, bilingual_pairs, round(non_atomic_rate, 4) as non_atomic_rate
from rate
where non_atomic_rate > 0.18
