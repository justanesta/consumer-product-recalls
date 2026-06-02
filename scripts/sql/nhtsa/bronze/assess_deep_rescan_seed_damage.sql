-- NHTSA deep-rescan seed-damage assessment (READ-ONLY — mutates nothing).
--
-- Context: the deep-rescan historical_seed was run with the pre-fix
-- NhtsaDeepRescanLoader, which keyed dedup on the regen-unstable RECORD_ID
-- (source_recall_id) AND hashed it — see ADR 0030 "Amendment (2026-06-01)" and
-- documentation/audit/src_soundness_audit.md. Every seeded row therefore carries
-- a content_hash that includes RECORD_ID; the next CORRECT incremental run (which
-- excludes RECORD_ID from the hash) would cascade-duplicate the overlapping
-- corpus. This script sizes the damage BEFORE any mutation.
--
-- Read the four result sets, then decide:
--   • If the seed run's status = 'failed'  → the engine.begin() txn rolled back,
--     no rows landed → NO data remediation needed (just land the code fix).
--   • Otherwise (status = 'success' with rows in Q2) → proceed to
--     reset_nhtsa_bronze.sql (truncate + re-seed with the fixed loader).
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/bronze/assess_deep_rescan_seed_damage.sql

\echo '== Q1: recent NHTSA extraction_runs (the historical_seed is the suspect) =='
select id, started_at, finished_at, status, change_type,
       records_extracted, records_inserted, raw_landing_path
from extraction_runs
where source = 'nhtsa'
order by started_at desc
limit 10;

\echo '== Q2: bronze totals (one distinct landing path ⇒ corroborates empty-before-seed) =='
select count(*)                          as total_rows,
       count(distinct raw_landing_path)  as distinct_landing_paths,
       count(distinct content_hash)      as distinct_content_hashes
from nhtsa_recalls_bronze;

\echo '== Q3: rows per landing path (the buggy seed should be the only / dominant one) =='
select raw_landing_path, count(*) as rows
from nhtsa_recalls_bronze
group by raw_landing_path
order by rows desc;

\echo '== Q4: duplication under the CORRECT 11-tuple oracle (ADR 0030) =='
-- The fixed oracle keys on the 11-tuple and excludes RECORD_ID. Any 11-tuple
-- group with >1 row is a duplicate the correct loader would have collapsed
-- (the ~0.7% TSV byte-duplicate set, Finding L) but the buggy single-column
-- loader retained. excess_duplicate_rows = the inflation the truncate+reseed
-- will remove. On an empty-before seed there are NO phantom-vs-incremental
-- rows; this is the only duplication mechanism in play.
with grouped as (
    select campno, maketxt, modeltxt, yeartxt, compname, rcl_cmpt_id,
           mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
           count(*) as rows_in_group
    from nhtsa_recalls_bronze
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
select count(*)                                    as distinct_logical_rows,
       coalesce(sum(rows_in_group), 0)             as total_rows,
       coalesce(sum(rows_in_group) - count(*), 0)  as excess_duplicate_rows,
       count(*) filter (where rows_in_group > 1)   as groups_with_dups
from grouped;
