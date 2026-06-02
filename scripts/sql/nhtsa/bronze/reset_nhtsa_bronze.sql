-- NHTSA bronze reset — post-buggy-deep-rescan-seed remediation (MUTATING).
--
-- Run AFTER assess_deep_rescan_seed_damage.sql confirms the buggy seed landed
-- rows (status != 'failed'), and AFTER the dedup-contract code fix is on the
-- branch you re-seed from. See ADR 0030 "Amendment (2026-06-01)" and
-- project_scope/src-consolidation-plan.md (Task 10).
--
-- Why a full TRUNCATE (not a targeted delete): the buggy historical_seed was the
-- first NHTSA load on the main branch (bronze was empty beforehand), so 100% of
-- nhtsa_recalls_bronze rows are buggy-seed output carrying RECORD_ID-polluted
-- content hashes. There is nothing clean to preserve. (If clean incremental rows
-- ever coexist with a bad run, isolate that run by its raw_landing_path instead —
-- the general lever; unnecessary here.)
--
-- NOTE: this is deliberately SEPARATE from _pipeline/truncate_for_dev_subset.sql,
-- whose tripwire ABORTS when a non-routine (historical_seed) run exists — exactly
-- the state this remediation targets. This script preserves the extraction_runs
-- audit trail (the incident record) and only truncates the polluted data + resets
-- the watermark.
--
-- Usage:
--   psql "$NEON_DATABASE_URL" -f scripts/sql/nhtsa/bronze/reset_nhtsa_bronze.sql

\set ON_ERROR_STOP on

\echo '== before: NHTSA bronze + rejected row counts =='
select 'bronze' as tbl, count(*) as rows from nhtsa_recalls_bronze
union all
select 'rejected' as tbl, count(*) as rows from nhtsa_recalls_rejected;

begin;

truncate table nhtsa_recalls_bronze, nhtsa_recalls_rejected;

-- Deep-rescan never advances the watermark, but clear it defensively so a clean
-- re-seed and the first incremental run after it start from a known-empty state.
-- No-op if the nhtsa watermark row doesn't exist yet.
update source_watermarks
set last_successful_extract_at = null,
    last_etag                  = null,
    last_cursor                = null,
    updated_at                 = now()
where source = 'nhtsa';

commit;

\echo '== after: NHTSA bronze + rejected row counts (expect 0 / 0) =='
select 'bronze' as tbl, count(*) as rows from nhtsa_recalls_bronze
union all
select 'rejected' as tbl, count(*) as rows from nhtsa_recalls_rejected;

\echo
\echo '================================================================'
\echo 'Done. extraction_runs history is intentionally preserved (incident audit).'
\echo 'Re-seed with the FIXED loader:'
\echo '  recalls deep-rescan nhtsa --change-type=historical_seed'
\echo 'Then run it a SECOND time and confirm 0 inserts (idempotence — the live'
\echo 'form of the tests/bronze/test_dedup_contracts.py RECORD_ID-churn guard).'
\echo '================================================================'
