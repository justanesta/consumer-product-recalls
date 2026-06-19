-- Verify the planner USES the mart_recall_summary.firms GIN for the firm-profile
-- GET /recalls?firm_id={id} containment filter — not just that the index exists.
--
-- Companion to scripts/sql/gold/audit_schema_and_indexes.sql §C (which only confirms the index is
-- PRESENT). This script proves the planner actually PICKS it: a Bitmap Index Scan on the firms GIN,
-- not a Seq Scan of the ~93k-row corpus. Added with the firms GIN (2026-06-19, website-frontend-plan
-- §5.4; documentation/index_audit.md addendum).
--
-- Read-only (EXPLAIN ANALYZE on a SELECT). Run it yourself:
--     psql -f scripts/sql/gold/verify_firms_gin_plan.sql
--
-- The predicate below is the sargable form the recalls-api should emit for ?firm_id: a jsonb `@>`
-- containment whose RHS folds to a constant (jsonb_build_* is immutable), so the GIN is eligible. The
-- literal equivalent is `firms @> '[{"firm_id":"<id>"}]'::jsonb`.

\timing on

-- 1) Pick a REAL, representative firm_id — one with a handful of recalls (5–100), i.e. the typical
--    firm-page case. At that selectivity (<=~0.1% of ~93k rows) the GIN bitmap scan is unambiguously the
--    right plan. (Note: for the very highest-cardinality firms — a few % of the table — the planner may
--    *correctly* flip to a seq-scan; that's the planner doing its job, not the index failing.)
select (elem ->> 'firm_id') as sample_firm_id
from mart_recall_summary,
     lateral jsonb_array_elements(firms) as elem
group by 1
having count(*) between 5 and 100
order by count(*) desc
limit 1
\gset

\echo '>>> Sampling a representative firm_id (5-100 recalls) =' :'sample_firm_id'

-- 2) ACTUAL plan with all access paths available. Expect: Bitmap Heap Scan -> Bitmap Index Scan on the
--    GIN over (firms), with a Recheck Cond on `firms @> ...`. A "Seq Scan on mart_recall_summary" here
--    would mean the GIN was NOT chosen (investigate stats / selectivity).
\echo '>>> [A] Planner-chosen plan (GIN should be used):'
explain (analyze, buffers)
select recall_event_id, source, published_at, primary_firm_name
from mart_recall_summary
where firms @> jsonb_build_array(jsonb_build_object('firm_id', :'sample_firm_id'));

-- 3) Forced seq-scan BASELINE for contrast (disable the bitmap path the GIN rides on). This is the
--    cost/time the firm page would pay on EVERY load WITHOUT the index — the before/after the perf
--    footnote describes. Compare the total runtime + Buffers against [A].
set enable_bitmapscan = off;
\echo '>>> [B] Forced seq-scan baseline (index path disabled) — the cost without the GIN:'
explain (analyze, buffers)
select recall_event_id, source, published_at, primary_firm_name
from mart_recall_summary
where firms @> jsonb_build_array(jsonb_build_object('firm_id', :'sample_firm_id'));
reset enable_bitmapscan;

\timing off
