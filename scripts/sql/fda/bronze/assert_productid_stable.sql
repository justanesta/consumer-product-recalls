-- Phase 5c follow-up — assert that FDA's PRODUCTID never renumbers
-- across bronze snapshots.
--
-- Context: FDA bronze identity is `(source_recall_id,)` where
-- `source_recall_id = PRODUCTID` (per `src/schemas/fda.py:88`). Silver
-- product surrogate is `md5('FDA' || '|' || source_recall_id)` per
-- `dbt/models/silver/recall_product.sql:58`. The entire FDA branch
-- of silver depends on PRODUCTID being a permanent, contractual
-- identifier — FDA documents it as such.
--
-- ADR 0031:82 sets the threshold at "any non-zero rate" for triggering
-- Phase 6 reconciliation. PRODUCTID renumbering would be catastrophic:
-- every silver `recall_product_id` for an affected product would
-- fragment, and the lineage chain in `recall_event_history` would lose
-- continuity for that record.
--
-- Why it matters: this is the F1 assumption from the source-stability
-- audit. FDA has already partially violated F2 (EVENTLMD bumped by
-- archive migration per ADR 0023); F1 is the remaining linchpin
-- assumption keeping FDA silver coherent. If F1 falls too, the FDA
-- silver model needs structural redesign.
--
-- Strategy: detection is hard because PRODUCTID is itself the natural
-- key — there's no upstream "true identity" to compare against. We
-- approximate by looking at (recall_event_id, product_description_txt,
-- recall_num) — the would-be natural key if PRODUCTID didn't exist —
-- and counting distinct PRODUCTIDs per group. >1 PRODUCTID for the same
-- (event, description, recall_num) across runs is a strong renumber
-- signal. False positives are possible (two genuinely-different
-- products with coincidentally-similar fields); the sample query (Q2)
-- includes EVENTLMD/extraction_timestamp context to help disambiguate.
--
-- Caveat: this assertion is structurally weaker than NHTSA's because
-- there's no "second corpus" to compare PRODUCTID assignment against.
-- Strengthening would require capturing PRODUCTLMD (currently absent
-- from `fda_recalls_bronze` schema) so per-record edit timestamps can
-- be cross-checked. Filed as a follow-up in
-- `documentation/source_assumption_audit.md`.
--
-- Expected outcome on a clean corpus: drift_group_count = 0.
-- Non-zero results require manual investigation per group:
--   (a) confirmed PRODUCTID renumber → catastrophic; trigger Phase 6
--       FDA silver redesign per ADR 0031, OR
--   (b) coincidental collision on the would-be natural key → tighten
--       the candidate-key tuple in this assertion (add center_cd,
--       firm_legal_nam, etc.) and re-run.
--
-- Wire-up: also exercised via dbt singular test
-- `dbt/tests/source_assumptions/assert_fda_productid_stable.sql`
-- at severity=warn.

\set ON_ERROR_STOP on
\pset null '<NULL>'

\echo
\echo '=== Q1: PRODUCTID stability headline assertion ==='
\echo 'drift_group_count = 0 means no (recall_event_id, product_description_txt,'
\echo 'recall_num) candidate key has been observed with >1 distinct PRODUCTID'
\echo 'across runs. Non-zero requires per-group investigation via Q2.'

select count(*) as drift_group_count
from (
    select recall_event_id, product_description_txt, recall_num
    from fda_recalls_bronze
    group by recall_event_id, product_description_txt, recall_num
    having count(distinct source_recall_id) > 1
       and count(distinct raw_landing_path) > 1
) g;

\echo
\echo '=== Q2: candidate PRODUCTID-renumber groups (up to 10) ==='
\echo 'For each (recall_event_id, description, recall_num) seen with multiple'
\echo 'PRODUCTIDs, list the PRODUCTIDs and EVENTLMD ranges. Use this to triage'
\echo 'genuine renumbers vs. coincidental natural-key collisions.'

select
    recall_event_id,
    product_description_txt,
    recall_num,
    string_agg(distinct source_recall_id, ' | ' order by source_recall_id) as distinct_productids,
    min(event_lmd) as min_event_lmd,
    max(event_lmd) as max_event_lmd,
    count(distinct raw_landing_path) as n_landing_paths,
    count(*) as n_rows
from fda_recalls_bronze
group by recall_event_id, product_description_txt, recall_num
having count(distinct source_recall_id) > 1
   and count(distinct raw_landing_path) > 1
limit 10;

\echo
\echo '=== Q3: PRODUCTID corpus shape (context) ==='
\echo 'How many distinct PRODUCTIDs vs. how many bronze rows. Disparities'
\echo 'are normal (one PRODUCTID has multiple snapshots over time as content'
\echo 'edits accumulate); used as denominator when interpreting Q1.'

select
    count(*)                              as bronze_row_count,
    count(distinct source_recall_id)      as distinct_productids,
    count(distinct recall_event_id)       as distinct_recall_events,
    count(distinct raw_landing_path)      as distinct_runs
from fda_recalls_bronze;
