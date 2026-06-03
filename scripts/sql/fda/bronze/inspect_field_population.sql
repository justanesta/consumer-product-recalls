-- Phase 6 (feature/silver-field-remap, W1) — FDA bronze field-population & shape profiling.
--
-- CAVEAT: this is population / shape / enum / length / relationship EVIDENCE for the
-- silver remap's NOT NULL, accepted_values, canonical-name, and length-sizing decisions.
-- It is NOT a re-derivation of which fields to capture — that field-selection audit is
-- Phase 6a, done (see documentation/fda/field_audit_2026_w22.md and
-- documentation/audit/methodology.md "bronze is not the starting point"). An empty-in-
-- bronze finding describes the capture state, never a capture-removal recommendation.
--
-- When to run: against the full-corpus fda_recalls_bronze (Phase 6a.5 seed complete
-- 2026-06-02, ~134,450 distinct products). Sibling of explore_bronze_shape.sql, which
-- already covers cardinality / cadence / dedup / center+product+phase distributions /
-- null rates; this file adds the three things the remap needs and that one lacks:
-- narrative length stats (L), the enum domains it doesn't cover (D), and cross-column
-- relationships (R).
--
-- FDA preserves BOTH null and '' as null sentinels for the same text fields (Finding J);
-- silver staging normalizes via nullif(col, '') per ADR 0027. So population/length here
-- treat '' as empty via nullif(col, '') to match what silver will see.
--
-- Output feeds: documentation/fda/field_audit_2026_w22.md §8 (corpus-scale re-validation)
-- + documentation/audit/bronze_corpus_profile.md (cross-source population + enum matrices).
-- Each Qn block is cited by number in those docs.
--
-- Run with: psql ... -f scripts/sql/fda/bronze/inspect_field_population.sql

\echo '=== Q1: length stats for narrative text fields (Bug 1/2/3 length evidence) ==='
-- product_short_reason_txt is the *defect reason* full-text (misnamed "short");
-- product_description_txt is the product narrative; distribution_area_summary_txt is a
-- geographic distribution list currently mis-sourced into recall_event.description (Bug 1).
-- The length distribution discriminates narrative vs name vs list and sizes silver columns.
-- '' is treated as empty via nullif so a blank doesn't register as length 0.
select
  'distribution_area_summary_txt' as field,
  count(*) filter (where nullif(distribution_area_summary_txt, '') is not null) as non_empty,
  round(100.0 * count(*) filter (where nullif(distribution_area_summary_txt, '') is null) / count(*), 1) as pct_empty,
  min(length(nullif(distribution_area_summary_txt, ''))) as min_len,
  round(avg(length(nullif(distribution_area_summary_txt, '')))) as avg_len,
  percentile_cont(0.5) within group (order by length(nullif(distribution_area_summary_txt, ''))) as p50_len,
  percentile_cont(0.95) within group (order by length(nullif(distribution_area_summary_txt, ''))) as p95_len,
  max(length(nullif(distribution_area_summary_txt, ''))) as max_len
from fda_recalls_bronze
union all
select
  'product_short_reason_txt',
  count(*) filter (where nullif(product_short_reason_txt, '') is not null),
  round(100.0 * count(*) filter (where nullif(product_short_reason_txt, '') is null) / count(*), 1),
  min(length(nullif(product_short_reason_txt, ''))),
  round(avg(length(nullif(product_short_reason_txt, '')))),
  percentile_cont(0.5) within group (order by length(nullif(product_short_reason_txt, ''))),
  percentile_cont(0.95) within group (order by length(nullif(product_short_reason_txt, ''))),
  max(length(nullif(product_short_reason_txt, '')))
from fda_recalls_bronze
union all
select
  'product_description_txt',
  count(*) filter (where nullif(product_description_txt, '') is not null),
  round(100.0 * count(*) filter (where nullif(product_description_txt, '') is null) / count(*), 1),
  min(length(nullif(product_description_txt, ''))),
  round(avg(length(nullif(product_description_txt, '')))),
  percentile_cont(0.5) within group (order by length(nullif(product_description_txt, ''))),
  percentile_cont(0.95) within group (order by length(nullif(product_description_txt, ''))),
  max(length(nullif(product_description_txt, '')))
from fda_recalls_bronze
union all
select
  'firm_legal_nam',
  count(*) filter (where nullif(firm_legal_nam, '') is not null),
  round(100.0 * count(*) filter (where nullif(firm_legal_nam, '') is null) / count(*), 1),
  min(length(nullif(firm_legal_nam, ''))),
  round(avg(length(nullif(firm_legal_nam, '')))),
  percentile_cont(0.5) within group (order by length(nullif(firm_legal_nam, ''))),
  percentile_cont(0.95) within group (order by length(nullif(firm_legal_nam, ''))),
  max(length(nullif(firm_legal_nam, '')))
from fda_recalls_bronze;

\echo ''
\echo '=== Q2: voluntary_type_txt distribution (canonicalization evidence) ==='
-- The audit observed two surface forms for the same concept (e.g. 'Firm Initiated' vs
-- 'Voluntary: Firm Initiated'). The corpus distribution decides the silver normalize
-- rule in recall_event.recall_initiator and the accepted_values list. '' kept visible.
select voluntary_type_txt, count(*) as rows
from fda_recalls_bronze
group by voluntary_type_txt
order by rows desc;

\echo ''
\echo '=== Q3: center_classification_type_txt distribution (enum domain) ==='
-- The severity classification ('1'/'2'/'3'/'NC' per the API). Full corpus value set is
-- the SSOT for the staging accepted_values test and the cross-source severity alignment.
select center_classification_type_txt, count(*) as rows
from fda_recalls_bronze
group by center_classification_type_txt
order by rows desc;

\echo ''
\echo '=== Q4: initial_firm_notification_txt distribution (enum domain) ==='
-- Notification method enum (Letter / E-Mail / Telephone / Press Release / Combination /
-- Other per the Definitions PDF). Corpus value set → accepted_values (warn) list.
select initial_firm_notification_txt, count(*) as rows
from fda_recalls_bronze
group by initial_firm_notification_txt
order by rows desc;

\echo ''
\echo '=== Q5: termination_dt populated vs phase_txt (relationship evidence) ==='
-- Expectation: termination_dt is populated iff the recall reached a terminated/closed
-- phase. Drives a conditional singular dbt test (termination_dt present ⇒ phase terminal).
-- A nonzero without_termination_dt on a terminal phase, or a populated termination_dt on
-- an Ongoing phase, is the anomaly count that sets the test's warn threshold.
select
  phase_txt,
  count(*) as rows,
  count(termination_dt) as with_termination_dt,
  count(*) - count(termination_dt) as without_termination_dt
from fda_recalls_bronze
group by phase_txt
order by rows desc;

\echo ''
\echo '=== Q6: null recall_num vs classification (NC-correlation, conditional-required) ==='
-- The audit notes null recall_num tracks center_classification_type_txt = 'NC'
-- (not-classified). Confirms whether recall_num can be conditionally required (hard
-- not_null where classified, warn-tripwire overall).
select
  center_classification_type_txt,
  count(*) as rows,
  count(*) filter (where nullif(recall_num, '') is null) as empty_recall_num,
  round(100.0 * count(*) filter (where nullif(recall_num, '') is null) / count(*), 1) as pct_empty
from fda_recalls_bronze
group by center_classification_type_txt
order by rows desc;

\echo ''
\echo '=== Q7: distribution_area_summary_txt content samples (Bug 1 smoking gun) ==='
-- Confirms this column holds geographic DISTRIBUTION text, not a product reason — the
-- direct evidence that recall_event.description <- distribution_area_summary_txt is wrong
-- and must move to its own distribution_area_summary column. Expect 'Nationwide',
-- state-list, 'Distribution in ...' shapes to dominate.
select left(nullif(distribution_area_summary_txt, ''), 80) as sample_first_80, count(*) as rows
from fda_recalls_bronze
where nullif(distribution_area_summary_txt, '') is not null
group by left(nullif(distribution_area_summary_txt, ''), 80)
order by rows desc
limit 15;

\echo ''
\echo '=== Q8: product_short_reason_txt content samples (correct recall_reason source) ==='
-- Confirms this column holds defect/reason narrative — the correct source for the renamed
-- recall_event.recall_reason. Reading these alongside Q7 makes the Bug 1 swap self-evident.
select left(nullif(product_short_reason_txt, ''), 80) as sample_first_80, count(*) as rows
from fda_recalls_bronze
where nullif(product_short_reason_txt, '') is not null
group by left(nullif(product_short_reason_txt, ''), 80)
order by rows desc
limit 15;
