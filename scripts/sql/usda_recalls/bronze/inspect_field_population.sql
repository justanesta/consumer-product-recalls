-- Phase 6 (feature/silver-field-remap, W1) — USDA recall bronze silver-remap profiling.
--
-- CAVEAT: population / length / enum / relationship EVIDENCE for the silver remap's
-- derive (risk_level), accepted_values (processing/recall_reason/classification),
-- length-sizing, and NOT NULL decisions. NOT a re-derivation of which fields to
-- capture — that field-selection audit is Phase 6a, done (see
-- documentation/usda/field_audit_2026_w22.md + documentation/audit/methodology.md).
-- An empty-in-bronze finding describes the capture state, never a capture-removal rec.
--
-- When to run: against the full-corpus usda_fsis_recalls_bronze. Sibling to
-- explore_usda_bronze.sql, which already covers (and should be RERUN alongside this
-- for the corpus refresh): cadence (Q1-4), edit detection (Q5-7), bilingual model
-- (Q8-10), enum distributions (Q11-15: recall_type/classification/reason/processing/
-- risk_level), active/archive cross-tabs (Q16-17), per-field null rates (Q19),
-- establishment + qty_recovered free-text samples (Q20-21). This file adds the four
-- things that script lacks and the remap needs: the risk_level↔classification
-- crosstab (derive proof), narrative length stats (L) for the lift fields, the
-- comma-separated multi-value prevalence + exploded-token enum SSOT, and an all-time
-- cadence companion (explore Q1-4 are recent-window only).
--
-- SILVER GRAIN + '' SENTINEL: silver staging (stg_usda_fsis_recalls.sql) consumes
-- ENGLISH-only, latest-per-identity, and normalizes '' → NULL via nullif (ADR 0027 —
-- USDA preserves '' as a missing-value sentinel). Every query below mirrors that:
-- a `latest_en` CTE (DISTINCT ON source_recall_id WHERE langcode='English') + nullif.
-- NOTE: explore_usda_bronze.sql Q19 counts SQL NULL only (not ''), so it UNDERSTATES
-- missingness on the '' -sentinel string fields (states/summary/distro_list/etc.);
-- the pct_empty here (nullif-based) is the silver-accurate figure and will read higher.
--
-- Output feeds: documentation/usda/field_audit_2026_w22.md §9 (corpus-scale re-val)
-- + documentation/audit/bronze_corpus_profile.md §2/§3/§4 (USDA grain/population/enum).
-- Each Qn block is cited by number in those docs.
--
-- Run with: psql ... -f scripts/sql/usda_recalls/bronze/inspect_field_population.sql

\echo '=== Q1: silver-grain snapshot (bronze rows vs English silver grain) ==='
-- Profile §1/§2 inputs. Business key = (source_recall_id, langcode); silver is
-- English-only, so distinct English recall_ids is the silver recall_event row count.
-- edit_version_rows = bronze rows beyond distinct identities (captured FSIS edits).
select
  (select count(*) from usda_fsis_recalls_bronze) as total_bronze_rows,
  (select count(distinct (source_recall_id, langcode)) from usda_fsis_recalls_bronze) as distinct_identities,
  (select count(*) - count(distinct (source_recall_id, langcode)) from usda_fsis_recalls_bronze) as edit_version_rows,
  (select count(distinct source_recall_id) from usda_fsis_recalls_bronze where langcode = 'English') as english_recalls_silver_grain,
  (select count(distinct source_recall_id) from usda_fsis_recalls_bronze where langcode = 'Spanish') as spanish_recalls;

\echo ''
\echo '=== Q2: risk_level × recall_classification crosstab (the DERIVE proof) ==='
-- §4/§5 decided risk_level is DERIVED from recall_classification (not lifted) because
-- they are 1:1. This confirms at corpus scale: risk_levels_per_classification must be
-- 1 on every row for the CASE-WHEN derive to be safe. Any value >1 breaks the derive
-- and forces a real lift. '' shown via nullif so a blank doesn't masquerade as a value.
with latest_en as (
  select distinct on (source_recall_id) *
  from usda_fsis_recalls_bronze
  where langcode = 'English'
  order by source_recall_id, extraction_timestamp desc
),
crosstab as (
  select
    coalesce(nullif(recall_classification, ''), '<empty>') as recall_classification,
    coalesce(nullif(risk_level, ''), '<empty>')            as risk_level,
    count(*)                                                as n
  from latest_en
  group by 1, 2
),
per_class as (
  -- crosstab already holds one row per distinct (classification, risk_level),
  -- so counting its rows per classification IS the distinct-risk-level count.
  -- (count(distinct ...) over () is unsupported as a window fn in Postgres.)
  select recall_classification, count(*) as risk_levels_per_classification
  from crosstab
  group by recall_classification
)
select
  c.recall_classification,
  c.risk_level,
  c.n,
  p.risk_levels_per_classification
from crosstab c
join per_class p using (recall_classification)
order by c.recall_classification, c.n desc;

\echo ''
\echo '=== Q3: length stats for the free-text lift fields (sizing + filename-vs-narrative) ==='
-- summary = HTML narrative (long); product_items = free-text SKU block (long, the
-- deferred-parse surface); distro_list = should be a SHORT pdf filename (confirms the
-- §4 rename to *_artifact_name, not narrative); qty_recovered/recall_reason/states/
-- company_media_contact = free text. '' treated as empty via nullif (silver-accurate).
with latest_en as (
  select distinct on (source_recall_id) *
  from usda_fsis_recalls_bronze
  where langcode = 'English'
  order by source_recall_id, extraction_timestamp desc
),
f as (
  select
    field,
    count(*) filter (where v is not null)                                          as non_empty,
    round(100.0 * count(*) filter (where v is null) / count(*), 1)                  as pct_empty,
    min(length(v))                                                                  as min_len,
    round(avg(length(v)))                                                           as avg_len,
    percentile_cont(0.5) within group (order by length(v))                         as p50_len,
    percentile_cont(0.95) within group (order by length(v))                        as p95_len,
    max(length(v))                                                                  as max_len
  from (
    select 'summary'               as field, nullif(summary, '')               as v from latest_en
    union all select 'product_items',        nullif(product_items, '')               from latest_en
    union all select 'distro_list',          nullif(distro_list, '')                 from latest_en
    union all select 'qty_recovered',        nullif(qty_recovered, '')               from latest_en
    union all select 'recall_reason',        nullif(recall_reason, '')               from latest_en
    union all select 'states',               nullif(states, '')                      from latest_en
    union all select 'company_media_contact', nullif(company_media_contact, '')      from latest_en
  ) s
  group by field
)
select * from f order by avg_len desc nulls last;

\echo ''
\echo '=== Q4: comma-separated multi-value prevalence (processing + recall_reason) ==='
-- Both are multi-valued comma-separated per R2 ("Raw - Intact, Raw - Non Intact";
-- "Misbranding, Unreported Allergens"). Silver preserves the comma-joined form
-- (structured array parse deferred to 6/7), so accepted_values tests must run on the
-- EXPLODED tokens (Q5/Q6), not the raw combinations. This sizes how much that matters.
with latest_en as (
  select distinct on (source_recall_id) *
  from usda_fsis_recalls_bronze
  where langcode = 'English'
  order by source_recall_id, extraction_timestamp desc
)
select
  'processing' as field,
  count(*) filter (where nullif(processing, '') is not null)                     as non_empty,
  count(*) filter (where processing like '%,%')                                  as multivalued_rows,
  round(100.0 * count(*) filter (where processing like '%,%')
    / nullif(count(*) filter (where nullif(processing, '') is not null), 0), 1)  as pct_multivalued,
  count(distinct nullif(processing, ''))                                         as distinct_raw_values
from latest_en
union all
select
  'recall_reason',
  count(*) filter (where nullif(recall_reason, '') is not null),
  count(*) filter (where recall_reason like '%,%'),
  round(100.0 * count(*) filter (where recall_reason like '%,%')
    / nullif(count(*) filter (where nullif(recall_reason, '') is not null), 0), 1),
  count(distinct nullif(recall_reason, ''))
from latest_en;

\echo ''
\echo '=== Q5: processing exploded-token distribution (accepted_values SSOT) ==='
-- Split the comma-separated processing on ',' and trim → the canonical single-value
-- taxonomy for the recall_product.type accepted_values test. distinct_raw (Q4) is
-- inflated by combinations; this is the real base set.
with latest_en as (
  select distinct on (source_recall_id) *
  from usda_fsis_recalls_bronze
  where langcode = 'English'
  order by source_recall_id, extraction_timestamp desc
),
tokens as (
  select trim(t) as token
  from latest_en, unnest(string_to_array(nullif(processing, ''), ',')) as t
  where nullif(processing, '') is not null
)
select token, count(*) as occurrences,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from tokens
where token <> ''
group by token
order by occurrences desc;

\echo ''
\echo '=== Q6: recall_reason exploded-token distribution (accepted_values SSOT) ==='
-- Same treatment for recall_reason → the canonical reason taxonomy (R2 saw ~9 base
-- values behind 26 raw combinations). Feeds recall_event.recall_reason test design.
with latest_en as (
  select distinct on (source_recall_id) *
  from usda_fsis_recalls_bronze
  where langcode = 'English'
  order by source_recall_id, extraction_timestamp desc
),
tokens as (
  select trim(t) as token
  from latest_en, unnest(string_to_array(nullif(recall_reason, ''), ',')) as t
  where nullif(recall_reason, '') is not null
)
select token, count(*) as occurrences,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from tokens
where token <> ''
group by token
order by occurrences desc;

\echo ''
\echo '=== Q7: all-time cadence by recall_year (historical span companion) ==='
-- explore_usda_bronze.sql Q1-4 are recent-window (16wk / 6mo); this is the full
-- historical span for the profile §1 snapshot — shows the archive tail distribution.
with latest_en as (
  select distinct on (source_recall_id) *
  from usda_fsis_recalls_bronze
  where langcode = 'English'
  order by source_recall_id, extraction_timestamp desc
)
select
  extract(year from recall_date)::int as recall_year,
  count(*) as recalls
from latest_en
group by 1
order by 1;
