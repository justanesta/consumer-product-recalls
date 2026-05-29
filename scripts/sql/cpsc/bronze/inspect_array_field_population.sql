-- Phase 6a foundation audit — CPSC nested-array field population.
--
-- Drills into the per-element fields inside CPSC's 13 JSONB array columns.
-- The companion Python inspect script
-- (scripts/cpsc/audit/inspect_landed_payloads.py) reports top-level field
-- statistics but cannot drill into nested keys (e.g., Manufacturers[].CompanyID)
-- because list-of-dict elements aren't naturally hashable for Counter()-based
-- distinct counts.
--
-- KEY-CASING NOTE (2026-05-29 — diagnosed during initial run):
-- The CPSC bronze JSONB columns store **snake_case** keys (`name`,
-- `company_id`, `hazard_type`, etc.), NOT the PascalCase keys the API returns.
-- This is because `src/bronze/loader.py` serializes the Pydantic model via
-- `model_dump(mode="json")` without `by_alias=True` — Pydantic defaults to
-- field-name serialization, and the field names in `src/schemas/cpsc.py`
-- are snake_case. The PascalCase API keys (`Name`, `CompanyID`, `HazardType`)
-- only exist in the R2 landed payload (raw API response); they DO NOT exist
-- in bronze. The Python inspect script runs against R2 (sees PascalCase);
-- this SQL runs against bronze (uses snake_case).
--
-- Production silver code already follows this convention — see
-- `dbt/models/silver/recall_product.sql:32-37` (`prod.value ->> 'name'`) and
-- `dbt/models/silver/firm.sql:53-55` (`firm_json ->> 'company_id'`).
--
-- Cross-script fix 2026-05-29: `scripts/sql/cpsc/bronze/explore_bronze_shape.sql`
-- Q9 had the same broken PascalCase pattern (`hazards->0->>'HazardType'`); its
-- result coincidentally aligned with Finding G's "always empty" conclusion so
-- the bug had been silent. Fixed alongside this script's updates.
--
-- These queries close that gap and validate the §9 R2 validation list in
-- documentation/cpsc/field_audit_2026_w22.md:
--   • CompanyID 100% empty across all 4 firm-role arrays (§3 Bug 3)
--   • Products[].Type + .CategoryID + .Description + .Model populated rates
--     (§7 proposed decision #7)
--   • Hazards[].HazardType + .HazardTypeID populated rates (§5 + Finding G)
--   • Images[].Caption populated rate (§1b — PDF drift)
--   • RemedyOptions[].Option enum cardinality (§4 lift design)
--   • ManufacturerCountries[].Country enum distribution (§4 lift design)
--   • Per-array element-count distribution (§9 cassette confirmation)
--   • Inconjunctions, Importers, Distributors empty rates (§4 lift design)
--
-- Sibling to scripts/sql/cpsc/bronze/explore_bronze_shape.sql which covers
-- scalar NULL rates + per-array empty rates (Q6, Q7) but does not drill
-- into nested keys.
--
-- Run with:
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/cpsc/bronze/inspect_array_field_population.sql

\echo '=== Q1: CompanyID emptiness across all 4 firm-role arrays ==='
-- Validates §3 Bug 3: CompanyID is empirically dead across Manufacturers,
-- Retailers, Importers, Distributors. The cassette shows CompanyID = '' across
-- all sampled records; this query confirms at corpus scale.
--
-- A row appears in 'pct_empty_companyid' for each role. If any role's
-- pct_empty < 100%, CompanyID has signal there and the §3 Bug 3 claim
-- needs refinement.
with elements as (
  select 'manufacturer' as role, m.value->>'company_id' as company_id
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  union all
  select 'retailer' as role, m.value->>'company_id' as company_id
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) m
  union all
  select 'importer' as role, m.value->>'company_id' as company_id
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  union all
  select 'distributor' as role, m.value->>'company_id' as company_id
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
)
select
  role,
  count(*) as elements,
  sum(case when company_id is null or company_id = '' then 1 else 0 end) as empty_companyid,
  round(100.0 * sum(case when company_id is null or company_id = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_empty_companyid,
  count(distinct company_id) filter (where company_id is not null and company_id <> '') as distinct_non_empty_values
from elements
group by role
order by role;

\echo ''
\echo '=== Q2: Products[] nested-field populated rates ==='
-- Per cassette: Products[].Description, .Model, .Type, .CategoryID all appear
-- as '' (empty string) across all 3 sampled records. Confirm at corpus scale
-- so the §7 proposed decision #7 (drop or document-as-empty) has empirical
-- backing.
--
-- Treats null and '' as empty. Most records have a single Products[] element
-- per Q3 below, so per-recall and per-element rates are nearly identical.
with elements as (
  select
    p.value->>'name' as name,
    p.value->>'description' as description,
    p.value->>'model' as model,
    p.value->>'type' as type,
    p.value->>'category_id' as category_id,
    p.value->>'number_of_units' as number_of_units
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(products, '[]'::jsonb)) p
)
select
  count(*) as total_product_elements,
  round(100.0 * sum(case when name is null or name = '' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_empty_name,
  round(100.0 * sum(case when description is null or description = '' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_empty_description,
  round(100.0 * sum(case when model is null or model = '' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_empty_model,
  round(100.0 * sum(case when type is null or type = '' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_empty_type,
  round(100.0 * sum(case when category_id is null or category_id = '' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_empty_category_id,
  round(100.0 * sum(case when number_of_units is null or number_of_units = '' then 1 else 0 end) / nullif(count(*), 0), 1) as pct_empty_number_of_units
from elements;

\echo ''
\echo '=== Q3: Products[] array length distribution (multi-product check at corpus scale) ==='
-- Validates the C2 array-stability assumption from
-- documentation/cpsc/array_stability_findings.md: as of 2026-05-08 all
-- observed CPSC recalls have Products length = 1, so the C2 append-only
-- assumption is vacuous. This query re-runs the check against current bronze.
-- If length > 1 appears, C2 is no longer vacuous and Phase 6 silver
-- surrogate-key recipe (ordinal-based) becomes load-bearing.
select
  jsonb_array_length(products) as product_count,
  count(*) as recalls,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from cpsc_recalls_bronze
where products is not null
group by jsonb_array_length(products)
order by product_count;

\echo ''
\echo '=== Q4: Hazards[] HazardType + HazardTypeID populated rates ==='
-- Validates Finding G: HazardType + HazardTypeID are always empty across
-- the original 1,191 hazard-bearing rows. Re-confirm at current corpus scale.
-- If either field ever populates, the Hazard= search parameter (currently
-- documented broken in bruno/cpsc/data_exploration/search_by_hazard.yml)
-- could become useful.
with elements as (
  select
    h.value->>'name' as name,
    h.value->>'hazard_type' as hazard_type,
    h.value->>'hazard_type_id' as hazard_type_id
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(hazards, '[]'::jsonb)) h
)
select
  count(*) as total_hazard_elements,
  round(100.0 * sum(case when name is null or name = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_empty_name,
  round(100.0 * sum(case when hazard_type is null or hazard_type = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_empty_hazard_type,
  round(100.0 * sum(case when hazard_type_id is null or hazard_type_id = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_empty_hazard_type_id,
  count(distinct hazard_type) filter (where hazard_type is not null and hazard_type <> '') as distinct_hazard_type_values,
  count(distinct hazard_type_id) filter (where hazard_type_id is not null and hazard_type_id <> '') as distinct_hazard_type_id_values
from elements;

\echo ''
\echo '=== Q5: Images[] Caption populated rate ==='
-- §1b notes Caption is positive drift since the 2018 PDF (PDF says Image{URL}
-- only; API actually returns Image{URL, Caption}). Cassette shows Caption
-- populated. Confirm at corpus scale for the landing-page lift in §4.
with elements as (
  select
    i.value->>'url' as url,
    i.value->>'caption' as caption
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(images, '[]'::jsonb)) i
)
select
  count(*) as total_image_elements,
  round(100.0 * sum(case when url is null or url = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_empty_url,
  round(100.0 * sum(case when caption is null or caption = '' then 1 else 0 end) / nullif(count(*), 0), 2) as pct_empty_caption
from elements;

\echo ''
\echo '=== Q6: RemedyOptions[] Option enum distribution ==='
-- §4 lift design needs to know if Option is a clean low-cardinality enum
-- (Refund / Repair / Replace observed in cassette) or has additional values
-- at corpus scale. If cardinality > 5 the lift goes as JSONB; <= 5 supports
-- text[] with accepted_values dbt test.
with elements as (
  select ro.value->>'option' as option_value
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(remedy_options, '[]'::jsonb)) ro
)
select
  option_value,
  count(*) as occurrences,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from elements
group by option_value
order by occurrences desc;

\echo ''
\echo '=== Q7: ManufacturerCountries[] Country distribution ==='
-- §4 lift design: Country distribution informs whether a text[] or JSONB
-- shape is best. Also surfaces any normalization issues (e.g., "China" vs
-- "Peoples Republic of China"). Cassette shows simple country names.
with elements as (
  select mc.value->>'country' as country
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturer_countries, '[]'::jsonb)) mc
)
select
  country,
  count(*) as occurrences,
  round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from elements
group by country
order by occurrences desc
limit 30;

\echo ''
\echo '=== Q8: Per-array element-count distribution (firm-role arrays + Hazards/Remedies) ==='
-- Cassette suggests "usually 1 element" for most arrays. Confirm at corpus
-- scale to inform the §6 firm-architecture decision: if Manufacturers
-- routinely has >1 element, Option B's filter+lift design needs to handle
-- multi-element narratives.
--
-- A separate row per array column; element_count = 0 means empty array.
with array_lengths as (
  select 'manufacturers' as col, jsonb_array_length(coalesce(manufacturers, '[]'::jsonb)) as n from cpsc_recalls_bronze
  union all
  select 'retailers', jsonb_array_length(coalesce(retailers, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'importers', jsonb_array_length(coalesce(importers, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'distributors', jsonb_array_length(coalesce(distributors, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'hazards', jsonb_array_length(coalesce(hazards, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'remedies', jsonb_array_length(coalesce(remedies, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'remedy_options', jsonb_array_length(coalesce(remedy_options, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'images', jsonb_array_length(coalesce(images, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'injuries', jsonb_array_length(coalesce(injuries, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'inconjunctions', jsonb_array_length(coalesce(in_conjunctions, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'manufacturer_countries', jsonb_array_length(coalesce(manufacturer_countries, '[]'::jsonb)) from cpsc_recalls_bronze
  union all
  select 'product_upcs', jsonb_array_length(coalesce(product_upcs, '[]'::jsonb)) from cpsc_recalls_bronze
)
select
  col,
  n as element_count,
  count(*) as recalls,
  round(100.0 * count(*) / sum(count(*)) over (partition by col), 2) as pct_of_col
from array_lengths
group by col, n
order by col, n;

\echo ''
\echo '=== Q9: All 13 array columns — empty rates (complement to explore_bronze_shape.sql Q7) ==='
-- explore_bronze_shape.sql Q7 covers 6 of the 13 array columns (products,
-- hazards, remedies, manufacturers, retailers, manufacturer_countries). This
-- query reports all 13 in one place for the §4 lift NULL-rate inputs.
select
  round(100.0 * sum(case when products is null or products = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_products,
  round(100.0 * sum(case when manufacturers is null or manufacturers = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_manufacturers,
  round(100.0 * sum(case when retailers is null or retailers = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_retailers,
  round(100.0 * sum(case when importers is null or importers = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_importers,
  round(100.0 * sum(case when distributors is null or distributors = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_distributors,
  round(100.0 * sum(case when manufacturer_countries is null or manufacturer_countries = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_manufacturer_countries,
  round(100.0 * sum(case when product_upcs is null or product_upcs = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_product_upcs,
  round(100.0 * sum(case when hazards is null or hazards = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_hazards,
  round(100.0 * sum(case when remedies is null or remedies = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_remedies,
  round(100.0 * sum(case when remedy_options is null or remedy_options = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_remedy_options,
  round(100.0 * sum(case when in_conjunctions is null or in_conjunctions = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_inconjunctions,
  round(100.0 * sum(case when images is null or images = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_images,
  round(100.0 * sum(case when injuries is null or injuries = '[]'::jsonb then 1 else 0 end) / count(*), 2) as pct_empty_injuries
from cpsc_recalls_bronze;

\echo ''
\echo '=== Q10: Retailers[] Name length distribution (firm-dim pollution signal) ==='
-- Confirms §3 Bug 1: Retailers[].Name is a sales-channel narrative, not a
-- firm name. Long average length (cassette ~80-150 chars) suggests narrative;
-- short (<30 chars) suggests actual firm names. Compare to Manufacturers[]
-- Name length as a sanity check.
with elements as (
  select 'manufacturers' as col, m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(manufacturers, '[]'::jsonb)) m
  where (m.value->>'name') is not null and (m.value->>'name') <> ''
  union all
  select 'retailers', m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) m
  where (m.value->>'name') is not null and (m.value->>'name') <> ''
  union all
  select 'importers', m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(importers, '[]'::jsonb)) m
  where (m.value->>'name') is not null and (m.value->>'name') <> ''
  union all
  select 'distributors', m.value->>'name'
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(distributors, '[]'::jsonb)) m
  where (m.value->>'name') is not null and (m.value->>'name') <> ''
)
select
  col,
  count(*) as elements,
  min(length(name)) as min_len,
  round(avg(length(name)))::int as avg_len,
  max(length(name)) as max_len,
  count(distinct name) as distinct_names,
  round(100.0 * count(distinct name) / count(*), 1) as pct_distinct
from elements
group by col
order by col;

\echo ''
\echo '=== Q11: Sample Retailers[] Names (qualitative firm-dim pollution evidence) ==='
-- Pulls 10 distinct Retailers[].Name values to make the §3 Bug 1 finding
-- visible in the output. Expected: narrative strings starting with "Online
-- at", "Sold at", containing date ranges and prices.
with elements as (
  select distinct m.value->>'name' as name
  from cpsc_recalls_bronze, jsonb_array_elements(coalesce(retailers, '[]'::jsonb)) m
  where (m.value->>'name') is not null and (m.value->>'name') <> ''
)
select left(name, 200) as sample_name
from elements
order by random()
limit 10;
