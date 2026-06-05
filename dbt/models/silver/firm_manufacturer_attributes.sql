{{ config(materialized='table') }}

-- USCG-registered boat-manufacturer attributes — the CURRENT view (dbt_valid_to is
-- null) over the SCD-2 snapshot uscg_manufacturer_attributes_snapshot (ADR 0035).
-- One row per MIC. Sibling to firm_establishment_attributes (USDA) / firm_fda_attributes;
-- firm.sql stays keyed on normalized name for cross-source dedup, this dim sits beside it.
--
-- ADR 0035 changed this model in three ways vs the Phase 5d listing-only version:
--   1. Source switched from the listing view (stg_uscg_manufacturers, ~30-char truncated
--      address — Finding F.1) to the detail-page snapshot's current view (full address +
--      the succession lineage the listing lacks).
--   2. Two derived time-sensitivity flags surface the MIC-recycle NEED. They mirror
--      assert_mic_holder_stable.sql EXACTLY (all three past_company_* slots, `\(OOB`
--      marker) so the silver flag count reconciles with the monitor's 205-OOB / 365-prior
--      measurement — the slots are NOT filled sequentially, so checking only slot 1
--      undercounts the recycle surface by ~40%.
--   3. prior_holders carries the non-null past holders as a jsonb array for inspection.
--
-- mic_oob_recycled (the high-confidence recycle: a prior holder marked (OOB)) is a SUBSET
-- of mic_has_prior_holder (any prior holder at all). recall_event_firm stamps
-- uscg_mic_time_sensitive_unresolved when EITHER is true (precision-over-recall: flag
-- broadly rather than silently misattribute a pre-reassignment recall to the current holder).
-- The grain is one current row per mic; the SCD-2 history is the snapshot itself (Policy C).

with current_manufacturers as (
    select *
    from {{ ref('uscg_manufacturer_attributes_snapshot') }}
    where dbt_valid_to is null
)

select
    mic,
    company_name,
    dba,
    parent_company,
    parent_mic,
    past_company_1,
    past_company_2,
    past_company_3,
    address,
    city,
    state,
    zip,
    country,
    status,
    in_business,
    out_of_business,
    date_modified,
    uscg_directory_id,
    detail_url,

    -- ADR 0035 time-sensitivity flags (mirror assert_mic_holder_stable.sql Q2-Q4):
    --   has_prior_holder = any of the three Past Company slots is populated (the 365 set).
    --   oob_recycled     = a Past Company is marked out-of-business. probe_mic_prior_holder_not_oob.sql
    --     (2026-06-05) showed the source uses MULTIPLE OOB notations — `(OOB)`, `(OOB 1991)`, AND a
    --     dash form `- OOB` (e.g. "ARLINGTON BOAT WORKS - OOB") — so we match a word-boundary
    --     `\yOOB\y`, NOT the paren-only `\(OOB`. (The monitor's published "205" is paren-only and
    --     undercounts; this column is the true superset.) `(previous name)` is a RENAME, not OOB, so
    --     it is intentionally NOT matched. coalesce('') keeps an empty slot false, not NULL.
    (coalesce(past_company_1, past_company_2, past_company_3) is not null) as mic_has_prior_holder,
    (
        coalesce(past_company_1, '') ~ '\yOOB\y'
        or coalesce(past_company_2, '') ~ '\yOOB\y'
        or coalesce(past_company_3, '') ~ '\yOOB\y'
    ) as mic_oob_recycled,
    to_jsonb(
        array_remove(array[past_company_1, past_company_2, past_company_3], null)
    ) as prior_holders
from current_manufacturers
