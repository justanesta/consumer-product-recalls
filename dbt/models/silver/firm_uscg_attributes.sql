{{ config(
    materialized='table',
    indexes=[
      {'columns': ['mic'], 'unique': True},
      {'columns': ['state']},
    ],
    post_hook="analyze {{ this }}"
) }}

-- USCG-registered boat-manufacturer attributes — the CURRENT view (dbt_valid_to is
-- null) over the SCD-2 snapshot firm_uscg_attributes_snapshot (ADR 0035).
-- One row per MIC. Sibling to firm_usda_attributes (USDA) / firm_fda_attributes;
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
    from {{ ref('firm_uscg_attributes_snapshot') }}
    where dbt_valid_to is null
),

flagged as (
    select
        *,
        -- ADR 0035 time-sensitivity flags (mirror assert_mic_holder_stable.sql Q2-Q4):
        --   has_prior_holder = any of the three Past Company slots is populated (the 365 set).
        --   oob_recycled     = a Past Company marked out-of-business. The source uses MULTIPLE OOB
        --     notations — `(OOB)`, `(OOB 1991)`, AND a dash form `- OOB` ("ARLINGTON BOAT WORKS - OOB",
        --     probe_mic_prior_holder_not_oob.sql 2026-06-05) — so match word-boundary `\yOOB\y`, not
        --     paren-only `\(OOB`. Slots are NOT filled sequentially, so all three must be checked.
        (coalesce(past_company_1, past_company_2, past_company_3) is not null) as mic_has_prior_holder,
        (
            coalesce(past_company_1, '') ~ '\yOOB\y'
            or coalesce(past_company_2, '') ~ '\yOOB\y'
            or coalesce(past_company_3, '') ~ '\yOOB\y'
        ) as mic_oob_recycled,
        -- 6c.5 (a): every POPULATED prior slot is a source-tagged "(previous name)" rename — the
        -- same manufacturer renamed, not a different firm. The source marks renames explicitly with
        -- "(previous name)" (gate probe_uscg_refinement_gates Q1b confirmed the literal format).
        (
            (coalesce(past_company_1, '') = '' or past_company_1 ~* '\(previous name\)')
            and (coalesce(past_company_2, '') = '' or past_company_2 ~* '\(previous name\)')
            and (coalesce(past_company_3, '') = '' or past_company_3 ~* '\(previous name\)')
        ) as all_priors_are_renames
    from current_manufacturers
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
    mic_has_prior_holder,
    mic_oob_recycled,
    -- 6c.5 (a): a PURE rename (has a prior holder, none OOB-recycled, every prior slot is a
    -- "(previous name)" marker) → same manufacturer, no misattribution → recall_event_firm downgrades
    -- it from uscg_mic_time_sensitive_unresolved to uscg_mic_unambiguous. Gate Q1: 2 recalled MICs
    -- (ACB, CEC). A MIC with ANY OOB or unmarked-distinct prior is NOT a pure rename (stays flagged).
    (mic_has_prior_holder and not mic_oob_recycled and all_priors_are_renames) as mic_renamed_not_recycled,
    to_jsonb(
        array_remove(array[past_company_1, past_company_2, past_company_3], null)
    ) as prior_holders
from flagged
