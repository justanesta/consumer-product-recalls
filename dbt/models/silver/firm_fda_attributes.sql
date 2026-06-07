{{ config(
    materialized='table',
    indexes=[
      {'columns': ['firm_fei_num'], 'unique': True},
      {'columns': ['firm_state_cd']},
    ],
    post_hook="create index if not exists idx_firm_fda_attributes_fei_text on {{ this }} ((firm_fei_num::text))"
) }}

-- FDA-registered establishment (firm) attributes — address + firm-continuity
-- metadata that doesn't fit on firm.sql (which is keyed on normalized name and
-- shared across CPSC/FDA/USDA/NHTSA/USCG). The third firm-attribute sidecar,
-- sibling to firm_establishment_attributes (USDA) and firm_manufacturer_attributes
-- (USCG); see documentation/silver_design_notes.md §3 (firm supertype / per-source
-- attribute subtype). Capture-expansion (b) PR W1 — fields landed by migration 0019.
--
-- One row per firm_fei_num (the FDA FEI — the structured registry id also written
-- to firm.observed_company_ids for FDA firms in firm.sql). Joined to the conformed
-- firm dim via observed_company_ids, the same pattern as the USDA/USCG sidecars.
-- Unlike those two, FDA has NO directory source — the firm fields ride inline on
-- the recall feed (stg_fda_recalls, one row per product), so this model collapses
-- to one row per FEI via DISTINCT ON ... latest.
--
-- Grain / SCD evidence (2026-06-03, full corpus 134,450 —
-- scripts/sql/fda/bronze/profile_firm_fei_for_sidecar.sql):
--   * 14,285 distinct firm names; only 53 (0.4%) lack an FEI. Those are EXCLUDED
--     here (the `where firm_fei_num is not null` below) exactly as the other two
--     sidecars exclude null-key rows — their names still live in firm.sql; only
--     the FDA address sidecar row is absent. Not a silent drop: documented here.
--   * 15.3% of FEIs carry >1 distinct address across recalls (max 9), so the
--     latest-wins collapse is genuine Type-1 work, not a no-op. SCD-2 history of
--     firm moves is deferred to 6c (ADR 0035) — the same call as USCG MIC
--     reassignment in firm_manufacturer_attributes.
--   * 12.4% of FEIs map to >1 normalized name (renamed/variant spellings); the
--     firm_surviving_nam/fei columns are FDA's own succession signal for those.
--
-- As of Phase 6c.4 this is the CURRENT view (dbt_valid_to is null) over the SCD-2 snapshot
-- firm_fda_attributes_snapshot (ADR 0035 Policy C) — an additive history layer. The column
-- contract is UNCHANGED. The latest-per-FEI collapse (DISTINCT ON ... event_lmd desc, then
-- extraction_timestamp desc, then source_recall_id) now lives in the snapshot driver.

select
    firm_fei_num,
    firm_legal_nam,
    firm_city_nam,
    firm_state_cd,
    firm_state_prvnc_nam,
    firm_country_nam,
    firm_postal_cd,
    firm_line1_adr,
    firm_line2_adr,
    firm_surviving_nam,
    firm_surviving_fei
from {{ ref('firm_fda_attributes_snapshot') }}
where dbt_valid_to is null
