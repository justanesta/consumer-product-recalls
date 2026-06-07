{{ config(
    materialized='table',
    indexes=[
      {'columns': ['firm_fei_num'], 'unique': True},
      {'columns': ['firm_state_cd']},
    ],
    post_hook=[
      "create index if not exists idx_firm_fda_attributes_fei_text on {{ this }} ((firm_fei_num::text))",
      "analyze {{ this }}",
    ]
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

-- Data-quality overrides for firm-country gaps FDA leaves null (inferred, extensible). Phase 6e.5:
-- Visaris DOO (FEI 3012569470) — Belgrade + the "DOO" (d.o.o.) corporate suffix = a Serbian LLC,
-- so firm_country_nam -> 'Serbia'. Add rows here as new null-country foreign firms surface (the
-- firm_country_nam not_null test stays severity=warn to flag the next one).
with country_overrides (firm_fei_num, country) as (
    values (3012569470::bigint, 'Serbia')
)

select
    c.firm_fei_num,
    c.firm_legal_nam,
    c.firm_city_nam,
    c.firm_state_cd,
    c.firm_state_prvnc_nam,
    coalesce(c.firm_country_nam, ov.country) as firm_country_nam,
    c.firm_postal_cd,
    c.firm_line1_adr,
    c.firm_line2_adr,
    c.firm_surviving_nam,
    c.firm_surviving_fei
from {{ ref('firm_fda_attributes_snapshot') }} c
left join country_overrides ov on ov.firm_fei_num = c.firm_fei_num
where c.dbt_valid_to is null
