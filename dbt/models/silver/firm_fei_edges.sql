{{ config(materialized='table') }}

-- FDA FEI identity rows (Phase 6b PR 6b.3; current-FEI resolution added in 6b.4).
--
-- A thin, additive artifact feeding the 6b.4 Tier-0 resolver. An FEI is FDA's government-
-- assigned **establishment (facility)** id, NOT a firm id (one firm has many FEIs; FDA
-- reassigns a firm's FEI on ownership/operational change). So the resolver does NOT merge on
-- the raw at-recall firm_fei_num. Instead it groups names by **current_fei** = FDA's own
-- post-rename id (``coalesce(firm_surviving_fei, firm_fei_num)``), which collapses renames
-- deterministically, and gates any current_fei that fans out to many distinct names (a shared
-- registrant / contract facility / sentinel — not one firm). See ADR 0037 + fei_resolve().
--
-- The 12.4%-of-FEIs-map-to->1-name figure (full-corpus profile 2026-06-03,
-- scripts/sql/fda/bronze/profile_firm_fei_for_sidecar.sql) are mostly rename/variant spellings;
-- scripts/sql/cross_source/silver/diagnose_fei_fanout.sql sizes the high-fan-out tail to gate.
--
-- NOT consumed by firm.sql / recall_event_firm.sql in 6b.3 — this PR only MATERIALIZES
-- the edges (deterministic, separately testable). 6b.4 ref()s it. Additive: no firm_id
-- change, no lockstep impact. firm_id mirrors firm.sql's fda_normalized recipe EXACTLY
-- (md5(upper(trim(firm_legal_nam)))) so the clusterer's merges land on real firm ids.
-- Intentionally NO relationships test to firm.firm_id: these are PRE-clustering raw
-- ids and feed the very pass that makes firm.firm_id canonical (a 6b.4 footgun to avoid).

select distinct
    md5(upper(trim(firm_legal_nam)))      as firm_id,
    upper(trim(firm_legal_nam))           as normalized_name,
    firm_fei_num::text                    as firm_fei_num,
    nullif(firm_surviving_fei::text, '')  as firm_surviving_fei,
    -- current_fei = FDA's own post-rename establishment id (firmsurvivingfei is the CURRENT FEI
    -- "if changed since the recall" — a single hop; FDA resolves the full chain). The 6b.4
    -- Tier-0 resolver groups names by current_fei (same establishment, fan-out gated), NOT by
    -- the raw at-recall firm_fei_num — this collapses renames deterministically.
    coalesce(nullif(firm_surviving_fei::text, ''), firm_fei_num::text)   as current_fei,
    -- current_name = the surviving (current) firm name if renamed, else the legal name — the
    -- canonical-display hint for the establishment's cluster.
    coalesce(nullif(trim(firm_surviving_nam), ''), trim(firm_legal_nam)) as current_name
from {{ ref('stg_fda_recalls') }}
where firm_fei_num is not null
  -- firm_id must be non-null (the _silver.yml not_null test) — exclude the handful of
  -- null/blank legal names exactly as firm.sql's fda_normalized does.
  and firm_legal_nam is not null
  and trim(firm_legal_nam) <> ''
