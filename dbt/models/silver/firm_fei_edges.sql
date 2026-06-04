{{ config(materialized='table') }}

-- FDA FEI deterministic merge edges (Phase 6b PR 6b.3).
--
-- A thin, additive artifact: the deterministic forced-merge CONSTRAINTS the 6b.4
-- RapidFuzz clusterer consumes (union-find) so the 12.4% of FEIs that map to >1
-- distinct legal name (full-corpus profile 2026-06-03,
-- scripts/sql/fda/bronze/profile_firm_fei_for_sidecar.sql) collapse to one firm
-- WITHOUT fuzzy matching. The FEI is FDA's government-assigned establishment id, so a
-- shared FEI is an authoritative same-firm signal — exact, not edit-distance.
--
-- Two edge kinds the clusterer derives from these rows:
--   1. SHARED-FEI  — every firm_id carrying the same firm_fei_num is the same firm
--      (one establishment, several name spellings across recalls).
--   2. SUCCESSION  — firm_surviving_fei is FDA's own "this firm was renamed/merged
--      into FEI Y" pointer; (firm_fei_num -> firm_surviving_fei) is a directed merge
--      edge. 6b.4 OWNS the interpretation (ignores null/self/dangling, guards cycles).
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
    -- '' / self / dangling sentinels are passed through as-is here; 6b.4 decides which
    -- surviving-FEI pointers are live succession edges. nullif normalizes the blank case.
    nullif(firm_surviving_fei::text, '')  as firm_surviving_fei
from {{ ref('stg_fda_recalls') }}
where firm_fei_num is not null
  -- firm_id must be non-null (the _silver.yml not_null test) — exclude the handful of
  -- null/blank legal names exactly as firm.sql's fda_normalized does.
  and firm_legal_nam is not null
  and trim(firm_legal_nam) <> ''
