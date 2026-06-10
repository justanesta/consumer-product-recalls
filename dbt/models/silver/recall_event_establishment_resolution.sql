{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
    ]
) }}

-- USDA recall -> FSIS establishment disambiguation (Phase 6b PR 6b.2).
--
-- One row per USDA recall_event_id that has >=1 name-matched establishment. USDA recalls carry only
-- a free-text `establishment` NAME (no FSIS number); ~28.5% of name-matched recalls fan out to 2+
-- establishments sharing that name. This model resolves the fan-out to a SINGLE establishment_number
-- via a precision-first signal hierarchy, or stamps usda_ambiguous_null (NULL number) when it can't
-- — a wrong attribution is worse than none (landing pages would render the wrong address).
--
-- Empirically calibrated (2026-06-03 gates, full corpus):
--   1. unambiguous  (n_candidates = 1)                                 -> usda_unambiguous           (562 recalls)
--   2. Signal 1: FSIS establishment number embedded in the recall text -> usda_product_items_extract (115 of 224 fan-outs)
--        Extracted from summary || product_items || labels (the number lives in summary's
--        "...a <City, State> establishment ... bearing establishment number 'EST. P-XXXX'" sentence,
--        NOT product_items — that mis-location is why a product_items-only probe read only ~2%).
--        Format-robust (bare / prefix-before / letter-after per the FSIS data doc) and SPLIT on the
--        '+' composite (M46712+P46712), matching the FULL grant token (suffix-sensitive: M1234A != M1234B).
--   3. Signal 2: field_states ∩ establishment.state (non-Nationwide)   -> usda_state_match           (+11 net)
--   4. else                                                            -> usda_ambiguous_null        (98 — mostly multi-plant
--        producers (Pilgrim's 31 plants, Cargill 10) with no stated number; correctly NULL)
-- Net 688/786 name-matched recalls resolved (87.5%). Runs BEFORE recall_event_firm.sql, which joins
-- this on recall_event_id to stamp establishment_number + match_confidence on USDA bridge rows.
-- DROPPED to v2: establishment_group_id same-facility-collapse (composites already collapse a
-- facility's grants) and Signal 3 processing->activities (coarse). cold_storage_flag is name-based
-- (Lineage/Americold are storage operators, not producers — the producer is a Phase 6/7 extraction).

with recalls as (
    select
        md5('USDA' || '|' || source_recall_id)                  as recall_event_id,
        establishment,
        states,
        (states ~* 'nationwide')                                as is_nationwide,
        (establishment ~* 'lineage|americold|cold[ -]?storage|\ylogistics\y|warehous|refrigerat') as is_cold_storage_name,
        coalesce(summary, '') || '  ' || coalesce(product_items, '') || '  ' || coalesce(labels, '') as alltext
    from {{ ref('stg_usda_fsis_recalls') }}
    where establishment is not null and trim(establishment) <> ''
),

-- USPS abbreviations (establishment.state is 2-letter; recall states are full names). Sourced from
-- the shared dbt/seeds/us_state_abbr.csv seed (Phase 6e geography foundation — DRY with
-- recall_distribution_area.sql; identical values to the prior inline list).
state_abbr as (
    select name, abbr from {{ ref('us_state_abbr') }}
),

-- Candidate establishments for each recall (matched by name); grant set = composite split, normalized.
candidates as (
    select
        r.recall_event_id,
        e.establishment_id,
        upper(trim(e.state))                                                                       as est_state,
        (select array_agg(distinct regexp_replace(upper(g), '[^A-Z0-9]', '', 'g'))
           from unnest(string_to_array(e.establishment_id, '+')) as g)                             as grants
    from recalls r
    join {{ ref('firm_usda_attributes') }} e
        on upper(trim(e.establishment_name)) = upper(trim(r.establishment))
),

n_cand as (
    select recall_event_id, count(*) as n_candidates from candidates group by recall_event_id
),

unambiguous as (
    select recall_event_id, max(establishment_id) as establishment_id
    from candidates group by recall_event_id having count(*) = 1
),

-- Signal 1: grant tokens extracted from the recall text, normalized to canonical LETTER+NUMBER+SUFFIX.
extracted as (
    select recall_event_id, array_agg(distinct norm_tok) as toks
    from (
        select r.recall_event_id,
               regexp_replace(
                   regexp_replace(upper(m[1]), '^([0-9]+)[ .-]*([MPGIV])$', '\2\1'),  -- "19924 M" -> "M19924"
                   '[^A-Z0-9]', '', 'g'
               ) as norm_tok
        from recalls r,
             lateral regexp_matches(r.alltext, '[MPGIV][ .-]?[0-9]{2,6}[A-Z]?|[0-9]{2,6}[ .-]?[MPGIV]', 'gi') as m
    ) z
    where norm_tok ~ '^[MPGIV][0-9]{2,6}[A-Z]?$'
    group by recall_event_id
),

sig1_resolved as (  -- fan-out recalls where the embedded number matches EXACTLY ONE candidate
    select c.recall_event_id, max(c.establishment_id) as establishment_id
    from candidates c
    join extracted x on x.recall_event_id = c.recall_event_id and x.toks && c.grants
    group by c.recall_event_id
    having count(*) = 1
),

-- Signal 2: recall states (normalized to abbrev, non-Nationwide) intersect candidate state.
recall_state_set as (
    select r.recall_event_id, coalesce(sa.abbr, upper(trim(tok))) as st
    from recalls r,
         lateral unnest(string_to_array(r.states, ',')) as tok
    left join state_abbr sa on sa.name = upper(trim(tok))
    where r.states is not null and not r.is_nationwide
      and upper(trim(tok)) <> 'MIDWEST' and trim(tok) <> ''
),

sig2_resolved as (
    select c.recall_event_id, max(c.establishment_id) as establishment_id
    from candidates c
    join recall_state_set rs on rs.recall_event_id = c.recall_event_id and rs.st = c.est_state
    group by c.recall_event_id
    having count(*) = 1
)

select
    nc.recall_event_id,
    nc.n_candidates,
    coalesce(u.establishment_id, s1.establishment_id, s2.establishment_id) as establishment_number,
    case
        when u.establishment_id  is not null then 'usda_unambiguous'
        when s1.establishment_id is not null then 'usda_product_items_extract'
        when s2.establishment_id is not null then 'usda_state_match'
        else 'usda_ambiguous_null'
    end                                                                    as match_confidence,
    (nc.n_candidates > 1 and r.is_cold_storage_name)                       as cold_storage_flag
from n_cand nc
join recalls r          on r.recall_event_id  = nc.recall_event_id
left join unambiguous u on u.recall_event_id  = nc.recall_event_id
left join sig1_resolved s1 on s1.recall_event_id = nc.recall_event_id
left join sig2_resolved s2 on s2.recall_event_id = nc.recall_event_id
