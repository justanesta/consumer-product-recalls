{% snapshot firm_fda_attributes_snapshot %}
{{
  config(
    schema='silver_snapshots',
    unique_key='firm_fei_num',
    strategy='check',
    check_cols=[
      'firm_legal_nam', 'firm_city_nam', 'firm_state_cd', 'firm_state_prvnc_nam',
      'firm_country_nam', 'firm_postal_cd', 'firm_line1_adr', 'firm_line2_adr',
      'firm_surviving_nam', 'firm_surviving_fei',
    ],
  )
}}

-- SCD-2 history for FDA firm (establishment) attributes (ADR 0035 Policy C; Phase 6c.4 BENEFIT
-- dim). Stable anchor = firm_fei_num (the FDA FEI). FDA has no directory source — the firm fields
-- ride inline on the recall feed (stg_fda_recalls, one row per product), so the driver collapses
-- to one row per FEI via DISTINCT ON ... latest (event_lmd desc) — the same recency the dim used
-- before — and the snapshot versions it. 15.3% of FEIs carry >1 address across recalls, so the
-- collapse is real Type-1 work and this captures the firm moves as Type-2 history. Lands in
-- silver_snapshots (exempt from ADR 0007 pruning).
--
-- BENEFIT, not NEED: firm_fei_num is stable — 0 edit-versions post-6a.5-reseed, so it banks one
-- version per anchor now and grows forward.

select distinct on (firm_fei_num)
    firm_fei_num,
    nullif(trim(firm_legal_nam), '') as firm_legal_nam,
    firm_city_nam,
    firm_state_cd,
    firm_state_prvnc_nam,
    firm_country_nam,
    firm_postal_cd,
    firm_line1_adr,
    firm_line2_adr,
    firm_surviving_nam,
    firm_surviving_fei
from {{ ref('stg_fda_recalls') }}
where firm_fei_num is not null
order by
    firm_fei_num,
    event_lmd desc nulls last,
    extraction_timestamp desc,
    source_recall_id

{% endsnapshot %}
