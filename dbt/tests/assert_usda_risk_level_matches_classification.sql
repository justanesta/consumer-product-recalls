-- USDA risk_level derive regression guard. risk_level is DERIVED 1:1 from
-- classification (W1 Q2 proof), not lifted from a separate bronze field. Any
-- USDA event whose (classification, risk_level) pair breaks the documented
-- mapping fails this test. (A genuinely new classification value falls through
-- to NULL on both sides and is caught by the classification accepted_values
-- test instead — this guard locks the MAPPING, not the domain.)

select
    recall_event_id,
    classification,
    risk_level
from {{ ref('recall_event') }}
where source = 'USDA'
  and risk_level is distinct from (
      case classification
          when 'Class I'             then 'High - Class I'
          when 'Class II'            then 'Low - Class II'
          when 'Class III'           then 'Marginal - Class III'
          when 'Public Health Alert' then 'Public Health Alert'
      end
  )
