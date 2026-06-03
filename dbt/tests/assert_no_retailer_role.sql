-- CPSC Option B regression guard. The 'retailer' role was removed from the firm
-- dimension and the recall_event_firm bridge — CPSC retailer names now live in
-- recall_event.sales_channel_narrative, not the firm graph. No bridge row may
-- carry role='retailer'. (Backstops the accepted_values list, which could be
-- edited to re-add 'retailer' without this explicit assertion.)

select
    recall_event_id,
    firm_id,
    role
from {{ ref('recall_event_firm') }}
where role = 'retailer'
