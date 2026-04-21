-- Fails if any recall_product row references a recall_event_id that does not
-- exist in recall_event. Complements the generic `relationships` test by also
-- catching NULL FKs (relationships skips NULLs by default in some adapters).

select rp.recall_product_id, rp.recall_event_id
from {{ ref('recall_product') }} rp
left join {{ ref('recall_event') }} re
    on rp.recall_event_id = re.recall_event_id
where re.recall_event_id is null
