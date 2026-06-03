-- Bug 1 regression guard (the headline fix). FDA recall_event.recall_reason must
-- carry the product DEFECT reason (product_short_reason_txt), never the
-- geographic distribution list (distribution_area_summary_txt). Pre-fix it was
-- mis-sourced from the distribution summary. recall_reason and
-- distribution_area_summary are now distinct fields populated from distinct
-- bronze columns, so an equal non-null pair is the regression resurfacing.

select
    recall_event_id,
    recall_reason
from {{ ref('recall_event') }}
where source = 'FDA'
  and recall_reason is not null
  and distribution_area_summary is not null
  and recall_reason = distribution_area_summary
