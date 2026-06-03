-- USDA Bug 1 regression guard. recall_product.type must be the processing
-- category (e.g., 'Fully Cooked - Not Shelf Stable'), NOT the recall lifecycle
-- type, which belongs in recall_event.lifecycle_status. Pre-fix, type was
-- mis-sourced from recall_type. Any USDA product whose type is one of the
-- recall_type lifecycle values is the regression resurfacing. (Processing
-- categories never collide with these three lifecycle strings.)

select
    recall_product_id,
    type
from {{ ref('recall_product') }}
where source = 'USDA'
  and type in ('Active Recall', 'Closed Recall', 'Public Health Alert')
