{{ config(severity='warn') }}
-- C14 guard (the safety net for "the comma-split is clean"). Every processing_categories element
-- must be in the known FSIS processing-category taxonomy (10 tokens). A returned row means EITHER:
--   (a) FSIS added a new processing category -> legit taxonomy drift: extend the list here, or
--   (b) the comma-split over-split a token with an internal comma -> a BUG (corrupted value).
-- The 10 tokens use " - " (hyphen) and "/" internally, never a comma, so the comma-split is clean
-- (verified 2026-06-09; baseline 0). severity=warn so legit drift surfaces without breaking the build.
select
    rp.recall_product_id,
    tok.token as offending_token
from {{ ref('recall_product') }} rp,
     lateral jsonb_array_elements_text(rp.processing_categories) as tok(token)
where rp.processing_categories is not null
  and trim(tok.token) not in (
      'Eggs/Egg Products',
      'Fully Cooked - Not Shelf Stable',
      'Heat Treated - Not Fully Cooked - Not Shelf Stable',
      'Heat Treated - Shelf Stable',
      'Not Heat Treated - Shelf Stable',
      'Products with Secondary Inhibitors - Not Shelf Stable',
      'Raw - Intact',
      'Raw - Non Intact',
      'Thermally Processed - Commercially Sterile',
      'Unknown'
  )
