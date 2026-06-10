-- C5 (ADR 0007 / migration 0028): every USDA multivalue bronze field must be a JSONB **array**
-- (or NULL) — never a bare scalar or object. This is the uniformity the jsonb-consuming silver SQL
-- relies on: recall_event.reason_category / recall_product.processing_categories token arrays
-- (jsonb_array_elements_text, C14) and recall_distribution_area's USDA comma-token parse. A scalar
-- that slipped through (e.g. an un-migrated replay row, or a future API field that is sometimes a
-- string) would silently break those unnests. This fails loud with the offending column + value.
--
-- The ten fields are migration-0028's _ARRAY_FIELDS — keep this list in lockstep with that migration.
{% set usda_array_fields = [
    'recall_reason', 'processing', 'states', 'establishment', 'labels',
    'product_items', 'distro_list', 'company_media_contact',
    'en_press_release', 'press_release',
] %}

{% for col in usda_array_fields %}
select '{{ col }}' as bad_column, {{ col }}::text as bad_value
from {{ source('usda', 'usda_fsis_recalls_bronze') }}
where {{ col }} is not null and jsonb_typeof({{ col }}) <> 'array'
{% if not loop.last %}union all{% endif %}
{% endfor %}
