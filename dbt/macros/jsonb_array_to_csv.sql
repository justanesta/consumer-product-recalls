{#-
  Collapse a jsonb array of strings into a comma-joined text value, preserving element
  order. Used by stg_usda_fsis_recalls to reconstruct the pre-2026-06 scalar shape of the
  USDA multi-value fields (which FSIS flipped scalar → array, migration 0028) so downstream
  silver keeps its existing text contract. Exploiting the native arrays is deferred follow-up.

  - NULL            → '' (then the caller's nullif(...,'' ) → NULL)
  - '[]'::jsonb     → '' (empty array)
  - '["a","b"]'     → 'a, b'
  - '["x, y"]'      → 'x, y' (single element preserved verbatim — NOT re-split)

  Assumes the column holds a jsonb ARRAY or NULL (migration 0028 guarantees this; a scalar
  jsonb would error in jsonb_array_elements_text, which is the desired loud failure).
-#}
{% macro jsonb_array_to_csv(col) -%}
array_to_string(array(select jsonb_array_elements_text({{ col }})), ', ')
{%- endmacro %}
