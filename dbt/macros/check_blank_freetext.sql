{% macro check_blank_freetext(model_name, columns) %}
{#-
  Emits a UNION ALL of per-(column, source) blank counts for `model_name`, returning a row for any
  column/source that holds a literal '' or a whitespace-only value (~ '^\s+$'). Used by the singular
  test assert_no_blank_freetext_serving to guarantee ADR 0027 empty-string normalization holds at the
  silver serving boundary for EVERY source — catching a staging-nullif removal, a new un-normalized
  source, or an accidental revert of a wrap. On-demand discovery peer:
  scripts/sql/cross_source/empty_string_freetext_audit.sql.
-#}
{%- for col in columns %}
    select
        '{{ model_name }}' as model_name,
        '{{ col }}'        as column_name,
        source,
        count(*)           as n_blank
    from {{ ref(model_name) }}
    where {{ col }} = '' or {{ col }} ~ '^\s+$'
    group by source
    {%- if not loop.last %}
    union all
    {%- endif %}
{%- endfor %}
{% endmacro %}
