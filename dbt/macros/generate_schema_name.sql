{#
    Custom generate_schema_name override (Phase 6b PR 6b.5, ADR 0035).

    dbt's BUILT-IN default concatenates the target schema with any custom schema
    (`{{ target.schema }}_{{ custom_schema_name }}`) — so a model/snapshot that sets
    `schema='silver_snapshots'` would land in `public_silver_snapshots`. This override
    uses a custom schema name VERBATIM instead:

      - no custom schema set      -> target.schema (today: `public`) — UNCHANGED, so every
                                     existing staging/silver/gold model stays in `public`.
      - schema='silver_snapshots' -> `silver_snapshots` (the ADR 0033 designated snapshot
                                     home; the ADR 0007 pruning exemption names this schema).

    Blast radius: nothing else in the project sets a custom schema today, so this only moves
    the SCD-2 snapshot into its own schema. STANDING CAVEAT — this is GLOBAL: any future
    `+schema` / `schema=` lands verbatim with NO environment prefix. If this project ever
    runs against multiple schemas/targets on one database, guard by `target.name` here so
    dev and prod don't collide in the same custom schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
