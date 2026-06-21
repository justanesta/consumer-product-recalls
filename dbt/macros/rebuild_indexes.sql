{% macro rebuild_indexes() %}
{#-
  Oscillation-safe index (re)build, driven by each model's `config(meta={'index_specs': [...]})`. Wire it
  ONCE as a folder-level `+post-hook` (see dbt_project.yml — silver + gold); it then runs for every model
  in those folders, reads that model's `meta.index_specs`, and DROP-THEN-CREATEs each index on the FINAL
  relation AFTER dbt's table swap. Models with no `meta.index_specs` (views, index-less tables) render to
  nothing and dbt skips the empty hook — so it is safe folder-wide. New/changed indexes: just edit
  `meta.index_specs`. (The key is `index_specs`, NOT `indexes`: `meta.indexes` would make dbt's table
  materialization — which calls `config.get('indexes')` — emit a spurious "detected under meta" nudge.)

  WHY (dbt 1.11.x, gold-audit 2026-W26): the native `config(indexes=[...])` builds indexes on the
  `__dbt_tmp` relation via `create index if not exists "<hash>"` BEFORE the swap. The hash is
  md5(cols + relation + unique + type) and the relation is the stably-named `<model>__dbt_tmp`, so the
  name is IDENTICAL across builds and collides with the same-named index still on the OLD (not-yet-renamed)
  table — Postgres index names are unique per SCHEMA, so the IF NOT EXISTS no-ops and the index is dropped
  with the backup. Net: it OSCILLATES out every other build. (This SUPERSEDES the 2026-06-15 "config
  indexes are oscillation-immune" note — that held only while older dbt created indexes on the FINAL
  relation post-swap.) DROP-THEN-CREATE on {{ this }} after the swap is immune. So we DON'T use
  `config(indexes)` at all; specs live in `meta.index_specs` and this macro is the single builder.

  No functional downside within a `dbt build`: a model node isn't "done" until its post_hooks finish, so
  downstream `ref()`s never query the table during the brief post-swap un-indexed window.

  Index names are deterministic: {{ this.name }}_<suffix> (keep the result <= 63 chars).
  meta.index_specs spec keys: suffix (required, unique per model), cols (required — raw column list or
  expression; wrap a functional expression in its own parens, e.g. '(firm_fei_num::text)'),
  method (optional — 'gin', etc.), unique (optional bool).
-#}
{%- set specs = (model.config.meta or {}).get('index_specs', []) -%}
{%- for s in specs %}
drop index if exists {{ this.schema }}.{{ this.name }}_{{ s.suffix }};
create {% if s.get('unique') %}unique {% endif %}index {{ this.name }}_{{ s.suffix }} on {{ this }} {% if s.get('method') %}using {{ s.method }} {% endif %}({{ s.cols }});
{%- endfor %}
{% endmacro %}
