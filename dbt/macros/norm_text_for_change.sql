{#-
  Normalize a text value for recall_event_history change-DETECTION (ADR 0022 / Phase 6c.1).
  Used only to decide whether two consecutive snapshots differ — the emitted old_value /
  new_value stay RAW. Folds three cosmetic, non-editorial differences to a single canonical
  form so they never synthesize a phantom edit:

    1. internal whitespace runs  → one space   (USDA Finding Q: 10 leading newlines on
       company_media_contact drove ~1024 phantom re-versions per wave)
    2. leading / trailing whitespace → stripped
    3. '' and whitespace-only and NULL → NULL  (bronze stores raw and the sources mix '' and
       NULL for "empty" — staging nullif()s, but history reads bronze, so a '' ↔ NULL flip
       must NOT read as an edit)

  Real edits (different non-empty content, or genuine populate/erase) survive: '' / NULL both
  → NULL, so NULL→'Class I' and 'Class I'→'' are still detected via `is distinct from`.
-#}
{% macro norm_text_for_change(col) -%}
nullif(trim(regexp_replace(coalesce({{ col }}, ''), '\s+', ' ', 'g')), '')
{%- endmacro %}
