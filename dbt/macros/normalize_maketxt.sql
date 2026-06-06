{#
  normalize_maketxt(col) — canonical, over-merge-SAFE normalization of a NHTSA `maketxt`
  (vehicle/equipment make) for use as a stable surrogate-key ANCHOR (ADR 0033 Normalization
  class, Phase 6b PR 6b.3).

  Collapses ASCII whitespace (incl. tabs/newlines via \s) and case ONLY:
      regexp_replace(upper(trim(coalesce(col, ''))), '\s+', '', 'g')
  'AC DELCO' / 'ac  delco' / 'ACDELCO' -> 'ACDELCO'. Over-merge-safe BY CONSTRUCTION: two
  genuinely-distinct makes cannot differ only by whitespace/case, so this never fuses
  unrelated products (precision-over-recall).

  SINGLE SOURCE OF TRUTH. Called byte-identically at every site that MUST agree:
    - stg_nhtsa_recalls.sql                  (identity-grain DISTINCT-ON partition -> 1 row survives)
    - recall_product.sql                     (recall_product_id md5 -> stable, non-fragmenting surrogate)
    - tests/assert_nhtsa_maketxt_drift_caught.sql (forward drift monitor)
    - tests/assert_no_ac_delco_firm_drift.sql     (firm-leak regression guard)
  Changing the recipe HERE updates all sites at once — they cannot silently diverge and
  re-fragment. This is deliberately CONSERVATIVE: punctuation / abbreviation / invisible-
  unicode drift is NOT collapsed here (that risks over-merge); it is DETECTED by the drift
  monitor and handled per-class (extend this macro after eyeballing G8, add a targeted alias,
  or send to the 6b.4 fuzzy layer). See ADR 0033's 2026-06-03 amendment.
#}
{% macro normalize_maketxt(col) -%}
regexp_replace(upper(trim(coalesce({{ col }}, ''))), '\s+', '', 'g')
{%- endmacro %}
