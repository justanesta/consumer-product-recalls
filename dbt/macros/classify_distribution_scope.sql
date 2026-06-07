{% macro classify_distribution_scope(raw_text) %}
{#-
  Cross-source distribution-scope classifier (ADR 0036 decision D7, completed in Phase 6e
  geography foundation). Maps a source's raw distribution-area text to the conformed enum
  {Nationwide, International, Regional, Unspecified}. Used by recall_event.sql for the two
  sources that carry distribution text — FDA (`distribution_area_summary_txt`) and USDA
  (`states`). Sources with no distribution field pass a NULL/absent value → 'Unspecified'
  (CPSC's "nationwide" lives only in free-text retailer narrative — Tier-2/NER, not here;
  NHTSA is set to 'Nationwide' directly in its branch since federal vehicle recalls are
  national by regulation).

  'Unspecified' (not NULL) is the spec value (bronze_corpus_profile.md:258) so the column is
  a closed, NOT-NULL, testable domain. International is checked before Nationwide so a recall
  distributed both nationally and abroad reads as the broader International scope.
-#}
    case
        when {{ raw_text }} is null or btrim({{ raw_text }}) = '' then 'Unspecified'
        when lower({{ raw_text }}) ~ 'worldwide|international|global' then 'International'
        when lower({{ raw_text }}) ~ 'nationwide|all 50|all fifty|all states' then 'Nationwide'
        else 'Regional'
    end
{% endmacro %}
