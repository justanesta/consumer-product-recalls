{% macro grant_gold_readonly() %}
{#- Re-applied per gold model (dbt_project.yml `gold: +post-hook`): keeps the public read-only API
    role's SELECT across the nightly drop+recreate of the gold tables — a one-time migration grant would
    be wiped on the next build. Gold-only by folder scope; tolerant of the role being absent (dev / CI
    ephemeral Neon branches) so those builds don't fail. The role itself is provisioned out-of-band by
    migration 0034 (serving-layer plan R1). -#}
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'recalls_readonly') then
        grant select on {{ this }} to recalls_readonly;
    end if;
end $$
{% endmacro %}
