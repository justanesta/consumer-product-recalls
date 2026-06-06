{{ config(severity='warn') }}

-- 6b.6 cross-source acceptance exemplars — the headline use-case made concrete: one firm unified
-- across federal agencies. American Honda Motor spans CPSC+NHTSA+USCG; Tyson Foods spans FDA+USDA.
-- A row here = the value prop regressed for that entity. This is NOT "Honda == 1 firm" — name/brand
-- grain CORRECTLY keeps Honda's divisions (Power Equipment, Marine, the Japan parent) separate; the
-- assertion is that the DOMINANT cluster for each is cross-source. Word-anchored (`token || '%'`) to
-- dodge the substring trap (`%FORD%` -> Crawford/Stafford). warn-severity: a value signal, not a gate.
with firm_sources as (
    select
        ref.firm_id,
        count(distinct re.source) as n_src
    from {{ ref('recall_event_firm') }} ref
    join {{ ref('recall_event') }} re using (recall_event_id)
    group by ref.firm_id
),

exemplars as (
    select *
    from (values ('AMERICAN HONDA MOTOR'), ('TYSON FOODS')) as e (token)
)

select e.token
from exemplars e
where not exists (
    select 1
    from {{ ref('firm') }} f
    join firm_sources fs on fs.firm_id = f.firm_id
    where
        fs.n_src >= 2
        and exists (
            select 1 from jsonb_array_elements_text(f.observed_names) n
            where upper(n) like e.token || '%'
        )
)
