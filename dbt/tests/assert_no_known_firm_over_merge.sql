-- 6b.6 anti-merge regression guard (ADR 0037). The FEI-blob disasters the name/brand-grain rewrite
-- fixed must STAY in distinct firms: a row here = a confirmed over-merge re-appeared (someone
-- re-enabled --fei-merge for the firm grain, or loosened Tier 2). error-severity — block the build.
-- Keyed on observed_names (not bridge role: FDA is 'establishment'), word-substring per pair.
with pairs as (
    select *
    from (
        values
            ('WHOLE FOODS', 'STRYKER'),
            ('TEVA', 'BAYER'),
            ('BIOMAT', 'GRIFOLS'),
            ('CSL PLASMA', 'OCTAPHARMA'),
            ('TAKEDA', 'BIOLIFE')
    ) as p (a, b)
)

select
    p.a,
    p.b
from pairs p
where exists (
    select 1
    from {{ ref('firm') }} f
    where
        exists (
            select 1 from jsonb_array_elements_text(f.observed_names) n
            where upper(n) like '%' || p.a || '%'
        )
        and exists (
            select 1 from jsonb_array_elements_text(f.observed_names) n
            where upper(n) like '%' || p.b || '%'
        )
)
