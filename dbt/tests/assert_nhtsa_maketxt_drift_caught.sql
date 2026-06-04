{{ config(severity='warn') }}

-- Forward drift monitor (ADR 0033 Normalization class, Phase 6b PR 6b.3). The AC DELCO ->
-- ACDELCO fix canonicalizes maketxt (whitespace+case via normalize_maketxt) so THAT observed
-- class folds to one product identity. But maketxt is a stable ANCHOR field, and ADR 0033
-- flags the Normalization class as the one drift class SCD cannot absorb on an anchor — the
-- full 1966-present seed (Phase 6a.5) and future incrementals WILL surface new wonky maketxt
-- drift the deterministic normalize does NOT catch (punctuation, abbreviation, invisible
-- unicode). This monitor is the defensive trip-wire: it WARNS when one logical product
-- (identical on all 10 non-maketxt identity fields) carries >1 DISTINCT normalize_maketxt
-- value in bronze -> a make-spelling drift our canonicalization missed. A non-zero warn is a
-- triage signal (extend the macro / add a targeted alias / send to the 6b.4 fuzzy layer), NOT
-- a build break. severity=warn = ADR 0031 Tier-2 detection.
--
-- Grain note: keying on the 10 OTHER identity fields (model/year/component/part/batch) pins
-- the exact product, so >1 make on that grain is a spelling drift, not a legitimate multi-make
-- recall (different makes differ in modeltxt / part numbers). Rare legit edge cases are
-- acceptable triage noise at warn level. Reads BRONZE (audit-quality, keeps every spelling) —
-- staging already collapsed the survivors away.

with drift as (
    select
        campno, modeltxt, yeartxt, compname, rcl_cmpt_id,
        mfr_comp_ptno, mfr_comp_desc, mfr_comp_name, endman, bgman,
        count(distinct {{ normalize_maketxt('maketxt') }})   as distinct_norm_makes,
        string_agg(distinct maketxt, ' | ' order by maketxt)  as raw_makes
    from {{ source('nhtsa', 'nhtsa_recalls_bronze') }}
    where maketxt is not null and trim(maketxt) <> ''
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

select campno, modeltxt, yeartxt, compname, raw_makes, distinct_norm_makes
from drift
where distinct_norm_makes > 1
