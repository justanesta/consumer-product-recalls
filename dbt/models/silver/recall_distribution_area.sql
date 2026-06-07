{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
      {'columns': ['distribution_state_codes'], 'type': 'gin'},
    ]
) }}

-- recall_distribution_area — Tier-2 structured distribution geography (Phase 6e geography
-- foundation). Pulls forward the deferred distribution_states[]/countries[] backlog item
-- (ADR 0036 D7 / cross_source_consolidation.md §163). One row per recall_event that has >=1
-- parseable US state in its distribution text — FDA + USDA only (the two sources with a
-- distribution field; CPSC/NHTSA/USCG carry none). Feeds fct_recalls_by_geography (distribution
-- lens) and mart_recall_summary. recall_events with no parseable state simply have no row here
-- (the consumers LEFT JOIN); their geography is carried by recall_event.distribution_scope.
--
-- PRECISION DESIGN (tuned to the 2026-06-07 full-corpus profile —
-- scripts/sql/cross_source/silver/profile_distribution_geography.sql; precision-over-recall):
--   * FDA distribution_area_summary is free text. The dominant false-positive mode is FOREIGN
--     2-letter ISO codes in international sections — "INTERNATIONAL ONLY: GB, FR, DE, ..." (DE =
--     Germany, not Delaware), "...countries: AU, BG, CA, ..." (CA = Canada, not California),
--     "US: IN, MI, TX. OUS: GERMANY...". Mitigation: cut the text at the FIRST international marker
--     (OUS / internationally / foreign / countries: / ROW / outside the US / abroad) and parse
--     ONLY the domestic head. FDA records are reliably states-first / international-after, so this
--     is high precision. An international-only row yields no row here (its scope is 'International').
--     NOTE: 'worldwide' is deliberately NOT a marker — states are routinely listed AFTER it
--     ("US Nationwide-Worldwide Distribution: CO, MS, HI, ...").
--   * State NAMES are matched whole-word against the us_state_abbr seed. "West Virginia" is
--     stripped before the bare "Virginia" test — the only state-name-in-state-name overlap.
--   * 2-letter codes are matched only as UPPERCASE standalone tokens (so lowercase words
--     "in"/"or"/"ok" never match) that are known USPS codes (the seed join filters APO/foreign).
--   * USDA distribution_states is a clean comma list (profile Q7: only Nationwide/Midwest are
--     non-state) — comma-token -> name/abbr, same logic as recall_event_establishment_resolution.
--   * Georgia-the-country guard: a marker-less WORLDWIDE country list that names "Georgia" (the
--     country) is detected by an unambiguous-foreign-country-name count (>=3) and the stray name-
--     derived GA is suppressed — see the foreign_indicators CTE. (53 such rows, 0.14%, at build.)
-- distribution_countries[] is intentionally NOT built in v1 (see ADR 0036 D7 / model header note).

with abbr as (
    select upper(name) as name, abbr from {{ ref('us_state_abbr') }}
),

-- ---------- FDA: free-text parse over the domestic segment ----------
fda_raw as (
    select recall_event_id, distribution_area_summary as t
    from {{ ref('recall_event') }}
    where source = 'FDA' and distribution_area_summary is not null
),

fda_dom as (
    -- domestic head only: strip from the first international marker to end of text.
    select
        recall_event_id,
        regexp_replace(
            t,
            '(\mOUS\M|internationa|foreign|countr(y|ies)|\mROW\M|outside\s+(the\s+)?u\.?\s*s|abroad).*',
            '',
            'i'
        ) as dom
    from fda_raw
),

fda_dom2 as (
    -- remember West Virginia, then strip it so the bare "Virginia" name test is safe.
    select
        recall_event_id,
        (dom ~* '\ywest virginia\y')                              as has_wv,
        regexp_replace(dom, '\ywest virginia\y', '  ', 'gi')      as dom
    from fda_dom
),

-- Georgia-the-country guard (precision): "Georgia" is the only US-state name that is also a
-- country. The marker-cut already drops short international lists ("OUS: GEORGIA, ARMENIA"); the
-- residual false positives are marker-LESS WORLDWIDE country lists ("...Algeria, Argentina, ...,
-- Georgia, ...", 70+ countries). Count UNAMBIGUOUS foreign-country names — this list deliberately
-- EXCLUDES US-collision names (Mexico/New Mexico, Canada, Georgia itself) — in the domestic text;
-- >=3 means it is a country list, so the name-derived GA is suppressed below. A genuine uppercase
-- "GA" TOKEN (a state code) is unaffected — it still resolves via fda_by_code. Legit US-state rows
-- carry ~0 of these names, so the guard does not drop real Georgia. Curated + slow-growing.
foreign_indicators(name) as (
    values
    ('AFGHANISTAN'),('ALBANIA'),('ALGERIA'),('ANDORRA'),('ANGOLA'),('ARGENTINA'),('ARMENIA'),
    ('AUSTRALIA'),('AUSTRIA'),('AZERBAIJAN'),('BAHRAIN'),('BANGLADESH'),('BELARUS'),('BELGIUM'),
    ('BOLIVIA'),('BRAZIL'),('BULGARIA'),('CAMBODIA'),('CHILE'),('CHINA'),('COLOMBIA'),('CROATIA'),
    ('CYPRUS'),('DENMARK'),('ECUADOR'),('EGYPT'),('ESTONIA'),('FINLAND'),('FRANCE'),('GERMANY'),
    ('GREECE'),('GUATEMALA'),('HONDURAS'),('HUNGARY'),('ICELAND'),('INDIA'),('INDONESIA'),('IRAN'),
    ('IRAQ'),('IRELAND'),('ISRAEL'),('ITALY'),('JAPAN'),('JORDAN'),('KAZAKHSTAN'),('KENYA'),
    ('KUWAIT'),('LATVIA'),('LEBANON'),('LITHUANIA'),('LUXEMBOURG'),('MALAYSIA'),('MOROCCO'),
    ('NETHERLANDS'),('NORWAY'),('PAKISTAN'),('PANAMA'),('PARAGUAY'),('PERU'),('PHILIPPINES'),
    ('POLAND'),('PORTUGAL'),('QATAR'),('ROMANIA'),('RUSSIA'),('SERBIA'),('SINGAPORE'),('SLOVAKIA'),
    ('SLOVENIA'),('SPAIN'),('SWEDEN'),('SWITZERLAND'),('TAIWAN'),('THAILAND'),('TURKEY'),('UKRAINE'),
    ('URUGUAY'),('VENEZUELA'),('VIETNAM'),('ZAMBIA')
),

fda_foreign as (
    -- Only Georgia-mentioning rows can trigger the guard (the GA name match requires "Georgia" in
    -- the text), so restrict this expensive per-pair-regex cross-join to them — keeps the guard
    -- O(~900 rows) not O(50k), the difference between a ~6s and a ~110s build. Result is identical:
    -- non-Georgia rows never produce a name-derived GA, so their n_foreign is irrelevant.
    select d.recall_event_id, count(distinct fi.name) as n_foreign
    from fda_dom2 d
    join foreign_indicators fi on d.dom ~* ('\y' || fi.name || '\y')
    where d.dom ~* '\ygeorgia\y'
    group by d.recall_event_id
),

fda_by_name as (
    select d.recall_event_id, a.abbr
    from fda_dom2 d
    join abbr a on d.dom ~* ('\y' || a.name || '\y')
    left join fda_foreign ff on ff.recall_event_id = d.recall_event_id
    where not (a.abbr = 'GA' and coalesce(ff.n_foreign, 0) >= 3)
),

fda_by_code as (
    select d.recall_event_id, a.abbr
    from fda_dom2 d
    cross join lateral regexp_matches(d.dom, '\m([A-Z]{2})\M', 'g') as m
    join abbr a on a.abbr = m[1]
),

fda_wv as (
    select recall_event_id, 'WV'::text as abbr from fda_dom2 where has_wv
),

fda_states as (
    select recall_event_id, abbr from fda_by_name
    union
    select recall_event_id, abbr from fda_by_code
    union
    select recall_event_id, abbr from fda_wv
),

-- ---------- USDA: clean comma-list of state names / codes ----------
usda_tokens as (
    select re.recall_event_id, btrim(tok) as tok
    from {{ ref('recall_event') }} re,
         lateral unnest(string_to_array(re.distribution_states, ',')) as tok
    where re.source = 'USDA' and re.distribution_states is not null
      and btrim(tok) <> ''
),

usda_states as (
    select t.recall_event_id, a.abbr
    from usda_tokens t
    join abbr a on a.name = upper(t.tok) or a.abbr = upper(t.tok)
),

all_states as (
    select recall_event_id, abbr from fda_states
    union
    select recall_event_id, abbr from usda_states
)

select
    recall_event_id,
    array_agg(distinct abbr order by abbr) as distribution_state_codes,
    count(distinct abbr)::int              as n_distribution_states
from all_states
group by recall_event_id
