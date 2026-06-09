{{ config(
    materialized='table',
    indexes=[
      {'columns': ['recall_event_id'], 'unique': True},
      {'columns': ['distribution_state_codes'], 'type': 'gin'},
      {'columns': ['distribution_country_codes'], 'type': 'gin'},
    ]
) }}

-- recall_distribution_area — Tier-2 structured distribution geography (Phase 6e geography
-- foundation; distribution_country_codes added 2026-06-09, C12). Pulls forward the deferred
-- distribution_states[]/countries[] backlog item (ADR 0036 D7 / cross_source_consolidation.md
-- §163). One row per recall_event with >=1 parseable US state OR >=1 parseable foreign country —
-- FDA + USDA only (the two sources with a distribution field; CPSC/NHTSA/USCG carry none). Feeds
-- fct_recalls_by_geography (distribution lens) and mart_recall_summary. recall_events with no
-- parseable geography simply have no row here (the consumers LEFT JOIN); their coarse geography is
-- carried by recall_event.distribution_scope. Either array may be EMPTY: a country-only recall has
-- distribution_state_codes = {}, a domestic recall has distribution_country_codes = {} (the FULL
-- OUTER at the bottom guarantees a row whenever EITHER side parsed something).
--
-- PRECISION DESIGN (tuned to the 2026-06-07 full-corpus profile —
-- scripts/sql/cross_source/silver/profile_distribution_geography.sql; precision-over-recall):
--   * FDA distribution_area_summary is free text. The dominant false-positive mode is FOREIGN
--     2-letter ISO codes in international sections — "INTERNATIONAL ONLY: GB, FR, DE, ..." (DE =
--     Germany, not Delaware), "...countries: AU, BG, CA, ..." (CA = Canada, not California),
--     "US: IN, MI, TX. OUS: GERMANY...". Mitigation: cut the text at the FIRST international marker
--     (OUS / internationally / foreign / countries: / ROW / outside the US / abroad) and parse
--     ONLY the domestic head for STATES. The mirror image feeds COUNTRIES: parse only the
--     international TAIL (from that marker to end) for country NAMES (see the country block below).
--     FDA records are reliably states-first / international-after, so both halves are high-precision.
--     NOTE: 'worldwide' is deliberately NOT a STATE marker (states are routinely listed AFTER it —
--     "US Nationwide-Worldwide Distribution: CO, MS, HI, ...") but IS a COUNTRY marker.
--   * State NAMES are matched whole-word against the us_state_abbr seed. "West Virginia" is
--     stripped before the bare "Virginia" test — the only state-name-in-state-name overlap.
--   * 2-letter codes are matched only as UPPERCASE standalone tokens (so lowercase words
--     "in"/"or"/"ok" never match) that are known USPS codes (the seed join filters APO/foreign).
--   * USDA distribution_states is a clean comma list (profile Q7: only Nationwide/Midwest are
--     non-state) — comma-token -> name/abbr, same logic as recall_event_establishment_resolution.
--   * Georgia-the-country guard: a marker-less WORLDWIDE country list that names "Georgia" (the
--     country) is detected by an unambiguous-foreign-country-name count (>=3) and the stray name-
--     derived GA is suppressed — see the foreign_indicators CTE. (53 such rows, 0.14%, at build.)
--   * COUNTRY parse (C12): country NAMES from the curated country_iso seed (ISO-3166-1 alpha-2)
--     are matched whole-word in the INTERNATIONAL TAIL only, gated on a marker being present, so a
--     purely-domestic recall yields no country. This is what keeps US place-names that happen to
--     share a country name (Mexico MO, Lebanon PA, Peru IN, New England) out — they sit in the
--     domestic head, never the tail. "New Mexico" is stripped before the bare "Mexico" test. The
--     seed deliberately EXCLUDES the United States (we want foreign distribution only) and "Georgia"
--     (US-state collision; Georgia-the-country in FDA distribution is effectively nonexistent and
--     the false-positive risk on US-state Georgia is real). Foreign 2-letter codes are NOT matched
--     (too ambiguous with USPS codes — CA/IN/DE/...); names-only, precision-over-recall.
-- PERF: the country block extracts all seed names in ONE alternation regexp_matches pass per
-- international-tail row (country_pattern + fda_countries below), not 155 per-name regex joins. This
-- cut the model build ~84s -> ~13s (2026-06-09). Output is identical to the per-name parse EXCEPT it
-- also corrects a latent over-match: the per-name join matched IRELAND inside "NORTHERN IRELAND" (->GB)
-- and spuriously added IE on 12 recalls; leftmost-longest reads "NORTHERN IRELAND" whole, so IE is no
-- longer emitted unless a standalone Ireland is present (verified: all 12 had no standalone Ireland).

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
),

-- ========================= COUNTRIES (international tail) =========================
country_seed as (
    select upper(name) as name, alpha2 from {{ ref('country_iso') }}
),

-- One alternation pattern over every seed name, built once and reused for all rows (Postgres
-- caches the compiled regex). Longest-first ordering is belt-and-suspenders — Postgres ARE is
-- leftmost-LONGEST, so a multi-word name already beats any shorter prefix. The seed names are
-- verified free of regex metacharacters, so they are safe to splice straight into the pattern.
country_pattern as (
    select '\y(' || string_agg(name, '|' order by length(name) desc, name) || ')\y' as rx
    from country_seed
),

-- FDA: the international TAIL — mirror of the domestic-head state parse. Cut at the FIRST country
-- marker (keep marker..end) and match curated country NAMES there. 'worldwide'/'global'/'export'
-- ARE cut points here (a worldwide/foreign country list is exactly what we read). The marker WHERE
-- filter gates it so a purely-domestic recall (no marker) yields no country. "New Mexico" is
-- stripped so the bare "Mexico" name test never fires on the US state.
fda_intl as (
    -- Keep only recalls that HAVE an international marker, then trim to the tail. Filtering first
    -- shrinks the per-country regex join below from O(all FDA) to O(international FDA) — the same
    -- "keep the cross-join small" discipline as the foreign_indicators guard above.
    select
        recall_event_id,
        regexp_replace(
            regexp_replace(t, '\ynew mexico\y', '  ', 'gi'),
            '^.*?(\mOUS\M|internationa|foreign|countr(y|ies)|worldwide|global|\mROW\M|outside\s+(the\s+)?u\.?\s*s|abroad|export)',
            '\1',
            'i'
        ) as tail
    from fda_raw
    where t ~* '(\mOUS\M|internationa|foreign|countr(y|ies)|worldwide|global|\mROW\M|outside\s+(the\s+)?u\.?\s*s|abroad|export)'
),

-- ONE regexp_matches pass per international-tail row extracts every country name at once (the
-- fda_by_code idiom) and joins each hit back to its ISO code — instead of 155 per-name regex joins.
-- ~75s faster, and leftmost-longest also fixes the NORTHERN IRELAND->IE over-match (see header PERF).
fda_countries as (
    select i.recall_event_id, cs.alpha2
    from fda_intl i
    cross join country_pattern p
    cross join lateral regexp_matches(i.tail, p.rx, 'gi') as m
    join country_seed cs on cs.name = upper(m[1])
),

-- USDA: distribution_states is a clean comma list that is states-only per the 2026-06-07 profile
-- (Nationwide/Midwest the only non-state tokens), so this yields nothing today — included for
-- symmetry and to capture any future country token (exact comma-token match, not substring).
usda_countries as (
    select t.recall_event_id, cs.alpha2
    from usda_tokens t
    join country_seed cs on cs.name = upper(t.tok)
),

all_countries as (
    select recall_event_id, alpha2 from fda_countries
    union
    select recall_event_id, alpha2 from usda_countries
),

-- ============== aggregate + FULL OUTER (grain = >=1 state OR >=1 country) ==============
state_agg as (
    select
        recall_event_id,
        array_agg(distinct abbr order by abbr) as distribution_state_codes,
        count(distinct abbr)::int              as n_distribution_states
    from all_states
    group by recall_event_id
),

country_agg as (
    select
        recall_event_id,
        array_agg(distinct alpha2 order by alpha2) as distribution_country_codes,
        count(distinct alpha2)::int                as n_distribution_countries
    from all_countries
    group by recall_event_id
)

select
    coalesce(s.recall_event_id, c.recall_event_id)       as recall_event_id,
    coalesce(s.distribution_state_codes, '{}'::text[])   as distribution_state_codes,
    coalesce(s.n_distribution_states, 0)                 as n_distribution_states,
    coalesce(c.distribution_country_codes, '{}'::text[]) as distribution_country_codes,
    coalesce(c.n_distribution_countries, 0)              as n_distribution_countries
from state_agg s
full outer join country_agg c on c.recall_event_id = s.recall_event_id
