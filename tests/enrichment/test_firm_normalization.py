"""Unit tests for cross-source firm-name normalization (Phase 6b PRs 6b.1 + 6b.4).

Fixtures are REAL corpus strings: CPSC from data/exploratory/cpsc/g1_comma_less_cohort.csv
+ measure_comma_optional_of_strip.sql; the cross-source paren cases from
data/exploratory/cross_source/cleaning_blast_radius_by_source.csv. So the regression
surface is the actual data each cleaning decision was validated against (2026-06-03/04).
"""

import pytest

from src.enrichment.firm_normalization import (
    clean_firm_name,
    extract_firm_dba,
    extract_paren_aliases,
)

# Trailing geographic suffix -> stripped to the canonical legal name.
_STRIP = [
    ("ZOLIQUEX, of China", "ZOLIQUEX"),
    ("Apex Gaming PCs Inc., of Houston, Texas", "Apex Gaming PCs Inc."),
    ("Aria Child Inc. of Dedham, Mass.", "Aria Child Inc."),  # comma-less "of"
    ("Fisher-Price of East Aurora, N.Y.", "Fisher-Price"),
    ("Acme United of Rocky Mount, North Carolina", "Acme United"),  # multi-word state
    ("American Honda Motor Co. of Torrance, Calif.", "American Honda Motor Co."),
    ("Altar'd State of Knoxville, Tenn.", "Altar'd State"),  # 'state' NOT blocklisted
    ("Ballard Designs, Inc. of Atlanta, Ga.", "Ballard Designs, Inc."),  # 'Ga.' abbrev
    ("Deere & Company of Moline, IL", "Deere & Company"),  # 'IL' postal
    ("Bombardier Recreational Products Inc. of Canada", "Bombardier Recreational Products Inc."),
    ("Eugster/Frismag of Switzerland", "Eugster/Frismag"),
    ("Walmart of Bentonville, Ark.", "Walmart"),
]


@pytest.mark.parametrize("raw,expected", _STRIP)
def test_clean_strips_trailing_geo(raw, expected):
    assert clean_firm_name(raw) == expected


# Integral / narrative / place-internal "of", non-geo tail, or city-only tail -> left WHOLE.
_KEEP = [
    "Bank of America",  # 'bank of'
    "FAO Schwarz Inc., King of Prussia, Pa.",  # 'king of' (place name)
    "Boy Scouts of America, Charlotte, N.C.",  # 'scouts of' wins over the trailing geo
    "Book Club of the Month",  # 'club of'
    "Asweets Global Inc., City of Industry, California",  # 'city of'
    "Comtrad, Division of Waljon Ltd., Mississauga, Ontario, Canada",  # 'division of'
    "Pines of America",  # 'America' is not a strip target
    "Nintendo of America Inc.",  # subsidiary marker, kept
    "Warehouse of Fashion",  # non-geo tail
    "Husqvarna Zenoah Co. Ltd., an affiliate of Husqvarna Professional Products Inc., Charlotte, N.C.",  # noqa: E501
    "Tristar Products Inc. on behalf of Empower Brands, LLC of Middleton, Wisconsin, after February 17, 2022",  # noqa: E501
    "El Gringo Imports of Seattle",  # city-only miss (no state/country) — accepted; fuzzy recovers
    "PROFOF DESIGNS",  # glued 'of' is not a word -> never matched
    "Acme Tools",
]


@pytest.mark.parametrize("raw", _KEEP)
def test_clean_keeps_whole(raw):
    assert clean_firm_name(raw) == raw


def test_clean_fixes_greedy_internal_geo():
    # "of Alabama" is part of the name; only the trailing ", of Adamsville, Ala." is geo.
    # Greedy-leftmost would wrongly yield "Fireworks".
    assert (
        clean_firm_name("Fireworks of Alabama, Inc. of Adamsville, Ala.")
        == "Fireworks of Alabama, Inc."
    )


# ── geo_mode gate (ADR 0037 amendment): on for CPSC/NHTSA, off for FEI/estab/MIC sources ──
# 'off' (FDA/USDA/USCG) — never strip a geo suffix; integral "X of <State>" names stay whole.
@pytest.mark.parametrize(
    "raw",
    [
        "BLOODCENTER OF WISCONSIN, INC.",  # FDA integral name geo-strip would corrupt
        "Fisher-Price of East Aurora, N.Y.",  # would strip under 'full', kept under 'off'
        "BAKERY EXPRESS OF CENTRAL FLORIDA, INC.",
        "PACIFIC FOODS OF OREGON, INC",
    ],
)
def test_geo_off_keeps_what_full_strips(raw):
    # geo OFF must NOT strip the geo tail that geo FULL would (the over-strip these sources avoid).
    off = clean_firm_name(raw, geo_mode="off")
    assert off != clean_firm_name(raw, geo_mode="full")
    assert clean_firm_name(off, geo_mode="off") == off  # idempotent


# 'guarded' (NHTSA) — strip a multi-token base, but NEVER reduce to a bare single token.
_GUARDED_BLOCK = [
    "WINNEBAGO OF INDIANA, LLC",  # -> WINNEBAGO would collide with WINNEBAGO INDUSTRIES INC.
    "CAPACITY OF TEXAS",
    "IDEAL OF IDAHO, INC.",
    "CAREFREE OF COLORADO",
]
_GUARDED_STRIP = [
    ("AUTO TRIM DESIGN OF TEXAS", "AUTO TRIM DESIGN"),  # multi-token dealer cohort still strips
    ("BEALL TRAILERS OF OREGON", "BEALL TRAILERS"),
    ("KAUFMAN TRAILERS OF NC, INC.", "KAUFMAN TRAILERS"),
]


@pytest.mark.parametrize("raw", _GUARDED_BLOCK)
def test_geo_guarded_blocks_single_token(raw):
    # guarded keeps the name whole (no bare-single-token over-strip)...
    assert clean_firm_name(raw, geo_mode="guarded") == clean_firm_name(raw, geo_mode="off")
    # ...whereas 'full' WOULD over-strip it to a bare token (that is the bug the guard prevents)
    assert len(clean_firm_name(raw, geo_mode="full").split()) <= 1


@pytest.mark.parametrize("raw,expected", _GUARDED_STRIP)
def test_geo_guarded_strips_multitoken(raw, expected):
    assert clean_firm_name(raw, geo_mode="guarded") == expected


def test_geo_full_is_the_default():
    assert clean_firm_name("Walmart of Bentonville, Ark.") == clean_firm_name(
        "Walmart of Bentonville, Ark.", geo_mode="full"
    )


# DBA clause + trailing geo both removed for the canonical legal name.
_DBA_AND_GEO = [
    (
        "Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China",
        "Cheyouhang Technology Shenzhen Co., Ltd.",
    ),
    (
        "Bexco Enterprises Inc., dba Million Dollar Baby of Montebello, Calif.",
        "Bexco Enterprises Inc.",
    ),
    (
        "Shenzhen Maikeer Industrial Co., Ltd., doing business as MalkerDirect, of China",
        "Shenzhen Maikeer Industrial Co., Ltd.",
    ),
    (
        "Foshan Kangzhibao Furniture Co., Ltd. (doing business as EVLWZL) of China",
        "Foshan Kangzhibao Furniture Co., Ltd.",
    ),
    ("BabyUnited LLC dba BabyLegs of Seattle, Wash.", "BabyUnited LLC"),
]


@pytest.mark.parametrize("raw,expected", _DBA_AND_GEO)
def test_clean_strips_dba_and_geo(raw, expected):
    assert clean_firm_name(raw) == expected


# DBA brand extraction (inline + parenthetical, all three forms incl. d.b.a.).
_DBA = [
    ("Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China", "ZOLIQUEX"),
    (
        "Shenzhen Maikeer Industrial Co., Ltd., doing business as MalkerDirect, of China",
        "MalkerDirect",
    ),
    (
        "Bexco Enterprises, Inc., d.b.a. Million Dollar Baby of Montebello, Cal",
        "Million Dollar Baby",
    ),
    ("Foshan Kangzhibao Furniture Co., Ltd. (doing business as EVLWZL) of China", "EVLWZL"),
    ("Chi Hsin Impex Inc., d/b/a Impex Fitness of Pomona, California", "Impex Fitness"),
    ("BabyUnited LLC dba BabyLegs of Seattle, Wash.", "BabyLegs"),
]


@pytest.mark.parametrize("raw,expected", _DBA)
def test_extract_dba(raw, expected):
    assert extract_firm_dba(raw) == expected


@pytest.mark.parametrize(
    "raw", ["Acme United of Rocky Mount, North Carolina", "Acme Tools", "", "   "]
)
def test_extract_dba_none(raw):
    assert extract_firm_dba(raw) is None


@pytest.mark.parametrize("raw", [r for r, _ in _STRIP] + _KEEP)
def test_clean_is_idempotent(raw):
    once = clean_firm_name(raw)
    assert clean_firm_name(once) == once


@pytest.mark.parametrize(
    "raw,expected", [("", ""), ("   ", ""), ("  Acme   Tools  ", "Acme Tools")]
)
def test_clean_edges(raw, expected):
    assert clean_firm_name(raw) == expected


# ── Parentheticals are NOT stripped (PR 6b.4, ADR 0037) ─────────────────────────
# The cross-source blast-radius review (cleaning_blast_radius_by_source.csv, 2026-06-04)
# showed a blanket paren strip is too blunt — abbreviation-prefix over-truncation, brand
# loss, (DBA) mashups. So clean_firm_name leaves non-DBA parens WHOLE (RapidFuzz handles
# the variants); only brand-bearing parens are lifted into alternate_names.
@pytest.mark.parametrize(
    "raw",
    [
        "CHRYSLER (FCA US, LLC)",  # parent-corp paren stays (was over-stripped to CHRYSLER)
        "FENGM (HONG KONG FENGMANG INTERNATIONAL CO. LTD.)",  # abbreviation prefix kept whole
        "DEERE & COMPANY (JOHN DEERE)",
        "RECREATIONAL EQUIPMENT INC. (REI)",
    ],
)
def test_clean_keeps_non_dba_parentheticals(raw):
    assert clean_firm_name(raw) == raw


# ── (DBA)-marker-alone form, brand OUTSIDE the parens (bucket 4 fix) ─────────────
_DBA_MARKER = [
    ("ANNONA COMPANY, LLC (DBA) HONEST FOODS", "ANNONA COMPANY, LLC", "HONEST FOODS"),
    ("ED ROLLER, INC. (DBA) ROLLER'S HORSERADISH", "ED ROLLER, INC.", "ROLLER'S HORSERADISH"),
    (
        "RICKETTS INVESTMENT GROUP (DBA) L & H INDUSTRIES",
        "RICKETTS INVESTMENT GROUP",
        "L & H INDUSTRIES",
    ),
]


@pytest.mark.parametrize("raw,clean,brand", _DBA_MARKER)
def test_dba_marker_alone_form(raw, clean, brand):
    # The brand is outside the "(DBA)" parens; clean drops it, extract captures it (no mashup).
    assert clean_firm_name(raw) == clean
    assert extract_firm_dba(raw) == brand


# ── extract_paren_aliases: keep brand parens, drop noise ─────────────────────────
_ALIAS_KEEP = [
    ("DEERE & COMPANY (JOHN DEERE)", ["JOHN DEERE"]),  # multiword brand
    ("NATIONAL PRESTO INDUSTRIES INC. (PRESTO), OF EAU CLAIRE, WIS.", ["PRESTO"]),  # shared token
    ("INTER-CITY PRODUCTS (ARCOAIRE, COMFORTMAKER)", ["ARCOAIRE, COMFORTMAKER"]),  # brands
    ("BCI (BUS & COACH INTERNATIONAL)", ["BUS & COACH INTERNATIONAL"]),  # corp-less multiword
    # First paren kept (corp form); second paren STELLANTIS (single word, no shared token)
    # is the documented skip — same class as the accepted "(Texsport)" gap.
    ("CHRYSLER (FCA US, LLC) (STELLANTIS)", ["FCA US, LLC"]),
]


@pytest.mark.parametrize("raw,expected", _ALIAS_KEEP)
def test_extract_paren_aliases_keeps_brands(raw, expected):
    assert extract_paren_aliases(raw) == expected


_ALIAS_DROP = [
    "ACER INC., OF TAIWAN (COMPUTERS)",  # product qualifier
    "MECO CORP., OF GREENEVILLE, TENN. (GRILL)",  # product qualifier
    "ERO INDUSTRIES (NO LONGER IN BUSINESS), OF MOUNT PROSPECT, ILL.",  # status
    "BAYSIDE FURNISHINGS (A DIVISION OF WHALEN), OF SAN DIEGO, CALIF.",  # narrative
    "KEY SAFETY SYSTEMS, INC. (DBA JOYSON)",  # DBA -> owned by extract_firm_dba
    "AMERICAN NATIONAL RED CROSS (THE)",  # article
    "BLACK & DECKER (U.S.) INC., OF TOWSON, MD.",  # region
    "POLYGROUP NORTH AMERICA INC. (EL PASO, TEXAS)",  # trailing City, State
    "ENGLISH RIDING SUPPLY LLC, OF SCRANTON, PENNSYLVANIA (12/31/2021-PRESENT)",  # date
    "FORD MOTOR COMPANY",  # no paren at all
]


@pytest.mark.parametrize("raw", _ALIAS_DROP)
def test_extract_paren_aliases_drops_noise(raw):
    assert extract_paren_aliases(raw) == []


# DBA brand still captured from the parenthetical "(DBA X)" form (extract_firm_dba).
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("KEY SAFETY SYSTEMS, INC. (DBA JOYSON)", "JOYSON"),
        ("ITR USA, INC. (DBA ITA)", "ITA"),
    ],
)
def test_extract_dba_nhtsa_paren(raw, expected):
    assert extract_firm_dba(raw) == expected
