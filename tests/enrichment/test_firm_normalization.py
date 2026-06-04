"""Unit tests for CPSC + NHTSA firm-name normalization (Phase 6b PRs 6b.1 + 6b.3).

Fixtures are REAL corpus strings: CPSC from data/exploratory/cpsc/g1_comma_less_cohort.csv
+ measure_comma_optional_of_strip.sql; NHTSA from
data/exploratory/nhtsa/name_normalization_features.csv. So the regression surface is the
actual data each cleaning decision was validated against (2026-06-03).
"""

import pytest

from src.enrichment.firm_normalization import (
    clean_firm_name,
    clean_nhtsa_firm_name,
    extract_firm_dba,
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


# ── NHTSA parenthetical cleaning (PR 6b.3) ──────────────────────────────────────
# Fixtures from data/exploratory/nhtsa/name_normalization_features.csv (2026-06-03):
# 41 parenthetical names, 0 over-strips, 10 merge clusters (21 names -> 10).

# Balanced (parenthetical) annotation removed -> canonical name.
_NHTSA_STRIP = [
    ("CHRYSLER (FCA US, LLC) (STELLANTIS)", "CHRYSLER"),
    ("CHRYSLER (FCA US, LLC)", "CHRYSLER"),
    ("CHRYSLER (FCA US LLC)", "CHRYSLER"),
    ("TAKATA (TK GLOBAL, LLC)", "TAKATA"),
    ("HONDA (AMERICAN HONDA MOTOR CO.)", "HONDA"),
    ("ALUMINUM TRAILER COMPANY (ATC)", "ALUMINUM TRAILER COMPANY"),
    ("DIONO (FORMERLY SUNSHINE KIDS JUVENILE)", "DIONO"),
    ("APOLLO TIRES (US) INC.", "APOLLO TIRES INC."),
    ("NOVA BUS (US) INC.", "NOVA BUS INC."),
    ("SEMPERIT, A.G.(AUSTRIA)", "SEMPERIT, A.G."),  # no space before paren
    ("HINO DIESEL TRUCKS(USA)", "HINO DIESEL TRUCKS"),  # no space before paren
    ("GENERAL RUBBER (THAILAND) CO., LTD", "GENERAL RUBBER CO., LTD"),
    ("MAZDA (NORTH AMERICA),INC", "MAZDA,INC"),
    ("KEY SAFETY SYSTEMS, INC. - (DBA JOYSON)", "KEY SAFETY SYSTEMS, INC."),
]


@pytest.mark.parametrize("raw,expected", _NHTSA_STRIP)
def test_clean_nhtsa_strips_parentheticals(raw, expected):
    assert clean_nhtsa_firm_name(raw) == expected


# Truncated open paren (CHAR(40) cut-off) or no paren -> left WHOLE.
_NHTSA_KEEP = [
    "AMERICAN PACIFIC INDUSTRIES, INC (A.P.I.",  # truncated open paren
    "TOMY INTERNATIONAL (LEARNING CURVE BRAND",  # truncated open paren
    "FORD MOTOR COMPANY",
    "TOYOTA MOTOR ENGINEERING & MANUFACTURING",
]


@pytest.mark.parametrize("raw", _NHTSA_KEEP)
def test_clean_nhtsa_keeps_whole(raw):
    assert clean_nhtsa_firm_name(raw) == raw


def test_clean_nhtsa_chrysler_variants_collapse():
    # 3 paren spellings -> ONE canonical (the cluster the truncated sample missed).
    forms = [
        "CHRYSLER (FCA US LLC)",
        "CHRYSLER (FCA US, LLC)",
        "CHRYSLER (FCA US, LLC) (STELLANTIS)",
    ]
    assert {clean_nhtsa_firm_name(f) for f in forms} == {"CHRYSLER"}


def test_clean_nhtsa_key_safety_variants_collapse():
    # The "- (DBA JOYSON)" trailing-hyphen form must tidy to merge with the rest.
    forms = [
        "KEY SAFETY SYSTEMS, INC.",
        "KEY SAFETY SYSTEMS, INC. (DBA JOYSON)",
        "KEY SAFETY SYSTEMS, INC. - (DBA JOYSON)",
    ]
    assert {clean_nhtsa_firm_name(f) for f in forms} == {"KEY SAFETY SYSTEMS, INC."}


@pytest.mark.parametrize("raw", ["(SOMETHING)", "(X)", "()"])
def test_clean_nhtsa_never_over_strips(raw):
    # Stripping to < 2 chars would destroy the name -> keep the original (precision-first).
    assert clean_nhtsa_firm_name(raw) == raw


@pytest.mark.parametrize("raw,expected", [("", ""), ("   ", ""), ("  NOVA BUS  (US) ", "NOVA BUS")])
def test_clean_nhtsa_edges(raw, expected):
    assert clean_nhtsa_firm_name(raw) == expected


@pytest.mark.parametrize("raw", [r for r, _ in _NHTSA_STRIP] + _NHTSA_KEEP)
def test_clean_nhtsa_is_idempotent(raw):
    once = clean_nhtsa_firm_name(raw)
    assert clean_nhtsa_firm_name(once) == once


# DBA brand still captured from the NHTSA paren form (extract_firm_dba unchanged).
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("KEY SAFETY SYSTEMS, INC. (DBA JOYSON)", "JOYSON"),
        ("ITR USA, INC. (DBA ITA)", "ITA"),
    ],
)
def test_extract_dba_nhtsa_paren(raw, expected):
    assert extract_firm_dba(raw) == expected
