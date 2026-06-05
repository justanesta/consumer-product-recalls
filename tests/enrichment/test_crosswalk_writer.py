"""Unit tests for build_crosswalk_rows + _geo_mode_for (Phase 6b PRs 6b.1 + 6b.4).

The DB read/write of resolve_firm_crosswalk is exercised by the integration suite; here we
pin the pure row-shape: firm_id key, canonical collapse, the SOURCE-GATED geo path (ADR 0037
amendment), the match_confidence tiers (dba / geo / exact — geo only when it actually fired),
and the alternate_names list.
"""

import hashlib

import pytest

from src.enrichment.crosswalk_writer import (
    RESOLVER_VERSION,
    _geo_mode_for,
    build_crosswalk_rows,
)


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ── _geo_mode_for: structured-id sources -> geo off (precedence off > guarded > full) ──
@pytest.mark.parametrize(
    "sources,expected",
    [
        ("cpsc", "full"),
        ("nhtsa", "guarded"),
        ("cpsc,nhtsa", "guarded"),  # any nhtsa (no id-source) -> guarded
        ("fda", "off"),
        ("usda", "off"),
        ("uscg", "off"),
        ("cpsc,fda", "off"),  # structured-id source wins the shared name
        ("cpsc,nhtsa,uscg", "off"),
    ],
)
def test_geo_mode_for(sources, expected):
    assert _geo_mode_for(sources) == expected


def test_cpsc_geo_strip_sets_suffix_confidence_and_distinct_canonical():
    [row] = build_crosswalk_rows(
        [("FISHER-PRICE OF EAST AURORA, N.Y.", "Fisher-Price of East Aurora, N.Y.", "cpsc")]
    )
    assert row["firm_id"] == _md5("FISHER-PRICE OF EAST AURORA, N.Y.")
    assert row["canonical_firm_id"] == _md5("FISHER-PRICE")
    assert row["firm_id"] != row["canonical_firm_id"]
    assert row["clean_name"] == "Fisher-Price"
    assert row["alternate_names"] is None
    assert row["match_confidence"] == "geo_suffix_strip_exact"
    assert row["match_score"] is None
    assert row["resolver_version"] == RESOLVER_VERSION


def test_blocklist_keeps_integral_of_whole():
    [row] = build_crosswalk_rows([("BANK OF AMERICA", "Bank of America", "cpsc")])
    assert row["firm_id"] == row["canonical_firm_id"] == _md5("BANK OF AMERICA")
    assert row["match_confidence"] == "exact_name"


def test_dba_name_sets_dba_confidence_and_alias():
    raw = "Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China"
    [row] = build_crosswalk_rows([(raw.upper(), raw, "cpsc")])
    assert row["alternate_names"] == ["ZOLIQUEX"]
    assert row["clean_name"] == "Cheyouhang Technology Shenzhen Co., Ltd."
    assert row["match_confidence"] == "dba_extract_exact"


# ── source-gated geo: FDA/USDA/USCG = off, NHTSA = guarded ───────────────────────
def test_fda_geo_off_keeps_integral_of_state_name():
    # "BloodCenter of Wisconsin" is an integral FDA establishment name; geo OFF keeps it whole
    # (the FEI resolves within-source identity). Geo-on would over-strip it to "BloodCenter".
    raw = "BLOODCENTER OF WISCONSIN, INC."
    [row] = build_crosswalk_rows([(raw, raw, "fda")])
    assert row["clean_name"] == "BLOODCENTER OF WISCONSIN, INC."
    assert row["firm_id"] == row["canonical_firm_id"] == _md5(raw)
    assert row["match_confidence"] == "exact_name"


def test_nhtsa_guard_blocks_single_token_overstrip():
    # Integral-name guard: "WINNEBAGO OF INDIANA" must NOT strip to the bare "WINNEBAGO"
    # (which would collide with the distinct WINNEBAGO INDUSTRIES INC.).
    raw = "WINNEBAGO OF INDIANA, LLC"
    [row] = build_crosswalk_rows([(raw, raw, "nhtsa")])
    assert row["clean_name"] == "WINNEBAGO OF INDIANA, LLC"
    assert row["match_confidence"] == "exact_name"


def test_nhtsa_guarded_strips_multitoken_dealer_cohort():
    # A multi-token base is safe even in guarded mode (the legit NHTSA dealer cohort).
    raw = "AUTO TRIM DESIGN OF TEXAS"
    [row] = build_crosswalk_rows([(raw, raw, "nhtsa")])
    assert row["clean_name"] == "AUTO TRIM DESIGN"
    assert row["canonical_firm_id"] == _md5("AUTO TRIM DESIGN")
    assert row["match_confidence"] == "geo_suffix_strip_exact"


def test_nhtsa_paren_is_not_stripped_but_captured_as_alias():
    raw = "CHRYSLER (FCA US, LLC)"
    [row] = build_crosswalk_rows([(raw, raw, "nhtsa")])
    assert row["clean_name"] == "CHRYSLER (FCA US, LLC)"
    assert row["firm_id"] == row["canonical_firm_id"] == _md5("CHRYSLER (FCA US, LLC)")
    assert row["alternate_names"] == ["FCA US, LLC"]
    assert row["match_confidence"] == "exact_name"


def test_nhtsa_paren_variants_do_not_collapse_deterministically():
    # Paren strip is nowhere; the three Chrysler spellings stay DISTINCT after the
    # deterministic floor — RapidFuzz (Increment 2) is what unifies them.
    rows = build_crosswalk_rows(
        [
            ("CHRYSLER (FCA US, LLC) (STELLANTIS)", "CHRYSLER (FCA US, LLC) (STELLANTIS)", "nhtsa"),
            ("CHRYSLER (FCA US, LLC)", "CHRYSLER (FCA US, LLC)", "nhtsa"),
            ("CHRYSLER", "CHRYSLER", "nhtsa"),
        ]
    )
    assert len({r["canonical_firm_id"] for r in rows}) == 3


def test_cpsc_geo_variants_collapse_to_one_canonical():
    rows = build_crosswalk_rows(
        [
            ("FISHER-PRICE OF EAST AURORA, N.Y.", "Fisher-Price of East Aurora, N.Y.", "cpsc"),
            (
                "FISHER-PRICE OF EAST AURORA, NEW YORK",
                "Fisher-Price of East Aurora, New York",
                "cpsc",
            ),
            ("FISHER-PRICE", "Fisher-Price", "cpsc"),
        ]
    )
    assert {r["canonical_firm_id"] for r in rows} == {_md5("FISHER-PRICE")}  # all collapse
    assert len({r["firm_id"] for r in rows}) == 3  # but keep distinct raw keys


def test_empty_input():
    assert build_crosswalk_rows([]) == []
