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
    apply_clustering,
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


def _tier_rows():
    return build_crosswalk_rows(
        [
            ("GRACO", "Graco", "cpsc"),
            ("GRACO INC", "Graco Inc", "cpsc"),  # Tier 1: identical distinctive set
            ("KAWASAKI MOTORS CORP USA", "Kawasaki Motors Corp USA", "cpsc"),
            ("KAWASAKI MOTORS CORP", "Kawasaki Motors Corp", "cpsc"),  # Tier 2: >=2-token rollup
            ("BLOODCENTER OF WISCONSIN", "BloodCenter of Wisconsin", "fda"),
            ("BLOOD CTR WISC", "Blood Ctr Wisc", "fda"),  # only a shared FEI links these
            ("ZZZ UNIQUE FIRM", "Zzz Unique Firm", "cpsc"),  # singleton
        ]
    )


_BLOOD_FEI = [
    (_md5("BLOODCENTER OF WISCONSIN"), "555", "BloodCenter of Wisconsin"),
    (_md5("BLOOD CTR WISC"), "555", "BloodCenter of Wisconsin"),
]


# ── apply_clustering: SHIPPED name/brand grain (Tier 1 + 2; FEI off) ──────────────
def test_apply_clustering_name_grain_default():
    rows = _tier_rows()
    fei_merged, fuzzy_merged, fei_gated = apply_clustering(rows, _BLOOD_FEI)  # fei_merge off
    by = {r["clean_name"]: r for r in rows}
    assert (fei_merged, fuzzy_merged, fei_gated) == (0, 4, 0)
    # Tier 1: identical distinctive set -> name_variant_exact
    assert by["Graco"]["canonical_firm_id"] == by["Graco Inc"]["canonical_firm_id"]
    assert by["Graco"]["match_confidence"] == "name_variant_exact"
    # Tier 2: >=2 shared distinctive tokens -> rapidfuzz_rollup
    assert (
        by["Kawasaki Motors Corp"]["canonical_firm_id"]
        == by["Kawasaki Motors Corp USA"]["canonical_firm_id"]
    )
    assert by["Kawasaki Motors Corp"]["match_confidence"] == "rapidfuzz_rollup"
    # FEI is NOT a merge key by default -> the two blood-center spellings stay distinct firms
    assert (
        by["BloodCenter of Wisconsin"]["canonical_firm_id"]
        != by["Blood Ctr Wisc"]["canonical_firm_id"]
    )
    assert by["BloodCenter of Wisconsin"]["match_confidence"] == "exact_name"
    assert by["Zzz Unique Firm"]["firm_id"] == by["Zzz Unique Firm"]["canonical_firm_id"]
    assert all(r["resolver_version"] == "allsrc-tier12-roll90-v4" for r in rows)


# ── apply_clustering: Tier 0 FEI is opt-in (deferred) ────────────────────────────
def test_apply_clustering_fei_merge_opt_in():
    rows = _tier_rows()
    fei_merged, fuzzy_merged, fei_gated = apply_clustering(rows, _BLOOD_FEI, fei_merge=True)
    by = {r["clean_name"]: r for r in rows}
    assert fei_merged == 2  # now the shared FEI links the blood-center spellings
    assert (
        by["BloodCenter of Wisconsin"]["canonical_firm_id"]
        == by["Blood Ctr Wisc"]["canonical_firm_id"]
    )
    assert by["BloodCenter of Wisconsin"]["match_confidence"] == "fei_exact"
    assert all(r["resolver_version"] == "allsrc-tier012-roll90-v4" for r in rows)


def test_apply_clustering_no_rollup_keeps_entity_rollup_split():
    rows = build_crosswalk_rows(
        [
            ("KAWASAKI MOTORS CORP USA", "Kawasaki Motors Corp USA", "cpsc"),
            ("KAWASAKI MOTORS CORP", "Kawasaki Motors Corp", "cpsc"),
        ]
    )
    apply_clustering(rows, [], rollup=False)  # Tier 2 off
    assert rows[0]["canonical_firm_id"] != rows[1]["canonical_firm_id"]
    assert all(r["resolver_version"] == "allsrc-tier1-roll90-v4" for r in rows)
    assert rows[0]["match_confidence"] == "exact_name"
