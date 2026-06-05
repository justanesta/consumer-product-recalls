"""Unit tests for the tiered firm resolver (Phase 6b PR 6b.4: Tier 0 FEI + Tier 1 + Tier 2).

Corpus-scale precision is validated empirically against firm_crosswalk_full.csv; here we pin the
pure mechanics on small controlled inputs (where the generic stop ~= the corp-form stop).
"""

from collections import Counter

import pytest

from src.enrichment.firm_resolution import (
    _BASE_STOP,
    block_key,
    cluster_names,
    document_frequencies,
    fei_resolve,
    generic_stopwords,
    pick_canonical,
)
from src.enrichment.place_words import PLACE_WORDS


# ── helpers ──────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "name,stop,expected",
    [
        ("THE HOME DEPOT", _BASE_STOP, "HOME"),  # article stripped
        ("FISHER-PRICE", _BASE_STOP, "FISHER"),  # punctuation folded
        ("ACME INC", _BASE_STOP, "ACME"),  # corp form skipped
        ("AMERICAN HONDA MOTOR", _BASE_STOP | frozenset({"AMERICAN"}), "HONDA"),
        ("INC LLC", _BASE_STOP, ""),  # all generic -> no block
    ],
)
def test_block_key(name, stop, expected):
    assert block_key(name, stop) == expected


def test_generic_stopwords_and_document_frequencies():
    df = document_frequencies(["ACME INC", "ACME CORP", "BETA INC"])
    assert df["ACME"] == 2 and df["INC"] == 2 and df["BETA"] == 1
    stop = generic_stopwords(Counter({"AMERICAN": 454, "HONDA": 8, "INC": 9000}), cutoff=80)
    assert "AMERICAN" in stop and "INC" in stop and "HONDA" not in stop


def test_pick_canonical_prefers_short_base():
    assert pick_canonical(["AMERICAN HONDA MOTOR CO", "HONDA", "HONDA MOTOR"]) == "HONDA"


def test_place_words_cover_the_observed_hub_tokens():
    for t in ("SAN", "ANTONIO", "ROCKY", "MOUNTAIN", "PUGET", "SOUND", "YORK", "VALLEY"):
        assert t in PLACE_WORDS


# ── Tier 0: fei_resolve (current-FEI grouping + fan-out gate) ─────────────────────
def test_fei_resolve_groups_by_current_fei():
    rows = [("ida", "100", "A"), ("idb", "100", "B"), ("idc", "200", "C")]
    clean = {"ida": "FIRM A", "idb": "FIRM B", "idc": "FIRM C"}
    pairs, gated = fei_resolve(rows, clean)
    assert pairs == [("FIRM A", "FIRM B")] and gated == 0  # 100 -> {A,B}; 200 alone, no pair


def test_fei_resolve_gates_high_fanout():
    # one current-FEI fanning out to 7 distinct names is a registrant/sentinel, not one firm
    rows = [(f"id{c}", "999", c) for c in "DEFGHIJ"]
    clean = {f"id{c}": c for c in "DEFGHIJ"}
    pairs, gated = fei_resolve(rows, clean, fanout_cap=6)
    assert pairs == [] and gated == 1


def test_fei_resolve_skips_unknown_firm_id_and_blank_fei():
    assert fei_resolve([("missing", "1", "X")], {"id": "A"}) == ([], 0)
    assert fei_resolve([("id", "", "X")], {"id": "A"}) == ([], 0)


# ── Tier 1: name repair (identical distinctive set, typo) ─────────────────────────
def test_tier1_name_variant_exact_merges_corp_form_variants():
    asg = cluster_names(["GRACO", "GRACO INC"])  # both -> {GRACO}
    assert asg["GRACO"].canonical == asg["GRACO INC"].canonical
    assert asg["GRACO"].method == "name_variant_exact"


def test_tier1_typo_merges_spelling():
    asg = cluster_names(["BRISTOL MEYERS SQUIBB", "BRISTOL MYERS SQUIBB"])
    assert asg["BRISTOL MEYERS SQUIBB"].canonical == asg["BRISTOL MYERS SQUIBB"].canonical
    assert asg["BRISTOL MEYERS SQUIBB"].method == "name_typo_high"
    assert asg["BRISTOL MEYERS SQUIBB"].score is not None


def test_tier1_keeps_distinct_content_words_separate():
    # the SUN-hub regression: "Sun Foods" must NOT collapse onto bare "Sun" (different firms)
    asg = cluster_names(["SUN", "SUN FOODS", "SUN VALLEY"])
    assert len({a.canonical for a in asg.values()}) == 3


def test_tier1_does_not_roll_up_without_flag():
    asg = cluster_names(["KAWASAKI MOTORS CORP USA", "KAWASAKI MOTORS CORP"])  # rollup default off
    assert asg["KAWASAKI MOTORS CORP USA"].canonical != asg["KAWASAKI MOTORS CORP"].canonical


# ── Tier 2: entity rollup + place guard ──────────────────────────────────────────
def test_tier2_rolls_up_shared_distinctive_tokens():
    asg = cluster_names(
        ["KAWASAKI MOTORS CORP USA", "KAWASAKI MOTORS CORP", "KAWASAKI MOTORS MFG"], rollup=True
    )
    assert len({a.canonical for a in asg.values()}) == 1
    assert any(a.method == "rapidfuzz_rollup" for a in asg.values())


def test_tier2_place_guard_blocks_geographic_coincidence():
    names = ["SAN ANTONIO BAKERY", "SAN ANTONIO EYE BANK", "SAN ANTONIO PACKING"]
    asg = cluster_names(names, rollup=True)  # share only the place phrase SAN ANTONIO
    assert len({a.canonical for a in asg.values()}) == 3


def test_tier2_place_guard_allows_real_token_alongside_place():
    # shares CHEESE (real) + a place word -> still merges (not a pure place coincidence)
    asg = cluster_names(["GREAT LAKES CHEESE", "GREAT LAKES CHEESE FOODS"], rollup=True)
    assert asg["GREAT LAKES CHEESE"].canonical == asg["GREAT LAKES CHEESE FOODS"].canonical


# ── Tier 0 wins over fuzzy; surnames block apart; singletons ──────────────────────
def test_fei_must_link_wins_method_label():
    asg = cluster_names(
        ["JONES SEAFOOD", "ATLANTIC FISH CO"], must_link=[("JONES SEAFOOD", "ATLANTIC FISH CO")]
    )
    assert asg["JONES SEAFOOD"].canonical == asg["ATLANTIC FISH CO"].canonical
    assert asg["JONES SEAFOOD"].method == "fei_exact"


def test_initialed_surnames_block_apart():
    asg = cluster_names(["A.O. SMITH", "C.E. SMITH", "E.D. SMITH"], rollup=True)
    assert len({a.canonical for a in asg.values()}) == 3  # different initials -> different blocks


def test_singletons_kept():
    asg = cluster_names(["WHOLLY DISTINCT FIRM"], rollup=True)
    assert asg["WHOLLY DISTINCT FIRM"].method == "singleton"
    assert asg["WHOLLY DISTINCT FIRM"].score is None
