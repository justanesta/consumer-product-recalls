"""Unit tests for build_crosswalk_rows (Phase 6b PR 6b.1) — the pure transform.

The DB read/write of resolve_firm_crosswalk is exercised by the integration suite;
here we pin the pure row-shape: firm_id key, canonical collapse, match_confidence
path, and DBA extraction.
"""

import hashlib

from src.enrichment.crosswalk_writer import RESOLVER_VERSION, build_crosswalk_rows


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def test_stripped_name_sets_suffix_confidence_and_distinct_canonical():
    [row] = build_crosswalk_rows(
        [("FISHER-PRICE OF EAST AURORA, N.Y.", "Fisher-Price of East Aurora, N.Y.")]
    )
    assert row["firm_id"] == _md5("FISHER-PRICE OF EAST AURORA, N.Y.")
    assert row["canonical_firm_id"] == _md5("FISHER-PRICE")  # cleaned -> new canonical
    assert row["firm_id"] != row["canonical_firm_id"]
    assert row["clean_name"] == "Fisher-Price"
    assert row["canonical_name"] == "Fisher-Price"
    assert row["extracted_dba"] is None
    assert row["match_confidence"] == "cpsc_suffix_strip_exact"
    assert row["match_score"] is None
    assert row["resolver_version"] == RESOLVER_VERSION


def test_unchanged_name_is_its_own_canonical():
    [row] = build_crosswalk_rows([("BANK OF AMERICA", "Bank of America")])
    assert row["firm_id"] == row["canonical_firm_id"] == _md5("BANK OF AMERICA")
    assert row["clean_name"] == "Bank of America"
    assert row["match_confidence"] == "exact_name"


def test_dba_name_sets_dba_confidence_and_brand():
    raw = "Cheyouhang Technology Shenzhen Co., Ltd., dba ZOLIQUEX, of China"
    [row] = build_crosswalk_rows([(raw.upper(), raw)])
    assert row["extracted_dba"] == "ZOLIQUEX"
    assert row["clean_name"] == "Cheyouhang Technology Shenzhen Co., Ltd."
    assert row["match_confidence"] == "cpsc_dba_extract_exact"


def test_raw_variants_collapse_to_one_canonical():
    rows = build_crosswalk_rows(
        [
            ("FISHER-PRICE OF EAST AURORA, N.Y.", "Fisher-Price of East Aurora, N.Y."),
            ("FISHER-PRICE OF EAST AURORA, NEW YORK", "Fisher-Price of East Aurora, New York"),
            ("FISHER-PRICE", "Fisher-Price"),
        ]
    )
    canonicals = {r["canonical_firm_id"] for r in rows}
    assert canonicals == {_md5("FISHER-PRICE")}  # all three collapse
    assert len({r["firm_id"] for r in rows}) == 3  # but keep distinct raw keys


def test_empty_input():
    assert build_crosswalk_rows([]) == []
