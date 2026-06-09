from __future__ import annotations

from decimal import Decimal

import pytest

from src.enrichment.quantity import (
    PER_PRODUCT,
    TOTAL_ALL_PRODUCTS,
    UNKNOWN,
    ParsedQuantity,
    parse_quantity,
)

D = Decimal

# Cases are drawn from the real FDA+USDA grammar corpus (scripts/sql/cross_source/bronze dump).
# Tuple: (raw, value, unit, category, basis).
_CASES: list[tuple[str, Decimal | None, str | None, str | None, str]] = [
    # --- clean count units (the 58.7% core) ---
    ("280 units", D("280"), "each", "count", PER_PRODUCT),
    ("1 unit", D("1"), "each", "count", PER_PRODUCT),
    ("1,200 cases", D("1200"), "case", "count", PER_PRODUCT),
    ("2,182 bottles", D("2182"), "bottle", "count", PER_PRODUCT),
    ("1,650 vials", D("1650"), "vial", "count", PER_PRODUCT),
    ("1 vial", D("1"), "vial", "count", PER_PRODUCT),
    ("13 kits", D("13"), "kit", "count", PER_PRODUCT),
    ("3,828 syringes", D("3828"), "syringe", "count", PER_PRODUCT),
    ("48 ea", D("48"), "each", "count", PER_PRODUCT),
    ("2,471cs", D("2471"), "case", "count", PER_PRODUCT),  # no space before unit
    ("114 units.", D("114"), "each", "count", PER_PRODUCT),  # trailing period
    # --- weight / volume / grouping with category ---
    ("1,645 lbs", D("1645"), "pound", "weight", PER_PRODUCT),
    ("30lbs.", D("30"), "pound", "weight", PER_PRODUCT),
    ("165 kg", D("165"), "kilogram", "weight", PER_PRODUCT),
    ("199 tons", D("199"), "ton", "weight", PER_PRODUCT),
    ("563 ml", D("563"), "milliliter", "volume", PER_PRODUCT),
    ("101 gallons", D("101"), "gallon", "volume", PER_PRODUCT),
    ("476 component", D("476"), "component", "grouping", PER_PRODUCT),
    ("153 lots", D("153"), "lot", "grouping", PER_PRODUCT),
    # --- modifier-skip: unit is the first taxonomy NOUN, not the first word ---
    ("1,138 units total", D("1138"), "each", "count", TOTAL_ALL_PRODUCTS),
    ("335 total units", D("335"), "each", "count", TOTAL_ALL_PRODUCTS),
    ("187 retail units total", D("187"), "each", "count", TOTAL_ALL_PRODUCTS),
    ("278 units in total", D("278"), "each", "count", TOTAL_ALL_PRODUCTS),
    ("57 individual units", D("57"), "each", "count", PER_PRODUCT),
    ("106 various bottles and jars", D("106"), "bottle", "count", PER_PRODUCT),
    # --- basis=total signals (number is a recall-wide total) ---
    (
        "919,616.31 total pounds, for all products",
        D("919616.31"),
        "pound",
        "weight",
        TOTAL_ALL_PRODUCTS,
    ),
    ("693,408 cases (total for all products)", D("693408"), "case", "count", TOTAL_ALL_PRODUCTS),
    ("266 cases (total)", D("266"), "case", "count", TOTAL_ALL_PRODUCTS),
    ("12,898 lbs (total of all products)", D("12898"), "pound", "weight", TOTAL_ALL_PRODUCTS),
    ("232575.4 lbs (total for all products)", D("232575.4"), "pound", "weight", TOTAL_ALL_PRODUCTS),
    # --- bare number (value, no unit) ---
    ("500", D("500"), None, None, PER_PRODUCT),
    ("5,947 total", D("5947"), None, None, TOTAL_ALL_PRODUCTS),
    # --- "approximately" prefix stripped ---
    (
        "Approximately 464,565 cases of pet food were distributed",
        D("464565"),
        "case",
        "count",
        PER_PRODUCT,
    ),
    (
        "Approximately 1099 total units for all products",
        D("1099"),
        "each",
        "count",
        TOTAL_ALL_PRODUCTS,
    ),
    (
        "approx 6,000,000 pounds for all products",
        D("6000000"),
        "pound",
        "weight",
        TOTAL_ALL_PRODUCTS,
    ),
    # --- spelled-out leading numbers ---
    ("one unit", D("1"), "each", "count", PER_PRODUCT),
    ("Two units", D("2"), "each", "count", PER_PRODUCT),
    # --- geo split / compound: take the leading number; unit None when next token isn't a unit ---
    ("1547 US; 2772 OUS", D("1547"), None, None, PER_PRODUCT),
    ("664 x 12 oz", D("664"), None, None, PER_PRODUCT),
    ("26,160 cartons of 110 chewing pieces", D("26160"), "carton", "count", PER_PRODUCT),
    # --- niche / unknown units: value kept, unit None (deferred tail) ---
    ("126 corneas", D("126"), None, None, PER_PRODUCT),
    ("97 loaves", D("97"), None, None, PER_PRODUCT),
    # --- sentinels -> all NULL, basis unknown ---
    ("Unknown", None, None, None, UNKNOWN),
    ("unknown.", None, None, None, UNKNOWN),
    ("N/A", None, None, None, UNKNOWN),
    ("Undetermined", None, None, None, UNKNOWN),
    ("all", None, None, None, UNKNOWN),
    ("xx", None, None, None, UNKNOWN),
    ("see line 1.", None, None, None, UNKNOWN),
    ("All product produced in the facility", None, None, None, UNKNOWN),
    # --- USDA shapes ---
    ("3,090 pounds", D("3090"), "pound", "weight", PER_PRODUCT),
    ("354 lbs", D("354"), "pound", "weight", PER_PRODUCT),
    ("18 ibs", D("18"), "pound", "weight", PER_PRODUCT),  # 'ibs' OCR typo for lbs
    ("1,234-lbs of product", D("1234"), "pound", "weight", PER_PRODUCT),
    ("zero pounds of product", D("0"), "pound", "weight", PER_PRODUCT),
]


@pytest.mark.parametrize("raw,value,unit,category,basis", _CASES)
def test_parse_quantity(
    raw: str, value: Decimal | None, unit: str | None, category: str | None, basis: str
) -> None:
    assert parse_quantity(raw) == ParsedQuantity(value, unit, category, basis)


@pytest.mark.parametrize("raw", [None, "", "   ", "\t\n"])
def test_parse_quantity_empty_is_unknown(raw: str | None) -> None:
    assert parse_quantity(raw) == ParsedQuantity(None, None, None, UNKNOWN)


def test_commas_and_decimals_strip_to_decimal() -> None:
    assert parse_quantity("1,159,754.46 total pounds").value == D("1159754.46")


def test_whitespace_runs_collapse() -> None:
    # double-spaced "#  units" template
    assert parse_quantity("182  units") == ParsedQuantity(D("182"), "each", "count", PER_PRODUCT)


def test_parse_never_raises_on_garbage() -> None:
    for junk in ("???", "lot #abc", "n/a - records lost", "...", "12/24/2020"):
        parse_quantity(junk)  # must not raise
