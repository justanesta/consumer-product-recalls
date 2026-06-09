"""Parse free-text recall-quantity strings into ``(value, unit, category, basis)``.

FDA ``product_distributed_quantity`` and USDA ``qty_recovered`` are firm-typed free text
("1,200 cases", "919,616.31 total pounds, for all products", "Unknown", "one unit"). This module
turns them into structured fields for ``recall_product`` + ``fct_units_recalled``, precision-over-
recall: a clean leading number + a recognized unit, plus a ``basis`` flag distinguishing a
per-product quantity from a recall-wide **total** — the same total repeats on every product row of a
recall, so ``fct_units_recalled`` must sum only ``per_product`` (take one-per-recall for totals).
Ambiguous / narrative values yield NULLs.

Pure + side-effect-free → unit-tested against the full-corpus grammar distribution
(``tests/enrichment/test_quantity.py``). Designed 2026-06-09 against the 123,548-row FDA +
4,011-row USDA corpus (plan C13; corpus dump in ``scripts/sql/cross_source/bronze/``). Two design
decisions the corpus forced: (1) the unit is the first *taxonomy noun* after skipping modifier words
(``# retail units`` / ``# total units`` are common — naive "first word" picks junk); (2) every
canonical unit carries a ``category`` (count / weight / volume / grouping) because
you cannot sum 1,000 units against 1,000 lbs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

PER_PRODUCT: Final = "per_product"
TOTAL_ALL_PRODUCTS: Final = "total_all_products"
UNKNOWN: Final = "unknown"


@dataclass(frozen=True, slots=True)
class ParsedQuantity:
    """Structured result of :func:`parse_quantity`. ``value``/``unit``/``category`` are NULL when
    un-extractable; ``basis`` is always one of per_product / total_all_products / unknown."""

    value: Decimal | None
    unit: str | None
    category: str | None
    basis: str


# --- unit taxonomy: synonym -> (canonical_unit, category) --------------------------------------
# Grounded in the observed FDA+USDA vocabulary (corpus profile Q2 + grammar templates). ``category``
# drives fct_units_recalled aggregation: count / weight / volume are incommensurable; ``grouping``
# is a count of lots/components/batches (not a retail item) and must not be summed with ``count``.
_UNITS: Final[dict[str, tuple[str, str]]] = {
    # count — individual items
    "unit": ("each", "count"),
    "units": ("each", "count"),
    "each": ("each", "count"),
    "eaches": ("each", "count"),
    "ea": ("each", "count"),
    "piece": ("piece", "count"),
    "pieces": ("piece", "count"),
    # count — packaging containers
    "case": ("case", "count"),
    "cases": ("case", "count"),
    "cs": ("case", "count"),
    "bottle": ("bottle", "count"),
    "bottles": ("bottle", "count"),
    "vial": ("vial", "count"),
    "vials": ("vial", "count"),
    "kit": ("kit", "count"),
    "kits": ("kit", "count"),
    "bag": ("bag", "count"),
    "bags": ("bag", "count"),
    "carton": ("carton", "count"),
    "cartons": ("carton", "count"),
    "box": ("box", "count"),
    "boxes": ("box", "count"),
    "can": ("can", "count"),
    "cans": ("can", "count"),
    "jar": ("jar", "count"),
    "jars": ("jar", "count"),
    "tube": ("tube", "count"),
    "tubes": ("tube", "count"),
    "tub": ("tub", "count"),
    "tubs": ("tub", "count"),
    "pack": ("pack", "count"),
    "packs": ("pack", "count"),
    "package": ("package", "count"),
    "packages": ("package", "count"),
    "packet": ("package", "count"),
    "packets": ("package", "count"),
    "pouch": ("pouch", "count"),
    "pouches": ("pouch", "count"),
    "tray": ("tray", "count"),
    "trays": ("tray", "count"),
    "cup": ("cup", "count"),
    "cups": ("cup", "count"),
    "container": ("container", "count"),
    "containers": ("container", "count"),
    "device": ("device", "count"),
    "devices": ("device", "count"),
    "tablet": ("tablet", "count"),
    "tablets": ("tablet", "count"),
    "capsule": ("capsule", "count"),
    "capsules": ("capsule", "count"),
    "caplet": ("capsule", "count"),
    "caplets": ("capsule", "count"),
    "syringe": ("syringe", "count"),
    "syringes": ("syringe", "count"),
    "system": ("system", "count"),
    "systems": ("system", "count"),
    "set": ("set", "count"),
    "sets": ("set", "count"),
    "pump": ("pump", "count"),
    "pumps": ("pump", "count"),
    "bar": ("bar", "count"),
    "bars": ("bar", "count"),
    "blister": ("blister", "count"),
    "blisters": ("blister", "count"),
    # weight
    "lb": ("pound", "weight"),
    "lbs": ("pound", "weight"),
    "pound": ("pound", "weight"),
    "pounds": ("pound", "weight"),
    "ibs": ("pound", "weight"),
    "g": ("gram", "weight"),
    "gram": ("gram", "weight"),
    "grams": ("gram", "weight"),
    "kg": ("kilogram", "weight"),
    "oz": ("ounce", "weight"),
    "ounce": ("ounce", "weight"),
    "ounces": ("ounce", "weight"),
    "ton": ("ton", "weight"),
    "tons": ("ton", "weight"),
    "cwt": ("hundredweight", "weight"),
    "cwts": ("hundredweight", "weight"),
    # volume
    "ml": ("milliliter", "volume"),
    "gallon": ("gallon", "volume"),
    "gallons": ("gallon", "volume"),
    "pint": ("pint", "volume"),
    "pints": ("pint", "volume"),
    # grouping — counts of lots/components/batches, NOT retail items
    "lot": ("lot", "grouping"),
    "lots": ("lot", "grouping"),
    "component": ("component", "grouping"),
    "components": ("component", "grouping"),
    "product": ("product", "grouping"),
    "products": ("product", "grouping"),
    "batch": ("batch", "grouping"),
    "batches": ("batch", "grouping"),
}

# Adjectives that sit between the number and the unit noun — skip them when scanning for the unit
# ("# total units", "# retail units", "# individual units", "# various bottles").
_MODIFIERS: Final[frozenset[str]] = frozenset(
    {
        "total",
        "retail",
        "individual",
        "various",
        "all",
        "reported",
        "approximate",
        "approximately",
        "approx",
        "of",
        "in",
        "the",
        "estimated",
        "est",
        "about",
    }
)

# spelled-out leading numbers ("one unit" is the 16th-most-common FDA grammar).
_SPELLED: Final[dict[str, int]] = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

# whole-value sentinels (the value is not a quantity) -> NULL value.
_SENTINEL_RE: Final[re.Pattern[str]] = re.compile(
    r"^(unknown|unk|none|n/?a|tbd|tba|undetermin\w*|unspecified|see\s+(line|above)|all|x{2,}|"
    r"not\s+(available|determined|provided|applicable))\b",
    re.IGNORECASE,
)

# "total / for all products / all varieties / combined" -> the number is a recall-wide total.
_TOTAL_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(total|combined|for\s+all|all\s+(products|varieties|items|models?|lots)|across\s+all)\b",
    re.IGNORECASE,
)

_APPROX_PREFIX_RE: Final[re.Pattern[str]] = re.compile(
    r"^(approximately|approx\.?|about|est\.?|estimated|~)\s*", re.IGNORECASE
)
# leading number with optional thousands commas / decimal. (#/# compounds -> the first number.)
_NUMBER_RE: Final[re.Pattern[str]] = re.compile(r"[0-9][0-9,]*(?:\.[0-9]+)?")
_WORD_RE: Final[re.Pattern[str]] = re.compile(r"[a-z]+")


def _scan_unit(tokens: list[str]) -> tuple[str | None, str | None]:
    """First taxonomy unit among ``tokens``, skipping known modifier words. A non-modifier,
    non-unit word means there is no unit (the unit, if present, sits right after the number)."""
    for tok in tokens:
        if tok in _MODIFIERS:
            continue
        hit = _UNITS.get(tok)
        return hit if hit is not None else (None, None)
    return (None, None)


def parse_quantity(raw: str | None) -> ParsedQuantity:
    """Parse a free-text recall-quantity string. Never raises; returns NULLs when unparseable."""
    if raw is None:
        return ParsedQuantity(None, None, None, UNKNOWN)
    text = " ".join(raw.split())  # collapse all whitespace runs to single spaces
    if not text:
        return ParsedQuantity(None, None, None, UNKNOWN)
    if _SENTINEL_RE.match(text):
        return ParsedQuantity(None, None, None, UNKNOWN)

    basis = TOTAL_ALL_PRODUCTS if _TOTAL_RE.search(text) else PER_PRODUCT
    body = _APPROX_PREFIX_RE.sub("", text)
    lowered = body.lower()

    first_word_match = _WORD_RE.match(lowered)
    if first_word_match is not None and first_word_match.group() in _SPELLED:
        value: Decimal | None = Decimal(_SPELLED[first_word_match.group()])
        unit, category = _scan_unit(_WORD_RE.findall(lowered[first_word_match.end() :]))
        return ParsedQuantity(value, unit, category, basis)

    number_match = _NUMBER_RE.search(body)
    if number_match is None:
        return ParsedQuantity(None, None, None, basis)
    try:
        value = Decimal(number_match.group().replace(",", ""))
    except InvalidOperation:  # pragma: no cover — _NUMBER_RE already guarantees a valid decimal
        value = None
    unit, category = _scan_unit(_WORD_RE.findall(lowered[number_match.end() :]))
    return ParsedQuantity(value, unit, category, basis)
