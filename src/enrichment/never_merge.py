"""Curated do-not-merge overrides — the manual lever for the residual Tier-2 false-merge mode the
denylists cannot reach (Phase 6b.6 precision review loop, 2026-06-05).

The full-corpus rollup review (``recalls audit-firm-rollups``) leaves one false-merge class that
NEITHER ``place_words`` NOR ``GENERIC_WORDS`` can refuse: the **two-real-token coincidence** — two
distinct firms that share >=2 *genuinely distinctive* tokens by chance, so neither token is a
denylistable weak word:

    "Eagle Family Discount Stores"  vs  "Eagle Family Foods Group"   (EAGLE + FAMILY)
    "General Parts, Inc."           vs  "General Trailer Parts LLC"  (GENERAL + PARTS)
    "Best Buy Bones Inc." (pet treats)  vs  the Best Buy retailer  (BEST + BUY)

This file is the explicit override. ``cluster_names`` consults ``NEVER_MERGE`` and refuses any
forbidden pair (``_classify`` never even sees it), so the two firms stay split on the next resolve.

TWO ways to write a rule — both expand to forbidden PAIRS (the resolver only knows pairs):

  ``_PAIRS``       — for a simple two-firm coincidence: ``(clean_a, clean_b)``.
  ``_APART_FROM``  — for "one odd firm inside an otherwise-legit cluster": ``(odd, [family…])``.
     Union-find connects a cluster through ANY edge, so isolating one member needs it forbidden
     against EVERY family member — listing them all here does that. The family keeps rolling up
     among itself; only the odd firm is pulled out.

Maintenance (operations.md "Firm resolution review loop"): ``audit-firm-rollups`` surfaces a
candidate ranked by suspicion; a reviewer copies the CLEAN names VERBATIM from the report's
``members`` column (the strings the resolver actually clusters on — case doesn't matter, but the
cleaning/punctuation must match) into ``_PAIRS`` or ``_APART_FROM``. Version-controlled, auditable,
reversible — same discipline as ``place_words``. Direct-pair only: a 3-firm weld bridged through a
middle name needs each bridging pair, which is exactly what ``_APART_FROM`` enumerates.
"""

from __future__ import annotations

# (clean_a, clean_b) — two confirmed-distinct firms sharing >=2 real tokens. Pairs the place /
# GENERIC denylists already refuse (Hudson River, Great American Marketing, …) are NOT here.
_PAIRS: tuple[tuple[str, str], ...] = (
    ("Eagle Family Discount Stores", "EAGLE FAMILY FOODS GROUP LLC"),
    ("GENERAL PARTS, INC.", "GENERAL TRAILER PARTS LLC"),
    ("Direct Source International", "DIRECT SOURCE SEAFOOD LLC"),
    ("All Strong Industry (USA) Inc.", "All-Power America, of City of Industry, Calif."),
    (
        "Water Heating Division of Rheem Sales Company Inc., of Montgomery, Ala.",
        "Water Heating Technologies Corp.",
    ),
    # SS-tail review 2026-06-05 (firm_rollup_review.csv rows 120-702)
    ("Fisher & Paykel Appliances (Thailand) Co., Ltd", "Fisher & Paykel Healthcare, Ltd."),
    ("SUPERIOR COACH INTL., LTD", "SUPERIOR INDUSTRIES INTL"),
    ("SPECIALTY WINDOW COVERING", "Window Covering Safety Council"),
    (
        "Jiujiang Pufei E-commerce Co., of China, through JD E Commerce America Limited",
        "Jiujiang Xunyi E-Commerce Co., (formerly Shenzhen Jiaqin Supply Chain Technology Co., Ltd.) of China, through JD E Commerce America Limited",  # noqa: E501
    ),
    ("GENERAL TIRE & RUBBER CO.", "General Rubber (Thailand) Co., Ltd"),
    ("Seven Seas International USA LLC", "Seven Seas Seafoods Inc"),
    ("Black Sheep Bakery", "Black Sheep Egg Company, LLC"),
    ("Off Grid Trailers", "Off-Grid Solutions USA LLC"),
    ("Bio-Save Resources Of Albuquerque LLC", "International Bio Resources, Llc"),
)

# (odd_one_out, [family members it must stay apart from]) — isolate ONE distinct firm from an
# otherwise-legit cluster it coincidentally shares tokens with. Copy the family members verbatim
# from the review report's `members` column. Expands to (odd, member) pairs for every member.
_APART_FROM: tuple[tuple[str, list[str]], ...] = (
    (
        "Best Buy Bones Inc.",  # pet treats — NOT the Best Buy retailer family below
        [
            "Best Buy",
            "Best Buy Co. Inc.",
            "Best Buy Purchasing LLC",
            "Best Buy Purchasing, LLC, a subsidiary of Best Buy Co., Inc. imported the Insignia fire tables",  # noqa: E501
        ],
    ),
    (
        "Natural Solutions for Life, Inc.",  # NOT "Food For Life Baking" (coincidental FOR + LIFE)
        [
            "Food For Life Baking Co Inc",
            "Food For Life Baking Company Inc.",
        ],
    ),
    (
        "Middle East Treasures Imports",
        [
            "Middle East/ Soli's Baking Company, Inc.",
            "Middle East Bakery, Inc.",
        ],
    ),
    # SS-tail review 2026-06-05 (firm_rollup_review.csv rows 120-702)
    ("Rivers Edge Pharmaceuticals", ["Rivers Edge Tree Stands Inc.", "Rivers Edge/Ardisam, Inc."]),
    ("Premier Pharmacy Labs Inc", ["Premier Research Labs LLC", "Premier Research Labs LP"]),
    (
        "Blue Ribbon Products, Inc",
        ["American Blue Ribbon Holdings", "American Blue Ribbon Holdings LLC"],
    ),
    ("21st Century Healthcare, Inc.", ["21ST CENTURY SNACK FOODS", "21st Century Foods, Inc."]),
    (
        "First Medical Source LLC",
        [
            "First Source",
            "First Source Inc",
            "First Source Llc",
            "First Source Llc Buffalo",
            "First Source, LLC",
        ],
    ),
    ("Red Bridge Baking Company LLC", ["Bridge City Baking", "Bridge City Baking LLC"]),
    # SS230 - keep the 3 "Hi-Tech Pharmaceuticals Inc" rows together; split Pharmacal + E.V.S.
    (
        "Hi-Tech Pharmacal Co., Inc.",
        [
            "Hi Tech Pharmaceuticals",
            "Hi-Tech Pharmaceuticals Inc",
            "Hi-Tech Pharmaceuticals Inc.",
            "Hi-Tech E.V.S.",
        ],
    ),
    (
        "Hi-Tech E.V.S.",
        ["Hi Tech Pharmaceuticals", "Hi-Tech Pharmaceuticals Inc", "Hi-Tech Pharmaceuticals Inc."],
    ),
)


def _pair(a: str, b: str) -> frozenset[str]:
    return frozenset({a.upper().strip(), b.upper().strip()})


# Order-independent, case-insensitive pair set the resolver consults (both forms expanded to pairs).
NEVER_MERGE: frozenset[frozenset[str]] = frozenset(
    [_pair(a, b) for a, b in _PAIRS]
    + [_pair(odd, member) for odd, family in _APART_FROM for member in family]
)
