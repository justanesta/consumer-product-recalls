"""Place / common-compound tokens that must NOT, on their own, justify an entity rollup.

Phase 6b PR 6b.4, Tier 2. The Tier-2 rollup rule merges two firms when they share >=2
distinctive (non-generic, multi-character) tokens. The residual false-merge mode this leaves is
the *geographic 2-token coincidence* — unrelated firms that share only a place phrase:

    "San Antonio Bakery" + "San Antonio Eye Bank" + "San Antonio Packing"   (SAN ANTONIO)
    "Rocky Mountain ATV" + "Rocky Mountain Bicycles" + "Rocky Mountain Choc." (ROCKY MOUNTAIN)
    "Puget Sound Blood Center" + "Puget Sound Drug Corporation"              (PUGET SOUND)

The guard: a rollup is REFUSED when every shared distinctive token is in this set (so the merge
rests entirely on a place/compound phrase). It is SAFE by construction — a genuine brand that
shares a place word (``Great Lakes Cheese`` variants) also shares a real token (``CHEESE``) that
is NOT here, so the shared set is not a subset of this list and the merge still fires.

These words have mid-range document frequency (below ``GENERIC_DF_CUTOFF``), so the df-based
generic filter does not catch them — the signal that they are non-distinctive is *semantic*, not
frequency-based, which is exactly what a curated list supplies. The set is **proactive**: it is
seeded from a standard US-geography gazetteer plus the compounds observed in the corpus review,
so a brand-new ``San Antonio Whatever`` is caught on first ingest, before it is ever a hub.

Maintenance lives in documentation/operations.md ("Firm resolution" review loop): a new
place/compound hub found at review time is added here, version-controlled and auditable. The set
is finite and slow-growing (US place vocabulary), so it does not balloon. It can be regenerated /
extended from the US Census place-name file if broader coverage is wanted.
"""

from __future__ import annotations

# Directionals + relative-position modifiers.
_DIRECTIONAL = {
    "NORTH",
    "SOUTH",
    "EAST",
    "WEST",
    "NORTHERN",
    "SOUTHERN",
    "EASTERN",
    "WESTERN",
    "NORTHEAST",
    "NORTHWEST",
    "SOUTHEAST",
    "SOUTHWEST",
    "CENTRAL",
    "UPPER",
    "LOWER",
    "GREATER",
    "MID",
    "TRI",
}

# Generic geographic features (the "X <feature>" naming pattern).
_FEATURES = {
    "MOUNTAIN",
    "MOUNTAINS",
    "LAKE",
    "LAKES",
    "VALLEY",
    "BAY",
    "RIVER",
    "ISLAND",
    "ISLANDS",
    "BEACH",
    "SPRING",
    "SPRINGS",
    "HILL",
    "HILLS",
    "HARBOR",
    "HARBOUR",
    "PORT",
    "FALLS",
    "CREEK",
    "FOREST",
    "PARK",
    "HEIGHTS",
    "GROVE",
    "RIDGE",
    "MESA",
    "MEADOW",
    "MEADOWS",
    "PRAIRIE",
    "CANYON",
    "DESERT",
    "COAST",
    "COASTAL",
    "GULF",
    "OCEAN",
    "SOUND",
    "POINT",
    "CAPE",
    "PLAINS",
    "SUMMIT",
    "SLOPE",
    "WOODS",
    "FIELD",
    "FIELDS",
    "GARDEN",
    "GARDENS",
    "CITY",
    "TOWN",
    "VILLAGE",
    "COUNTY",
    "LAND",
}

# Region words + Spanish place particles that anchor multi-word US place names.
_REGION = {
    "PACIFIC",
    "ATLANTIC",
    "ROCKY",
    "GREAT",
    "GOLDEN",
    "TROPICAL",
    "SUNNY",
    "SUN",
    "BLUE",
    "GREEN",
    "SILVER",
    "GRAND",
    "ROYAL",
    "EMPIRE",
    "LIBERTY",
    "HERITAGE",
    "PIONEER",
    "FRONTIER",
    "SAN",
    "SANTA",
    "LAS",
    "LOS",
    "EL",
    "DEL",
    "FE",
    "PUGET",
    "NEW",
    "OLD",
}

# Common non-geographic compounds seen to anchor false 2-token merges in the corpus.
_COMPOUND = {
    "FOUR",
    "FIVE",
    "STAR",
    "STARS",
    "SEASONS",
    "SEASON",
    "FIRST",
    "PREMIER",
    "PREMIUM",
    "QUALITY",
    "SUPERIOR",
    "ADVANCED",
    "ALLIED",
    "MODERN",
    "CLASSIC",
    "ELITE",
    "PRIDE",
    "VALUE",
    "CHAMPION",
    "DELUXE",
    "SELECT",
    "CHOICE",
    "DIRECT",
    "EXPRESS",
    "PRO",
    "MAX",
}

# US state + territory names (full forms; abbreviations are 2-char and handled separately).
_STATES = {
    "ALABAMA",
    "ALASKA",
    "ARIZONA",
    "ARKANSAS",
    "CALIFORNIA",
    "COLORADO",
    "CONNECTICUT",
    "DELAWARE",
    "FLORIDA",
    "GEORGIA",
    "HAWAII",
    "IDAHO",
    "ILLINOIS",
    "INDIANA",
    "IOWA",
    "KANSAS",
    "KENTUCKY",
    "LOUISIANA",
    "MAINE",
    "MARYLAND",
    "MASSACHUSETTS",
    "MICHIGAN",
    "MINNESOTA",
    "MISSISSIPPI",
    "MISSOURI",
    "MONTANA",
    "NEBRASKA",
    "NEVADA",
    "HAMPSHIRE",
    "JERSEY",
    "MEXICO",
    "YORK",
    "CAROLINA",
    "DAKOTA",
    "OHIO",
    "OKLAHOMA",
    "OREGON",
    "PENNSYLVANIA",
    "TENNESSEE",
    "TEXAS",
    "UTAH",
    "VERMONT",
    "VIRGINIA",
    "WASHINGTON",
    "WISCONSIN",
    "WYOMING",
    "AMERICA",
    "ANTONIO",  # San Antonio
    "DIEGO",  # San Diego
    "FRANCISCO",  # San Francisco
    "JOSE",  # San Jose
    "CLARA",  # Santa Clara
    "VEGAS",  # Las Vegas
    "ANGELES",  # Los Angeles
}

PLACE_WORDS: frozenset[str] = frozenset(_DIRECTIONAL | _FEATURES | _REGION | _COMPOUND | _STATES)
