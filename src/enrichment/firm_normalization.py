"""CPSC firm-name normalization (Phase 6b, PR 6b.1).

Deterministic cleaning of CPSC firm-role names (manufacturers / importers /
distributors) before they enter the silver firm dimension and the 6b.4 RapidFuzz
clustering. Validated against the full 9,828-record corpus (gate
``scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql`` + the
``data/exploratory/cpsc/g1_comma_less_cohort.csv`` dump, 2026-06-03).

Two pure operations:

- ``clean_firm_name`` — strip a trailing DBA clause and a trailing geographic
  suffix ("Fisher-Price of East Aurora, N.Y." -> "Fisher-Price"), returning the
  canonical legal name. **Precision-first**: when the "of" is integral
  ("Bank of America"), a place-name-internal "of" ("City of Industry", "King of
  Prussia"), corporate narrative ("a division of Eaton", "on behalf of"), or the
  trailing clause is not clearly geographic, the name is left WHOLE. Rationale: a
  missed strip merely fragments (RapidFuzz recovers it via subset
  ``token_set_ratio``), whereas an over-strip corrupts firm identity irreversibly.
  The strip anchors to the LAST "of" whose tail contains a geographic token, which
  prevents the greedy-leftmost bug ("Fireworks of Alabama, Inc. of Adamsville,
  Ala." correctly -> "Fireworks of Alabama, Inc.", not "Fireworks").
- ``extract_firm_dba`` — capture the DBA brand ("dba" / "d/b/a" / "d.b.a." /
  "doing business as", incl. the parenthetical form) as an alternate name.

No I/O. The caller (a ``recalls`` CLI step, PR 6b.1) maps distinct raw names through
these and persists the result so silver ``firm.sql`` and ``recall_event_firm.sql``
consume an identical cleaned name (lockstep). "America" / "USA" are deliberately NOT
geographic strip targets — "X of America" is overwhelmingly a subsidiary/brand
marker (Nintendo of America, Pines of America), not a location, in this corpus.
"""

from __future__ import annotations

import re


def _norm(token: str) -> str:
    """Lowercase + drop all non-alphanumerics, so 'N.Y.' / 'N. Y.' -> 'ny'."""
    return re.sub(r"[^a-z0-9]", "", token.lower())


# Geographic vocabulary for the trailing-geo strip: US states (full + AP-style
# abbreviation + 2-letter postal), Canadian provinces, and the countries/locales the
# corpus actually carries. Normalized via _norm. "America"/"USA"/"United States" are
# EXCLUDED on purpose (see module docstring).
_GEO_TERMS: frozenset[str] = frozenset(
    _norm(t)
    for t in (
        # --- US states, full ---
        "Alabama",
        "Alaska",
        "Arizona",
        "Arkansas",
        "California",
        "Colorado",
        "Connecticut",
        "Delaware",
        "Florida",
        "Georgia",
        "Hawaii",
        "Idaho",
        "Illinois",
        "Indiana",
        "Iowa",
        "Kansas",
        "Kentucky",
        "Louisiana",
        "Maine",
        "Maryland",
        "Massachusetts",
        "Michigan",
        "Minnesota",
        "Mississippi",
        "Missouri",
        "Montana",
        "Nebraska",
        "Nevada",
        "New Hampshire",
        "New Jersey",
        "New Mexico",
        "New York",
        "North Carolina",
        "North Dakota",
        "Ohio",
        "Oklahoma",
        "Oregon",
        "Pennsylvania",
        "Rhode Island",
        "South Carolina",
        "South Dakota",
        "Tennessee",
        "Texas",
        "Utah",
        "Vermont",
        "Virginia",
        "Washington",
        "West Virginia",
        "Wisconsin",
        "Wyoming",
        "District of Columbia",
        "Puerto Rico",
        # --- US states, AP-style abbreviations (periods stripped by _norm) ---
        "Ala",
        "Ariz",
        "Ark",
        "Calif",
        "Cal",
        "Colo",
        "Conn",
        "Del",
        "Fla",
        "Ga",
        "Ill",
        "Ind",
        "Kan",
        "Kans",
        "Ky",
        "La",
        "Md",
        "Mass",
        "Mich",
        "Minn",
        "Miss",
        "Mo",
        "Mont",
        "Neb",
        "Nebr",
        "Nev",
        "Okla",
        "Ore",
        "Oreg",
        "Pa",
        "Penn",
        "Tenn",
        "Tex",
        "Vt",
        "Va",
        "Wash",
        "WVa",
        "Wis",
        "Wisc",
        "Wyo",
        # --- US states, 2-letter postal ---
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "WA",
        "WV",
        "WI",
        "WY",
        "DC",
        # --- Canadian provinces ---
        "Ontario",
        "Quebec",
        "Alberta",
        "Manitoba",
        "Saskatchewan",
        "BC",
        "British Columbia",
        "Nova Scotia",
        "New Brunswick",
        # --- Countries / foreign locales seen in the corpus ---
        "China",
        "Taiwan",
        "Canada",
        "Mexico",
        "Spain",
        "Sweden",
        "Switzerland",
        "Italy",
        "Germany",
        "France",
        "UK",
        "England",
        "Britain",
        "Hong Kong",
        "Japan",
        "Korea",
        "South Korea",
        "India",
        "Vietnam",
        "Thailand",
        "Costa Rica",
        "Brazil",
        "Netherlands",
        "Denmark",
        "Norway",
        "Finland",
        "Israel",
        "Turkey",
        "Poland",
        "Austria",
        "Belgium",
        "Ireland",
        "Portugal",
        "Greece",
        "Australia",
        "New Zealand",
        "Singapore",
        "Malaysia",
        "Indonesia",
        "Philippines",
        "Chile",
        "Argentina",
        "Colombia",
        "Guangdong",
        "Fujian",
        "Zhejiang",
        "Jiangsu",
        "Shenzhen",
        "Zhuhai",
        "Tianjin",
    )
)

# Integral / narrative "of" — NOT a strippable geo suffix; the name is left whole.
# Head words = place names with internal "of" (City/King of ...) + brand heads
# (World/House/Club/Month/Empire of ...); the ", a/an <role> of" + on-behalf/owner/
# affiliate/business/formerly/brand/maker patterns = corporate narrative;
# "out of business" = status text. Validated on the corpus.
_BLOCKLIST = re.compile(
    r"\b(?:city|king|isle|cape|gulf|division|subsidiary|club|world|month|empire"
    r"|centre|center|board|department|university|college|institute|bank|house"
    r"|taste|scouts|county|district)\s+of\b"
    r"|,\s*an?\s+[a-z][a-z ]{0,25}\s+of\s"
    r"|\b(?:on\s+behalf|formerly|business|owner|affiliate|brand\s+name|brand|maker)\s+of\b"
    r"|\bout\s+of\s+business\b",
    re.IGNORECASE,
)

# A standalone " of " / ", of " token (word-bounded; glued "of" inside a word is NOT
# matched — protects "PROFOF").
_OF = re.compile(r"(?:,?\s+)\bof\b\s+", re.IGNORECASE)
_WS = re.compile(r"\s+")

# DBA markers: dba / d/b/a / d.b.a. / d/b/a/ / "doing business as".
_DBA_MARK = r"(?:doing\s+business\s+as|\bd[./]?b[./]?a[./]?)"
_DBA_PAREN = re.compile(r"\s*\(\s*" + _DBA_MARK + r"\s*([^)]*?)\s*\)", re.IGNORECASE)
_DBA_INLINE_CAPTURE = re.compile(r",?\s*" + _DBA_MARK + r"\s+(.+)$", re.IGNORECASE)
_DBA_INLINE_STRIP = re.compile(r",?\s*" + _DBA_MARK + r"\s+.*$", re.IGNORECASE)


def _tail_has_geo(tail: str) -> bool:
    """True if the trailing clause (after an 'of') carries a geographic token.

    Checks individual tokens AND whole comma-segments (so 'New York' / 'Costa Rica'
    match), tolerating a trailing parenthetical/punctuation ('of China (batteries)').
    """
    if any(_norm(tok) in _GEO_TERMS for tok in re.split(r"[\s,]+", tail) if tok):
        return True
    return any(_norm(seg) in _GEO_TERMS for seg in tail.split(","))


def _strip_trailing_geo(name: str) -> str:
    """Remove the geo suffix anchored to the LAST 'of' whose tail contains a geo token."""
    last: re.Match[str] | None = None
    for match in _OF.finditer(name):
        if _tail_has_geo(name[match.end() :]):
            last = match
    if last is None:
        return name
    return name[: last.start()].rstrip(",; ")


def clean_firm_name(raw: str) -> str:
    """Return the canonical legal name: DBA clause + trailing geographic suffix removed.

    Precision-first — integral/narrative/place-internal "of" and non-geographic
    tails are left whole (see module docstring). Idempotent; preserves case.
    """
    if not raw:
        return ""
    name = _WS.sub(" ", raw).strip()
    # 1. Remove DBA clauses (parenthetical, then inline-to-end).
    name = _DBA_PAREN.sub("", name)
    name = _DBA_INLINE_STRIP.sub("", name)
    name = _WS.sub(" ", name).strip().rstrip(",; ")
    # 2. Geographic suffix strip — skipped when the "of" is integral/narrative.
    if not _BLOCKLIST.search(name):
        name = _strip_trailing_geo(name)
    return _WS.sub(" ", name).strip().rstrip(",; ")


def extract_firm_dba(raw: str) -> str | None:
    """Return the DBA brand (parenthetical or inline form), or None if absent.

    The inline brand stops before a trailing ', of <geo>' clause
    ("dba BabyLegs of Seattle, Wash." -> "BabyLegs").
    """
    if not raw:
        return None
    name = _WS.sub(" ", raw).strip()
    paren = _DBA_PAREN.search(name)
    if paren and paren.group(1).strip():
        return paren.group(1).strip().rstrip(",; ") or None
    inline = _DBA_INLINE_CAPTURE.search(name)
    if inline:
        brand = _OF.split(inline.group(1), maxsplit=1)[0]
        brand = brand.strip().rstrip(",; ")
        return brand or None
    return None
