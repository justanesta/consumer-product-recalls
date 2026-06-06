"""Cross-source firm-name normalization (Phase 6b, PRs 6b.1 + 6b.4).

Deterministic cleaning of firm-role names from all five sources before they enter the
silver firm dimension and the 6b.4 RapidFuzz clustering. The geo/DBA strip was validated
against the full CPSC corpus (gate ``scripts/sql/cpsc/bronze/measure_comma_optional_of_strip.sql``
+ the ``data/exploratory/cpsc/g1_comma_less_cohort.csv`` dump, 2026-06-03); the
cross-source blast-radius review (``probe_cleaning_blast_radius_by_source.sql``, 2026-06-04)
established that a blanket parenthetical strip is too blunt cross-source, so paren-VARIANTS
are left to RapidFuzz (ADR 0037) and only paren-BRANDS are captured as aliases.

Three pure operations:

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
  "doing business as", incl. the parenthetical and marker-alone "(DBA) Brand" forms) as
  an alternate name.
- ``extract_paren_aliases`` — capture a non-DBA parenthetical that holds a brand / alternate
  company name ("Deere & Company (John Deere)") as an alternate name, dropping noise parens.

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
# Marker-alone-in-parens form with the brand OUTSIDE the parens:
# "Annona Company, LLC (DBA) Honest Foods" -> clean "Annona Company, LLC", dba "Honest Foods".
_DBA_PAREN_MARKER_CAPTURE = re.compile(r"\(\s*" + _DBA_MARK + r"\s*\)\s*(.+)$", re.IGNORECASE)
_DBA_PAREN_MARKER_STRIP = re.compile(r"\s*\(\s*" + _DBA_MARK + r"\s*\)\s*.*$", re.IGNORECASE)

# ── Parenthetical-alias capture (PR 6b.4) ───────────────────────────────────────
# The deterministic cleaner deliberately does NOT strip parentheticals: the cross-source
# blast-radius review (2026-06-04) showed a blanket strip is too blunt — abbreviation-prefix
# over-truncation ("FENGM (Hong Kong Fengmang International Co. Ltd.)" -> "FENGM"), brand
# loss, and (DBA) mashups. RapidFuzz handles paren-variants instead (ADR 0037). But a paren
# that holds a BRAND / alternate company name ("Deere & Company (John Deere)") is worth
# keeping as a search + fuzzy-match alias -> firm.alternate_names. extract_paren_aliases
# keeps those and drops the noise (status / succession / facility / location / date) parens.
_DBA_CONTENT = re.compile(r"^\s*" + _DBA_MARK, re.IGNORECASE)
_CORP_FORM = re.compile(
    r"\b(?:inc|incorporated|llc|l\.l\.c|corp|corporation|co|company|ltd|limited"
    r"|gmbh|a\.?g|s\.?a|pvt|plc|lp|l\.p|kg|bhd|sdn)\b",
    re.IGNORECASE,
)
# Parenthetical content that is never an alternate name (status / succession / corporate
# narrative / facility-or-org-unit tag / a year or date). Locations are handled separately.
_ALIAS_NOISE = re.compile(
    r"\b(?:formerly|f\.?/?k\.?/?a|n\.?/?k\.?/?a|now\s+known|previously|no\s+longer"
    r"|out\s+of\s+business|ceased|owner\s+of|under\s+license|licensee|subsidiary"
    r"|division\s+of|a\s+division|wholly.owned|in\s+turn|trademark|brand\s+name"
    r"|also\s+known\s+as|also\s+does\s+business|conducting\s+this\s+recall"
    r"|located\s+in|department\s+of|authorized\b|note\b|plant\b|unit\b|site\b|facility"
    r"|\bmfg\b|corporate|headquarters|\bhq\b|production|department|branch|region\b"
    r"|warehouse|distribution|service\s+center|blood\s+bank|commissary|kitchen|office\b"
    r"|present\b|prior\s+to|\bafter\b|\bbefore\b|\bthrough\b|\bsince\b)\b"
    r"|\b(?:19|20)\d{2}\b|\d{1,2}/\d",
    re.IGNORECASE,
)
# Regional parentheticals not covered by the single-token geo vocabulary (drop as alias).
_ALIAS_REGION = re.compile(
    r"^(?:u\.?s\.?a?\.?|uk|u\.?k\.?|north\s+america|n\.?\s*america|america"
    r"|united\s+states|hong\s+kong)$",
    re.IGNORECASE,
)


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


def clean_firm_name(raw: str, *, geo_mode: str = "full") -> str:
    """Return the canonical legal name: DBA clause removed + (source-gated) trailing geo suffix.

    ``geo_mode`` gates the geographic-suffix strip per the ADR 0037 amendment (tie geo-strip
    intensity to whether the source has a better identity signal than its name):

    - ``'full'``    — strip the geo suffix (CPSC: the name is the only identity signal, and
      the "<Firm> of <City>, <State>"/"of <Country>" tail is reliably a location decoration).
    - ``'guarded'`` — strip, but NEVER reduce the name to a bare single token (the NHTSA
      integral-name guard: "WINNEBAGO OF INDIANA" -> "WINNEBAGO" would collide with the
      distinct "WINNEBAGO INDUSTRIES INC."). A multi-token base still strips (the dealer
      cohort "AUTO TRIM DESIGN OF TEXAS" -> "AUTO TRIM DESIGN").
    - ``'off'``     — no geo strip (FDA/USDA/USCG: FEI / establishment_number / MIC carry the
      authoritative within-source identity, and the geo-strip over-strips integral
      "X of <State>" establishment names, e.g. "BLOODCENTER OF WISCONSIN").

    The DBA-clause strip is source-agnostic (always applied — an explicit labeled marker).
    Precision-first — integral/narrative/place-internal "of" and non-geographic tails are
    left whole (see module docstring). Idempotent; preserves case.
    """
    if not raw:
        return ""
    name = _WS.sub(" ", raw).strip()
    # 1. Remove DBA clauses (always): marker-then-brand "(dba) Brand..." (brand outside the
    #    parens), parenthetical "(dba Brand)", then inline-to-end "dba Brand ...".
    name = _DBA_PAREN_MARKER_STRIP.sub("", name)
    name = _DBA_PAREN.sub("", name)
    name = _DBA_INLINE_STRIP.sub("", name)
    name = _WS.sub(" ", name).strip().rstrip(",; ")
    # 2. Geographic suffix strip — GATED by geo_mode + the integral/narrative blocklist.
    if geo_mode != "off" and not _BLOCKLIST.search(name):
        stripped = _strip_trailing_geo(name)
        if geo_mode == "guarded" and stripped != name and len(stripped.split()) <= 1:
            stripped = name  # integral-name guard: never strip down to a bare single token
        name = stripped
    return _WS.sub(" ", name).strip().rstrip(",; ")


def extract_firm_dba(raw: str) -> str | None:
    """Return the DBA brand (parenthetical or inline form), or None if absent.

    The inline brand stops before a trailing ', of <geo>' clause
    ("dba BabyLegs of Seattle, Wash." -> "BabyLegs").
    """
    if not raw:
        return None
    name = _WS.sub(" ", raw).strip()
    # Marker-alone-in-parens, brand outside: "...LLC (DBA) Honest Foods" -> "Honest Foods".
    marker = _DBA_PAREN_MARKER_CAPTURE.search(name)
    if marker:
        brand = _OF.split(marker.group(1), maxsplit=1)[0].strip().rstrip(",; ")
        return brand or None
    paren = _DBA_PAREN.search(name)
    if paren and paren.group(1).strip():
        return paren.group(1).strip().rstrip(",; ") or None
    inline = _DBA_INLINE_CAPTURE.search(name)
    if inline:
        brand = _OF.split(inline.group(1), maxsplit=1)[0]
        brand = brand.strip().rstrip(",; ")
        return brand or None
    return None


def extract_paren_aliases(raw: str) -> list[str]:
    """Return parenthetical contents that are alternate NAMES, for firm.alternate_names.

    Keeps a paren that "looks like it names the firm" — multiword, carries a corporate
    form, or shares a >=4-char token with the surrounding name ("National Presto Industries
    Inc. (Presto)" -> ["Presto"]; "Deere & Company (John Deere)" -> ["John Deere"]). Drops
    the noise: DBA markers (``extract_firm_dba`` owns those), status / succession / facility
    / date parens, and pure locations.

    Precision-first on the NOISE side (a wrong alias is worse than a missing one in a search
    field), so two documented gaps are accepted: a truly unrelated single-word brand with no
    shared token ("(Texsport)") is skipped, and a bare multiword facility city
    ("(Hot Springs)") can leak. Order-preserving, de-duplicated, case-preserving.
    """
    if not raw or "(" not in raw:
        return []
    base_tokens = set(re.findall(r"[A-Za-z0-9]{4,}", re.sub(r"\([^)]*\)", " ", raw).upper()))
    aliases: list[str] = []
    seen: set[str] = set()
    for content in re.findall(r"\(([^)]*)\)", raw):
        alias = _WS.sub(" ", content).strip().rstrip(",;. ")
        if len(alias) < 2 or _DBA_CONTENT.match(alias) or _ALIAS_NOISE.search(alias):
            continue
        if _norm(alias) in _GEO_TERMS or _ALIAS_REGION.fullmatch(alias):
            continue
        if _norm(alias.rsplit(",", 1)[-1]) in _GEO_TERMS:  # trailing "City, State" location
            continue
        tokens = re.findall(r"[A-Za-z0-9]{4,}", alias.upper())
        looks_like_name = (
            bool(re.search(r"[ ,/&]", alias))
            or bool(_CORP_FORM.search(alias))
            or any(t in base_tokens for t in tokens)
        )
        if not looks_like_name:
            continue
        key = alias.upper()
        if key not in seen:
            seen.add(key)
            aliases.append(alias)
    return aliases
