"""Curated do-not-merge pairs — the manual override for the residual Tier-2 false-merge mode
the denylists cannot reach (Phase 6b.6 precision review loop, 2026-06-05).

The full-corpus rollup review (``recalls audit-firm-rollups``) leaves one false-merge class that
NEITHER ``place_words`` NOR ``GENERIC_WORDS`` can refuse: the **two-real-token coincidence** —
two distinct firms that share >=2 *genuinely distinctive* tokens by chance, so neither token is a
denylistable weak word:

    "Eagle Family Discount Stores"  vs  "Eagle Family Foods Group"   (EAGLE + FAMILY)
    "General Parts, Inc."           vs  "General Trailer Parts LLC"  (GENERAL + PARTS)
    "Direct Source International"    vs  "Direct Source Seafood"      (DIRECT + SOURCE)

A denylist can't touch these without nuking a real brand token. This file is the explicit override:
clean-name PAIRS the resolver must NEVER union — the symmetric counterpart to the FEI ``must_link``.
``cluster_names`` refuses the direct pair (``_classify`` returns ``None``), so the two firms stay
split on the next resolve. Direct-pair only: a 3-firm weld bridged through a middle name needs each
bridging pair listed (the observed false-merges are direct 2-name welds).

Maintenance (the operations.md "Firm resolution review loop"): ``audit-firm-rollups`` surfaces
candidates ranked by suspicion; a reviewer confirms a false merge and copies the two CLEAN names
verbatim from the report's ``members`` column into ``_PAIRS`` below. Version-controlled, auditable,
and reversible — same discipline as ``place_words``. Pairs are matched case-insensitively on the
clean name (``upper().strip()``), so the exact display case does not matter; the cleaning (geo/DBA
strip) must match, which is why the report's clean strings are the authoritative source.
"""

from __future__ import annotations

# (clean_name_a, clean_name_b) — confirmed-distinct firms sharing >=2 real tokens. Seeded from the
# 6b.6 cross-source review; extend from the audit report. Pairs the place/GENERIC denylists already
# refuse (Hudson River, Third Coast, Great American Marketing, Creative …Concepts, etc.) are NOT
# here — the denylist handles those; this is only the irreducible two-real-token residual.
_PAIRS: tuple[tuple[str, str], ...] = (
    ("Eagle Family Discount Stores", "EAGLE FAMILY FOODS GROUP LLC"),
    ("GENERAL PARTS, INC.", "GENERAL TRAILER PARTS LLC"),
    ("Direct Source International", "DIRECT SOURCE SEAFOOD LLC"),
)

# Order-independent, case-insensitive pair set the resolver consults.
NEVER_MERGE: frozenset[frozenset[str]] = frozenset(
    frozenset({a.upper().strip(), b.upper().strip()}) for a, b in _PAIRS
)
