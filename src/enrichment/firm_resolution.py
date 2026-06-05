"""Name-grain firm resolution (Phase 6b PR 6b.4) — the name-similarity layer (FEI tier deferred).

The deterministic floor (``firm_normalization`` + ``crosswalk_writer.build_crosswalk_rows``)
collapses raw variants that CLEAN to the same string (case / geo-suffix / DBA). This module
overlays the residual it cannot do. The SHIPPED grain is **name/brand** (Tier 1 + 2), uniform with
the other four sources whose structured ids are attributes, not merge keys (ADR 0037). All pure
(no I/O): ``crosswalk_writer.apply_clustering`` supplies the distinct clean names, gets back one
``ClusterAssignment`` per name, and repoints ``canonical_firm_id``.

  Tier 0  FEI (``fei_exact``)        — DEFERRED / OPT-IN (``fei_merge``, default off): group FDA
                                       names by current establishment id. An FEI is a FACILITY id,
                                       not a firm id, and facilities change corporate hands, so
                                       FEI-merging chains UNRELATED firms across owner changes (the
                                       plasma/blood blobs — ADR 0037). FEI rides on
                                       ``firm.observed_company_ids`` as an attribute instead.
  Tier 1  name repair                — within a distinctive-token block, merge names that are
              ``name_variant_exact``   essentially the SAME string: identical distinctive-token
              ``name_typo_high``       set (punctuation / case / spacing / corp-form), or a high
                                       ``token_sort_ratio`` (a spelling typo). Low risk.
  Tier 2  entity rollup              — OPTIONAL (``rollup=True``). Merge names sharing >=2
              ``rapidfuzz_rollup``     distinctive multi-character tokens above ``token_set_ratio``
                                       threshold, UNLESS every shared token is a place / common
                                       compound (``place_words.PLACE_WORDS``) — the residual
                                       false-merge mode (``San Antonio Bakery`` + ``…Eye Bank``).

WHY blocking. ``token_set_ratio`` returns 100 for any token subset, so comparing all ~28k names
pairwise would both be slow (~400M pairs) and chain unrelated firms. Names are partitioned into
blocks by their first DISTINCTIVE token (articles / corp-forms / >``GENERIC_DF_CUTOFF`` boilerplate
skipped) and only compared within a block. 1-character tokens are KEPT in the block key and the
identical-set test (so ``A.O. Smith`` and ``C.E. Smith`` block apart and never merge) but dropped
from the Tier-2 >=2-token count (so shared initials can't anchor a rollup).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rapidfuzz.fuzz import token_set_ratio, token_sort_ratio

from src.enrichment.place_words import PLACE_WORDS

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

# Precision-first defaults; tune via the CLI (all stamped into resolver_version).
ROLLUP_THRESHOLD = 90.0  # Tier 2: min token_set_ratio for a >=2-distinctive-token entity rollup
TYPO_THRESHOLD = 92.0  # Tier 1: min token_sort_ratio for a spelling-typo repair
# A current-FEI mapping to more than this many distinct clean names is a shared registrant /
# contract facility / sentinel, not one establishment's rename history -> its names are NOT
# FEI-merged (they fall through to the name tiers). A real establishment, even across renames,
# carries only a few spellings. Calibrate with the diagnose_fei_fanout.sql gate.
FEI_FANOUT_CAP = 6
# Tokens in more than this many distinct names are ubiquitous boilerplate (AMERICAN ~454,
# INTERNATIONAL ~717, USA, MEDICAL…) dropped from blocking + scoring, so variants differing only
# by boilerplate reduce to equal distinctive forms.
GENERIC_DF_CUTOFF = 80

_TOKEN = re.compile(r"[A-Za-z0-9]+")
_ARTICLES = frozenset({"THE", "A", "AN"})
# Unambiguous corporate-form tokens (folded to alnum-upper) — always generic for matching.
_CORP_FORMS = frozenset(
    {
        "INC",
        "INCORPORATED",
        "LLC",
        "LLP",
        "LP",
        "CO",
        "COMPANY",
        "CORP",
        "CORPORATION",
        "LTD",
        "LIMITED",
        "GMBH",
        "AG",
        "SA",
        "SAS",
        "SARL",
        "SRL",
        "BV",
        "NV",
        "PLC",
        "PTY",
        "PVT",
        "KG",
        "OY",
        "AB",
        "SPA",
        "BHD",
        "SDN",
    }
)
_BASE_STOP = _ARTICLES | _CORP_FORMS

# Method labels, strongest first — a node keeps the strongest reason it was merged by.
_METHOD_RANK = {
    "fei_exact": 4,
    "name_variant_exact": 3,
    "name_typo_high": 2,
    "rapidfuzz_rollup": 1,
    "singleton": 0,
}
_SCORE_METHODS = frozenset({"name_typo_high", "rapidfuzz_rollup"})


@dataclass(frozen=True)
class ClusterAssignment:
    """How one clean name resolved into its cluster."""

    canonical: str  # the cluster representative (a clean name from the cluster)
    method: str  # fei_exact | name_variant_exact | name_typo_high | rapidfuzz_rollup | singleton
    score: float | None  # the similarity that merged it (score-based methods only), else None


class _UnionFind:
    """Minimal union-find with path compression (pure)."""

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def add(self, x: str) -> None:
        self.parent.setdefault(x, x)

    def find(self, x: str) -> str:
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:  # path compression
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _tokens(name: str) -> list[str]:
    return _TOKEN.findall(name.upper())


def document_frequencies(names: list[str]) -> Counter[str]:
    """Per-token count of how many distinct names contain it (for generic detection)."""
    df: Counter[str] = Counter()
    for n in names:
        for tok in set(_tokens(n)):
            df[tok] += 1
    return df


def generic_stopwords(df: Counter[str], cutoff: int = GENERIC_DF_CUTOFF) -> frozenset[str]:
    """Articles + corporate forms + any token whose document-frequency exceeds the cutoff."""
    return _BASE_STOP | frozenset(tok for tok, n in df.items() if n > cutoff)


def block_key(name: str, stop: frozenset[str] = _BASE_STOP) -> str:
    """First DISTINCTIVE token (skipping articles / corp forms / generics), upper-cased."""
    for tok in _tokens(name):
        if tok not in stop:
            return tok
    return ""


def _scoring_form(name: str, stop: frozenset[str] = _BASE_STOP) -> str:
    """The name reduced to its distinctive tokens (generics dropped), for fuzzy ratios."""
    return " ".join(tok for tok in _tokens(name) if tok not in stop)


def pick_canonical(members: list[str]) -> str:
    """Deterministic cluster representative: fewest tokens, then shortest, then alphabetical.

    The shortest form is usually the base brand ("HONDA" over "AMERICAN HONDA MOTOR CO"). The
    representative is always a real member, so md5(canonical) is a real node id.
    """
    return min(members, key=lambda n: (len(n.split()), len(n), n))


def fei_resolve(
    fei_rows: Sequence[tuple[str, str, str]],
    clean_of: dict[str, str],
    *,
    fanout_cap: int = FEI_FANOUT_CAP,
) -> tuple[list[tuple[str, str]], int]:
    """Tier 0 (DEFERRED, opt-in) — FDA names sharing a *current* establishment id, into pairs.

    Retained, tested, and callable, but OFF in the shipped resolver (``fei_merge=False``): a per-FEI
    fan-out gate stops one FEI from over-merging, but it does NOT stop the cross-FEI failure mode —
    a parent / DBA / defunct-ancestor name sits on dozens of establishment-FEIs now owned by
    DIFFERENT firms, and union-find chains them into cross-corporate blobs (the plasma/blood
    clusters). FDA leaves ``firm_surviving_nam`` ~7% populated and 0% on the bridge names, so there
    is no FDA-internal signal to untangle it (ADR 0037). FEI is therefore an attribute, not a merge
    key. Kept for a future establishment dimension (+ the FEI portal API).

    ``fei_rows`` are ``(firm_id, current_fei, current_name)`` from ``firm_fei_edges``;
    ``current_fei = coalesce(firm_surviving_fei, firm_fei_num)``. ``clean_of`` maps a ``firm_id`` to
    its clean name. Names under one current-FEI merge; a current-FEI past ``fanout_cap`` distinct
    names is gated. Returns ``(star-shaped pairs, n_gated)``.
    """
    by_fei: dict[str, set[str]] = defaultdict(set)
    for firm_id, current_fei, _current_name in fei_rows:
        if not current_fei:
            continue
        clean = clean_of.get(firm_id)
        if clean is not None:
            by_fei[current_fei].add(clean)

    pairs: list[tuple[str, str]] = []
    gated = 0
    for names in by_fei.values():
        if len(names) < 2:
            continue
        if len(names) > fanout_cap:
            gated += 1  # too many distinct firms for one establishment — a facility/sentinel FEI
            continue
        ordered = sorted(names)
        pairs.extend((ordered[0], other) for other in ordered[1:])
    return pairs, gated


def _classify(
    a: str,
    b: str,
    *,
    dtok: dict[str, frozenset[str]],
    bform: dict[str, str],
    dmulti: dict[str, frozenset[str]],
    gform: dict[str, str],
    rollup: bool,
    typo_threshold: float,
    rollup_threshold: float,
    place: frozenset[str],
) -> tuple[str | None, float | None]:
    """Decide if/how two same-block names merge — strongest (safest) tier first.

    Tier 1 (identical-set + typo) compares the CORP-FORMS-ONLY distinctive set/form (``dtok`` /
    ``bform``) so content words that distinguish firms are kept (``Sun Valley Foods`` stays
    {SUN,VALLEY,FOODS}, not {SUN}). Tier 2 uses the high-df-GENERIC-dropped multi-char tokens
    (``dmulti`` / ``gform``) so a rollup rests on genuinely-distinctive shared tokens.
    """
    if not dtok[a] or not dtok[b]:
        return None, None
    # Tier 1: identical distinctive set (punct / case / spacing / corp-form) — but not a bare
    # place/compound phrase ("Sun" must not absorb "Sun" / "Sun Valley").
    if dtok[a] == dtok[b] and not dtok[a].issubset(place):
        return "name_variant_exact", None
    ts = token_sort_ratio(bform[a], bform[b])
    if ts >= typo_threshold:  # Tier 1: spelling typo
        return "name_typo_high", float(ts)
    if rollup:  # Tier 2: >=2 shared distinctive multi-char tokens, not purely a place phrase
        shared = dmulti[a] & dmulti[b]
        if len(shared) >= 2 and not shared.issubset(place):
            r = token_set_ratio(gform[a], gform[b])
            if r >= rollup_threshold:
                return "rapidfuzz_rollup", float(r)
    return None, None


def cluster_names(
    names: list[str],
    must_link: Iterable[tuple[str, str]] = (),
    *,
    rollup: bool = False,
    typo_threshold: float = TYPO_THRESHOLD,
    rollup_threshold: float = ROLLUP_THRESHOLD,
    generic_df_cutoff: int = GENERIC_DF_CUTOFF,
    place: frozenset[str] = PLACE_WORDS,
) -> dict[str, ClusterAssignment]:
    """Cluster distinct clean names; return one ClusterAssignment per name.

    ``must_link`` pairs (Tier 0 FEI) are unioned first and labelled ``fei_exact`` (the strongest
    method). Then within each distinctive-token block, pairs merge by ``_classify`` (Tier 1 always;
    Tier 2 only when ``rollup``). Each node keeps the strongest reason it was merged by; nodes that
    never merge are ``singleton``.
    """
    nodes = list(dict.fromkeys(names))
    present = set(nodes)
    uf = _UnionFind()
    for n in nodes:
        uf.add(n)
    method: dict[str, str] = {n: "singleton" for n in nodes}
    score: dict[str, float | None] = dict.fromkeys(nodes)

    def _bump(node: str, m: str, s: float | None) -> None:
        if _METHOD_RANK[m] > _METHOD_RANK[method[node]]:
            method[node] = m
        cur = score[node]
        if s is not None and (cur is None or s > cur):
            score[node] = s

    for a, b in must_link:
        if a in present and b in present:
            uf.union(a, b)
            _bump(a, "fei_exact", None)
            _bump(b, "fei_exact", None)

    df = document_frequencies(nodes)
    gstop = generic_stopwords(
        df, generic_df_cutoff
    )  # corp + high-df boilerplate (blocking + Tier 2)
    bform = {n: _scoring_form(n, _BASE_STOP) for n in nodes}  # corp-only (Tier 1 identical + typo)
    dtok = {n: frozenset(bform[n].split()) for n in nodes}
    gform = {n: _scoring_form(n, gstop) for n in nodes}  # generic-dropped (Tier 2 scoring)
    dmulti = {n: frozenset(t for t in gform[n].split() if len(t) > 1) for n in nodes}
    blocks: dict[str, list[str]] = defaultdict(list)
    for n in nodes:
        blocks[block_key(n, gstop)].append(n)

    for key, members in blocks.items():
        if not key or len(members) < 2:
            continue
        for i in range(len(members)):
            a = members[i]
            if not dtok[a]:
                continue
            for j in range(i + 1, len(members)):
                b = members[j]
                if not dtok[b] or uf.find(a) == uf.find(b):
                    continue
                m, s = _classify(
                    a,
                    b,
                    dtok=dtok,
                    bform=bform,
                    dmulti=dmulti,
                    gform=gform,
                    rollup=rollup,
                    typo_threshold=typo_threshold,
                    rollup_threshold=rollup_threshold,
                    place=place,
                )
                if m is None:
                    continue
                uf.union(a, b)
                _bump(a, m, s)
                _bump(b, m, s)

    clusters: dict[str, list[str]] = defaultdict(list)
    for n in nodes:
        clusters[uf.find(n)].append(n)

    out: dict[str, ClusterAssignment] = {}
    for mem in clusters.values():
        canon = pick_canonical(mem)
        single = len(mem) == 1
        for n in mem:
            m = "singleton" if single else method[n]
            s = score[n] if m in _SCORE_METHODS else None
            out[n] = ClusterAssignment(canonical=canon, method=m, score=s)
    return out
