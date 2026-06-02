"""Unit tests for the NHTSA by-archive inner-SHA backfill (pure transform).

The DB/R2 I/O (``backfill`` / ``_manifest_runs``) is exercised by the live acceptance run;
here we pin the pure manifest → ``{url: sha}`` transform, including the malformed-entry
edge cases that a real R2 manifest could carry.
"""

from __future__ import annotations

import sys
from pathlib import Path

# scripts/ is not on sys.path by default; add the repo root so we can import the script.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.nhtsa.backfill_inner_sha_by_archive import (  # noqa: E402 — sys.path mutated above
    manifest_to_sha_map,
)

_PRE_URL = "https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_PRE_2010.zip"
_POST_URL = "https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip"


def test_maps_both_archives() -> None:
    manifest = {
        "deep_rescan": True,
        "sources": [
            {"url": _PRE_URL, "r2_path": "nhtsa/x/pre.zip", "inner_content_sha256": "pre-sha"},
            {"url": _POST_URL, "r2_path": "nhtsa/x/post.zip", "inner_content_sha256": "post-sha"},
        ],
    }
    assert manifest_to_sha_map(manifest) == {_PRE_URL: "pre-sha", _POST_URL: "post-sha"}


def test_empty_when_no_sources() -> None:
    assert manifest_to_sha_map({"deep_rescan": True}) == {}
    assert manifest_to_sha_map({"deep_rescan": True, "sources": []}) == {}


def test_skips_entries_missing_url_or_sha() -> None:
    manifest = {
        "sources": [
            {"url": _PRE_URL, "inner_content_sha256": "pre-sha"},
            {"url": _POST_URL, "inner_content_sha256": ""},  # empty sha — skip
            {"inner_content_sha256": "orphan-sha"},  # no url — skip
        ]
    }
    assert manifest_to_sha_map(manifest) == {_PRE_URL: "pre-sha"}
