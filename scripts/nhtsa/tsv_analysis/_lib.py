"""Shared parsing helpers for NHTSA TSV-level analysis scripts.

Used by ``identity_search.py``, ``uniqueness_at_tuple.py``,
``find_differentiator.py``, and future TSV analysis tools. Centralizes
the RCL.txt field-name → index mapping, ZIP-to-TSV streaming, SHA-256
fingerprinting, and group-by primitives so each script doesn't re-implement
them.

Field positions: 0-indexed, mirroring RCL.txt's 1-indexed numbering minus 1.
``FIELD_NAMES[0]`` is RECORD_ID; ``FIELD_NAMES[26]`` is MFR_COMP_PTNO.

Sibling scripts add this directory to ``sys.path`` so they can ``import _lib``
when invoked directly (rather than as a package). See module docstrings in
each script.
"""

from __future__ import annotations

import hashlib
import zipfile
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

EXPECTED_FIELD_COUNT = 29

# RCL.txt fields, 0-indexed (RCL.txt 1-indexed minus 1). Schema-drift
# additions are at the right edge per Finding F: NOTES (post-2007),
# RCL_CMPT_ID (post-2008), MFR_COMP_* (post-2020), DO_NOT_DRIVE /
# PARK_OUTSIDE (post-May-2025). PRE_2010 records won't have all 29
# fields, but they pass through ``iter_tsv_rows`` because the ``>=``
# check tolerates extra/short rows by skipping the short ones (drift
# rows would be quarantined by the production extractor anyway).
FIELD_NAMES: tuple[str, ...] = (
    "RECORD_ID",
    "CAMPNO",
    "MAKETXT",
    "MODELTXT",
    "YEARTXT",
    "MFGCAMPNO",
    "COMPNAME",
    "MFGNAME",
    "BGMAN",
    "ENDMAN",
    "RCLTYPECD",
    "POTAFF",
    "ODATE",
    "INFLUENCED_BY",
    "MFGTXT",
    "RCDATE",
    "DATEA",
    "RPNO",
    "FMVSS",
    "DESC_DEFECT",
    "CONEQUENCE_DEFECT",
    "CORRECTIVE_ACTION",
    "NOTES",
    "RCL_CMPT_ID",
    "MFR_COMP_NAME",
    "MFR_COMP_DESC",
    "MFR_COMP_PTNO",
    "DO_NOT_DRIVE",
    "PARK_OUTSIDE",
)
assert len(FIELD_NAMES) == EXPECTED_FIELD_COUNT  # noqa: S101 — load-time invariant

# Lowercase name → 0-indexed position. CLI args use lowercase per
# project convention; internal field-name display uses the canonical
# uppercase form.
NAME_TO_INDEX: dict[str, int] = {name.lower(): i for i, name in enumerate(FIELD_NAMES)}


def _inner_txt_name(zip_path: Path) -> str:
    """Return the first ``*.txt`` member name inside the ZIP."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith(".txt"):
                return name
    raise ValueError(f"No .txt member found inside {zip_path}")


def iter_tsv_rows(zip_path: Path) -> Iterator[list[str]]:
    """Yield each TSV row as a list of fields, decoded UTF-8.

    Skips rows whose field count is below ``EXPECTED_FIELD_COUNT``
    (those would land in ``nhtsa_recalls_rejected`` as drift rows in
    production; for analysis we ignore them). Trailing ``\\r`` from
    Windows-style line endings (Finding E) is stripped so field 29
    isn't polluted with a trailing CR.
    """
    inner = _inner_txt_name(zip_path)
    with zipfile.ZipFile(zip_path) as zf, zf.open(inner) as f:
        for raw in f:
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            fields = line.split("\t")
            if len(fields) >= EXPECTED_FIELD_COUNT:
                yield fields


def inner_sha256_prefix(zip_path: Path, prefix_chars: int = 12) -> str:
    """Compute SHA-256 of the unzipped TSV bytes, return first N hex chars.

    Mirrors ``extraction_runs.response_inner_content_sha256`` in bronze
    so the user can tie an analysis run back to a specific extraction.
    """
    inner = _inner_txt_name(zip_path)
    h = hashlib.sha256()
    with zipfile.ZipFile(zip_path) as zf, zf.open(inner) as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:prefix_chars]


def parse_tuple_arg(s: str) -> tuple[int, ...]:
    """Parse a comma-separated lowercase field-name list into 0-indexed positions.

    Example: ``"campno,maketxt,modeltxt"`` → ``(1, 2, 3)``.

    Raises ``ValueError`` with the full known-fields list if any name
    is unknown — friendlier than a silent KeyError.
    """
    names = [n.strip().lower() for n in s.split(",")]
    indices: list[int] = []
    for name in names:
        if name not in NAME_TO_INDEX:
            known = ", ".join(sorted(NAME_TO_INDEX))
            raise ValueError(f"Unknown field {name!r}. Known fields: {known}")
        indices.append(NAME_TO_INDEX[name])
    return tuple(indices)


def group_by_tuple(
    rows: list[list[str]], indices: tuple[int, ...]
) -> dict[tuple[str, ...], list[list[str]]]:
    """Group rows by the values at the given 0-indexed field positions."""
    groups: dict[tuple[str, ...], list[list[str]]] = defaultdict(list)
    for fields in rows:
        key = tuple(fields[i] if i < len(fields) else "" for i in indices)
        groups[key].append(fields)
    return groups


def differing_fields_in_group(members: list[list[str]]) -> list[str]:
    """Given a multi-row group, return TSV field names whose values differ
    across the rows after stripping field 1 (RECORD_ID).

    Returns an empty list if all rows are byte-identical (modulo RECORD_ID).
    """
    if len(members) < 2:
        return []
    distinct_stripped = sorted({"\t".join(f[1:]) for f in members})
    if len(distinct_stripped) < 2:
        return []
    split_lines = [line.split("\t") for line in distinct_stripped]
    max_fields = max(len(s) for s in split_lines)
    differing: list[str] = []
    for stripped_idx in range(max_fields):
        values = {s[stripped_idx] if stripped_idx < len(s) else "" for s in split_lines}
        if len(values) > 1:
            tsv_idx = stripped_idx + 1  # +1 for the stripped RECORD_ID
            if tsv_idx < len(FIELD_NAMES):
                differing.append(FIELD_NAMES[tsv_idx])
            else:
                differing.append(f"field_{tsv_idx + 1}")
    return differing


def print_zip_header(zip_path: Path) -> None:
    """Emit a one-line header identifying the ZIP and its inner-TSV SHA prefix."""
    sha = inner_sha256_prefix(zip_path)
    print(f"# {zip_path.name}: inner-TSV SHA-256 = {sha}…")
    print()
