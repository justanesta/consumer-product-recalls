"""YAML loader for ``config/sources/<source>.yaml`` per ADR 0012.

Single base file per source, with an OPTIONAL per-environment overlay
(``<source>.<env>.yaml``) deep-merged on top when ``RECALLS_ENV`` is set
(the Phase 7 prerequisite). The merged dict is validated through the
discriminated union in ``source_registry``; mismatches — including any key an
overlay introduces that the schema doesn't declare — fail loud with Pydantic's
standard ``ValidationError`` (``extra="forbid"`` on every source model).

Overlay semantics (``deep_merge``):
- nested dicts merge field-by-field (recursively);
- scalars and lists in the overlay REPLACE the base value wholesale;
- a key present only in the overlay is added.

When ``RECALLS_ENV`` is unset/empty, or no ``<source>.<env>.yaml`` exists, the
base file is used unchanged — today's behavior, zero change.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import TypeAdapter, ValidationError

from src.config.source_registry import (
    FlatFileSourceConfig,
    HtmlScrapingSourceConfig,
    RestApiSourceConfig,
    SourceConfig,
)

_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config" / "sources"
_ADAPTER: TypeAdapter[SourceConfig] = TypeAdapter(SourceConfig)

# Env var selecting the active overlay (e.g. ``prod``, ``staging``). Read from
# ``os.environ`` rather than ``Settings`` deliberately: ``Settings`` is
# ``extra="forbid"`` and declaring this there would force it on every run, and
# it is config-selection, not a credential. Mirrors how ``LOG_FORMAT`` /
# ``GITHUB_*`` are read directly from the environment elsewhere in the codebase.
_ENV_VAR = "RECALLS_ENV"


def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge ``overlay`` onto ``base``, returning a new dict (pure — no I/O).

    Nested dicts merge recursively; scalars and lists in ``overlay`` replace the
    base value wholesale (a list is treated as an atomic value, not merged
    element-by-element). Keys present only in ``overlay`` are added. Neither
    input is mutated.
    """
    merged: dict[str, Any] = dict(base)
    for key, overlay_value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            merged[key] = deep_merge(base_value, overlay_value)
        else:
            merged[key] = overlay_value
    return merged


def _active_env() -> str | None:
    """The active overlay env name, or ``None`` when ``RECALLS_ENV`` is unset/empty."""
    env = os.environ.get(_ENV_VAR, "").strip()
    return env or None


def load_source_config(
    source_name: str,
) -> RestApiSourceConfig | FlatFileSourceConfig | HtmlScrapingSourceConfig:
    """Load and validate ``config/sources/<source_name>.yaml``.

    When ``RECALLS_ENV`` is set and ``config/sources/<source_name>.<env>.yaml``
    exists, it is deep-merged onto the base before validation.

    Raises ``FileNotFoundError`` if the base YAML file doesn't exist.
    Raises ``pydantic.ValidationError`` if the (merged) shape doesn't match the
    discriminated union (unknown ``source_type``, missing required fields, or an
    extra field under ``extra="forbid"`` — including one an overlay introduces).
    The error message names the offending YAML file(s) for fast triage.
    """
    yaml_path = _CONFIG_DIR / f"{source_name}.yaml"
    if not yaml_path.is_file():
        raise FileNotFoundError(
            f"No source config at {yaml_path} (looked up source_name={source_name!r})"
        )
    raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())

    files = [yaml_path]
    env = _active_env()
    if env is not None:
        overlay_path = _CONFIG_DIR / f"{source_name}.{env}.yaml"
        if overlay_path.is_file():
            overlay: dict[str, Any] = yaml.safe_load(overlay_path.read_text())
            raw = deep_merge(raw, overlay)
            files.append(overlay_path)

    try:
        return _ADAPTER.validate_python(raw)
    except ValidationError as exc:
        # Pydantic already names the offending key(s); attach the contributing
        # file path(s) so an unknown overlay key is traceable to the exact file,
        # not just the base. ``add_note`` keeps the ValidationError type (callers
        # / existing tests still catch ValidationError) and surfaces in the
        # traceback. Re-raise so the note travels with this stack frame.
        sources = ", ".join(str(p) for p in files)
        exc.add_note(f"while validating source config from: {sources}")
        raise
