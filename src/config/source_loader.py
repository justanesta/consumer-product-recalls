"""YAML loader for ``config/sources/<source>.yaml`` per ADR 0012.

Single-file-per-source MVP: no env overlays, no merge logic. Validates the
loaded YAML through the discriminated union in ``source_registry``; mismatches
fail loud with Pydantic's standard ValidationError.

Per-environment overlays (``<source>.<env>.yaml``) are deferred to a Phase 7
prerequisite tracked in ``project_scope/implementation_plan.md``.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import TypeAdapter

from src.config.source_registry import (
    FlatFileSourceConfig,
    HtmlScrapingSourceConfig,
    RestApiSourceConfig,
    SourceConfig,
)

_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config" / "sources"
_ADAPTER: TypeAdapter[SourceConfig] = TypeAdapter(SourceConfig)


def load_source_config(
    source_name: str,
) -> RestApiSourceConfig | FlatFileSourceConfig | HtmlScrapingSourceConfig:
    """Load and validate ``config/sources/<source_name>.yaml``.

    Raises ``FileNotFoundError`` if the YAML file doesn't exist.
    Raises ``pydantic.ValidationError`` if the shape doesn't match the
    discriminated union (unknown ``source_type``, missing required fields,
    extra fields under ``extra="forbid"``).
    """
    yaml_path = _CONFIG_DIR / f"{source_name}.yaml"
    if not yaml_path.is_file():
        raise FileNotFoundError(
            f"No source config at {yaml_path} (looked up source_name={source_name!r})"
        )
    raw = yaml.safe_load(yaml_path.read_text())
    return _ADAPTER.validate_python(raw)
