"""Guards for the centralized DB engine factory (``src/config/db.py``).

Two invariants protect the Neon connection hardening:
1. ``make_engine`` produces a ``NullPool`` engine — not the default ``QueuePool``.
2. No code under ``src/`` calls ``create_engine`` directly; everything routes
   through the factory. A new source that bypasses it would silently get the
   default ``QueuePool`` (the very thing we moved off of), so this test fails
   loudly to catch the regression.
"""

from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy.pool import NullPool

from src.config.db import make_engine

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC = _REPO_ROOT / "src"
_FACTORY = _SRC / "config" / "db.py"

# Matches an actual create_engine(...) call, not a docstring mention like
# "create_engine + R2LandingClient" (no opening paren).
_CREATE_ENGINE_CALL = re.compile(r"create_engine\s*\(")


def test_make_engine_uses_nullpool() -> None:
    engine = make_engine("postgresql://user:pass@localhost:5432/db")
    assert isinstance(engine.pool, NullPool)


def test_no_create_engine_call_outside_factory() -> None:
    offenders = [
        str(path.relative_to(_REPO_ROOT))
        for path in _SRC.rglob("*.py")
        if path != _FACTORY and _CREATE_ENGINE_CALL.search(path.read_text(encoding="utf-8"))
    ]
    assert not offenders, (
        f"create_engine() called outside src/config/db.py: {offenders}. "
        "Build engines via src.config.db.make_engine so they inherit the "
        "Neon-appropriate NullPool + keepalive config."
    )
