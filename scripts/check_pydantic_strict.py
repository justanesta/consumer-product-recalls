"""Pre-commit hook: verify Pydantic bronze models declare extra='forbid' and strict=True.

Receives src/schemas/*.py file paths as argv. Exits 1 if any BaseModel subclass
is missing either ConfigDict setting.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def _get_basemodel_classes(tree: ast.Module) -> list[ast.ClassDef]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef)
        and any(
            (isinstance(b, ast.Name) and b.id == "BaseModel")
            or (isinstance(b, ast.Attribute) and b.attr == "BaseModel")
            for b in node.bases
        )
    ]


def _configdict_keywords(classdef: ast.ClassDef) -> dict[str, ast.expr]:
    """Return keyword args from the first model_config = ConfigDict(...) assignment."""
    for node in ast.walk(classdef):
        if not isinstance(node, ast.Assign):
            continue
        if not (len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)):
            continue
        if node.targets[0].id != "model_config":
            continue
        if not isinstance(node.value, ast.Call):
            continue
        call = node.value
        func = call.func
        func_name = (
            func.id
            if isinstance(func, ast.Name)
            else func.attr
            if isinstance(func, ast.Attribute)
            else None
        )
        if func_name == "ConfigDict":
            return {kw.arg: kw.value for kw in call.keywords if kw.arg is not None}
    return {}


def _is_string_value(node: ast.expr, value: str) -> bool:
    return isinstance(node, ast.Constant) and node.value == value


def _is_true(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


def check_file(path: Path) -> list[str]:
    errors: list[str] = []
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        return [f"{path}: SyntaxError: {exc}"]

    for cls in _get_basemodel_classes(tree):
        kwargs = _configdict_keywords(cls)
        missing: list[str] = []

        extra_node = kwargs.get("extra")
        if extra_node is None or not _is_string_value(extra_node, "forbid"):
            missing.append("extra='forbid'")

        strict_node = kwargs.get("strict")
        if strict_node is None or not _is_true(strict_node):
            missing.append("strict=True")

        if missing:
            errors.append(
                f"{path}: class {cls.name} is missing ConfigDict settings: " + ", ".join(missing)
            )

    return errors


def main() -> int:
    files = [Path(p) for p in sys.argv[1:]]
    all_errors: list[str] = []
    for path in files:
        if path.suffix == ".py" and path.exists():
            all_errors.extend(check_file(path))

    for error in all_errors:
        print(error, file=sys.stderr)

    return 1 if all_errors else 0


if __name__ == "__main__":
    sys.exit(main())
