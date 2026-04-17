"""Pre-commit hook: verify no auth credentials are present in committed VCR cassettes.

Receives tests/fixtures/cassettes/**/*.yaml file paths as argv. Exits 1 if any
cassette contains a sensitive header or query parameter with a non-redacted value.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Header names and query params that must never appear with real values.
# Values are only allowed if they equal the redaction placeholder.
SENSITIVE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^\s*Authorization\s*:", re.IGNORECASE),
    re.compile(r"^\s*Authorization-User\s*:", re.IGNORECASE),
    re.compile(r"^\s*Authorization-Key\s*:", re.IGNORECASE),
    re.compile(r"^\s*X-Api-Key\s*:", re.IGNORECASE),
    re.compile(r"^\s*X-API-Key\s*:", re.IGNORECASE),
]

REDACTED_VALUE_PATTERN = re.compile(r"<REDACTED>", re.IGNORECASE)

# FDA signature query parameter in URIs
SIGNATURE_IN_URI_PATTERN = re.compile(r"signature=[^&\s<>\"']+")
REDACTED_SIGNATURE_PATTERN = re.compile(r"signature=<REDACTED>", re.IGNORECASE)


def check_file(path: Path) -> list[str]:
    errors: list[str] = []
    lines = path.read_text(encoding="utf-8").splitlines()

    for lineno, line in enumerate(lines, start=1):
        # Check sensitive headers
        for pattern in SENSITIVE_PATTERNS:
            if pattern.search(line):
                if not REDACTED_VALUE_PATTERN.search(line):
                    snippet = line.strip()
                    errors.append(
                        f"{path}:{lineno}: sensitive header with non-redacted value: {snippet!r}"
                    )
                break

        # Check signature= in URIs
        if SIGNATURE_IN_URI_PATTERN.search(line) and not REDACTED_SIGNATURE_PATTERN.search(line):
            snippet = line.strip()
            errors.append(
                f"{path}:{lineno}: signature= query param with non-redacted value: {snippet!r}"
            )

    return errors


def main() -> int:
    files = [Path(p) for p in sys.argv[1:]]
    all_errors: list[str] = []
    for path in files:
        if path.suffix in {".yaml", ".yml"} and path.exists():
            all_errors.extend(check_file(path))

    for error in all_errors:
        print(error, file=sys.stderr)

    return 1 if all_errors else 0


if __name__ == "__main__":
    sys.exit(main())
