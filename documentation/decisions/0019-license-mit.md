# 0019 — License: MIT

- **Status:** Accepted
- **Date:** 2026-04-17

## Context

The project is published publicly (per ADR 0018's public-repo requirement for unlimited GitHub Actions minutes) and needs a license declaring how others may use, modify, and redistribute the code. Candidate licenses considered:

| License | Characteristics |
|---|---|
| **MIT** | Simplest permissive license. Allows use, copy, modify, merge, publish, distribute, sublicense, sell — with attribution. No patent grant clause. Short legal text. Most widely recognized in the developer community. |
| **Apache 2.0** | Permissive, but includes an explicit patent-grant clause and requires notice-file preservation. More legal verbosity. Preferred when patents are a plausible concern (typically for enterprise contributions). |
| **BSD (2-clause / 3-clause)** | Similar to MIT; 3-clause adds a no-endorsement clause. Functionally close to MIT but less recognized. |
| **GPL variants (GPL, AGPL)** | Copyleft — derivatives must also be GPL. Incompatible with the permissive-OSS default of most dependencies in this project and discourages reuse. Not a fit for a portfolio project. |
| **Proprietary / no license** | Legally "all rights reserved"; others cannot use or contribute. Defeats the portfolio-visibility goal. |

Relevant considerations for this project specifically:

- All runtime dependencies (Pydantic, dbt-core, uv, `httpx`, `tenacity`, `pydantic-settings`, `pytest-vcr`) are permissively licensed (MIT / Apache 2.0 / BSD). Any permissive license on this project is compatible with all of them.
- As a solo-contributor portfolio project, there are no patents at stake; Apache 2.0's explicit patent clause provides protection against something that isn't a concern here.
- The project benefits from easy reuse by others — a reader should be able to fork, adapt patterns, or lift code directly with minimal legal friction.

## Decision

**MIT License.**

- A `LICENSE` file at the repository root contains the standard MIT license text with copyright year 2026 and copyright holder "Adrian Nesta."
- `README.md` references the license in its Stack / Status / License section.
- The `LICENSE` file is the authoritative text; any other mention of licensing in the repository must not conflict with it.

## Consequences

- Anyone can use, copy, modify, merge, publish, distribute, sublicense, and sell copies of this project, subject to including the copyright notice and license text.
- No warranty — standard "as-is" disclaimer from the MIT text applies.
- Contributors (if any arrive) retain their own copyright on contributions but grant MIT rights via the act of contributing, consistent with typical open-source norms. No CLA (Contributor License Agreement) is required at this scale.
- Compatible with all dependencies used today. Compatible with future Apache 2.0 or BSD dependencies or integrations.
- If patent protection ever becomes relevant (e.g., if an employer absorbs the work or it is incorporated into a commercial product), re-licensing the solo-contributor codebase to Apache 2.0 is legally straightforward. Adding a second contributor would make re-licensing significantly harder without their consent — worth noting but not a current concern.

### Open for revision

- **License change if scope changes.** If the project ever becomes a library that others build commercial software on top of, Apache 2.0's patent grant might be worth revisiting. Not a v1 concern.
- **Contributor license handling.** If a second contributor joins, add a CONTRIBUTING.md clarifying that contributions are licensed MIT via the act of opening a PR. Not needed until that happens.
