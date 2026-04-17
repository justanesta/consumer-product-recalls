# 0012 — Extractor pattern: custom ABC + per-source subclasses

- **Status:** Accepted
- **Date:** 2026-04-16

## Context

### Sources and extraction shapes

Five in-scope sources (per ADR 0001) with three distinct extraction shapes:

- **JSON REST APIs** — CPSC (no auth), FDA iRES (API key in custom headers), USDA FSIS (no auth)
- **Tab-delimited flat file download** — NHTSA (per ADR 0008)
- **HTML scraping** — USCG (no API exists)

### Options evaluated

Three candidate patterns:

- **NYC DCP's `dcpy`** (`github.com/NYCPlanning/dcpy`) — evaluated by subagent; details below.
- **General frameworks (Singer, Airbyte, Meltano).** None of our five sources have prebuilt connectors. Adopting any of them means writing custom taps inside someone else's conventions — pure overhead, no leverage.
- **Custom ABC + per-source subclasses.** Full control; portfolio-visible architecture; no external dependency to justify; ~500–800 LOC of infrastructure.

### The dcpy evaluation

Core abstractions (in `dcpy/connectors/registry.py` and `dcpy/models/lifecycle/ingest.py`):

- `GenericConnector(ABC, BaseModel)` — the base class. Notable for **double inheritance**: both a Python Abstract Base Class (enforcing that subclasses implement specific methods or Python refuses to instantiate them) and a Pydantic `BaseModel` (validating config at construction).
- Three operation-specialized ABCs: `Pull` (fetch data from sources), `Push` (publish data to external destinations), `VersionedConnector` (manage labeled dataset versions).
- `ConnectorRegistry` — maps string type names to connector classes for runtime dispatch from config.
- YAML ingest templates (`ingest_templates/*.yml`) parsed into Pydantic `DatasetDefinition` models.
- Typer CLI (`dcpy/lifecycle/ingest/_cli.py`) orchestrates extract → transform → validate.

Project health: active (last push 2026-04-16) but internal to NYC DCP — 13 contributors, 39 stars, 187 open issues, no PyPI release discipline. Consumers would pin a git SHA.

Fit for our sources: partial. Flat-file support is good. HTML scraping is per-source bespoke (no reusable abstraction). REST with custom-header auth is not first-class — `WebConnector` hardcodes a User-Agent only; custom auth requires subclassing or forking.

### What we adopt from dcpy (the two load-bearing patterns)

**1. The Pull ABC pattern — `ABC + BaseModel` double inheritance.**

The critical design choice is that a connector *is* its own config object. Because the class inherits from both:

- `ABC` gives the interface contract — subclasses must implement `pull()` (our `extract()`) or Python refuses to instantiate them.
- `BaseModel` gives Pydantic-validated config — URL, rate limits, auth settings are validated at construction time, not at first use.

This combination enables:

| Capability | Plain class | ABC + BaseModel |
|---|---|---|
| Config validation | Happens (if at all) inside methods at first use | At `__init__`, by Pydantic. Wrong type fails immediately with a field-level error. |
| Operation polymorphism | "Does it have `extract()`?" — trust the name | Type system guarantees: a `Pull` subclass *has* `pull()`, statically checkable. |
| Discriminated-union deserialization | Hand-rolled parsing | Pydantic's `Field(discriminator='conn_type')` turns a YAML dict into a typed connector instance in one line. |
| Composable capabilities | Multiple inheritance or copy-paste | Subclasses can layer interfaces cleanly. |

The discriminated-union property is what makes YAML-driven instantiation clean: a YAML template says `extractor_type: rest_api` and Pydantic builds the right fully-validated connector class.

**2. The YAML-template-per-source pattern.**

In dcpy, each dataset has a YAML file (e.g. `ingest_templates/bpl_libraries.yml`) declaring connector type, URL, filters, validation rules, and destination. The YAML is parsed into a `DatasetDefinition` Pydantic model containing nested validated connector instances ready to call `.pull()` on.

For our project, this enables:

- **Declarative source definitions.** Adding a CPSC-like source becomes a YAML file, not a Python class — an order-of-magnitude reduction in friction.
- **Config-as-diff.** URL changes, credential rotation, cadence tweaks are reviewable YAML PRs, not Python diffs with magic strings.
- **Environment separation without code branching.** Dev, staging, and prod become different YAML overlays, not `if env == "prod"` logic.
- **Config is its own documentation.** `cat config/sources/*.yaml` shows a reader everything the pipeline extracts, without grepping Python.

Break-even on setup cost is roughly 3–4 sources. We have 5, with potential for EPA to return (ADR 0001). The pattern pays off.

### What we deliberately do NOT adopt from dcpy

- **The `Push` ABC.** Push models "write data from our system out to someone else's system" — canonically, publishing datasets to Socrata, CKAN, or a public data portal. It carries vocabulary for destination credentials, published-artifact versioning, and ACL/permission handling. None of our terminal destinations (R2, Postgres) work that way — we own them. If we ever expose public recall data via an API, that's a FastAPI *serving-layer* concern, not a pipeline Push. Adopting Push would import vocabulary for operations we never perform.

- **The `VersionedConnector` ABC.** VersionedConnector models sources that publish discrete, labeled snapshots (e.g. "Census 2020 Decennial Release v3.2"), with `list_versions()` and `pull(version=...)` operations. None of our five sources work that way. They publish continuous record streams with last-modified timestamps — CPSC `LastPublishDate`, FDA `eventlmd`, USDA `field_last_modified_date`, NHTSA full-snapshot flat file, USCG scrape diff. We query incrementally or compare full snapshots with content hashing. The equivalent versioning concern is handled in our pipeline at the bronze layer (content-hashed snapshots per ADR 0007) and at silver via dbt snapshots (ADR 0011) where needed. Same capability reached by a cleaner abstraction for our shape.

- **The `ConnectorRegistry`.** dcpy loads dozens of connectors dynamically by string type; a registry earns its keep there. We have three operation types (REST, flat file, scrape) and five sources. Direct Python imports are simpler than runtime dispatch at this scale.

- **`edm_recipes`, `Socrata`, `geosupport`, and other NYC-specific integrations.** Irrelevant to recalls.

- **The Typer CLI orchestration layer.** GitHub Actions is our orchestrator per ADR 0010.

- **YAML "processing" step chaining.** dcpy's templates chain transformations inside the template. We keep extractors focused on extract + land + validate + load-bronze; transformation belongs in dbt (ADR 0011). Clean separation of concerns.

## Decision

Extraction uses a custom `Extractor` abstract base class with five concrete subclasses. Design patterns borrowed (not the dependency) from NYC DCP's `dcpy`: the `Pull` ABC shape and the YAML-template-per-source idea for declarative extractor config.

**Layout:**

```
src/
  extractors/
    _base.py            # Extractor ABC
    cpsc.py             # CpscExtractor(Extractor)
    fda.py              # FdaExtractor(Extractor)   -- API-key auth, signature cache-busting
    usda.py             # UsdaExtractor(Extractor)  -- bilingual raw landing
    nhtsa.py            # NhtsaFlatFileExtractor(Extractor)  -- per ADR 0008
    uscg.py             # UscgScrapingExtractor(Extractor)
  schemas/              # Pydantic bronze-contract models per source
    cpsc.py, fda.py, usda.py, nhtsa.py, uscg.py
config/
  sources/              # YAML per-source declarative config
    cpsc.yaml, fda.yaml, usda.yaml, nhtsa.yaml, uscg.yaml
```

**ABC lifecycle (enforced on every subclass):**

1. `extract()` — fetch raw bytes/records from the source
2. `land_raw()` — write raw payload to R2 (per ADR 0004), partitioned by `source/extraction_date/`
3. `validate()` — parse via Pydantic model; `ValidationError` → fail loud, do not proceed
4. `load_bronze()` — content-hash the canonical record, conditionally insert into source bronze table (per ADR 0007)

**Shared on the ABC:** exponential-backoff retry with jitter, rate limiting, structured JSON logging with correlation IDs, timing metrics emitted for each stage.

**Per-source YAML config (declarative, git-diff-able):** source URL, cadence, credentials secret names, filter parameters (e.g. FDA's `eventlmdfrom`), rate-limit policy. Python classes read their config at instantiation; no behavior baked into code that could instead be config.

**Pydantic schema stance (preview of ADR 0013):**
- `model_config = ConfigDict(extra='forbid', strict=True)` on every bronze schema — unknown fields surface loud.
- Fields declared as required (non-`Optional`, no default) unless the source explicitly documents them as nullable. This catches silent renames: old-name missing → missing-required-field error; new-name present → forbid error. Both sides loud.
- Truly nullable source fields use `Optional[...] = None` — downstream dbt `not_null` / `accepted_values` tests on silver provide the second net.

## Consequences

- Architecture is portfolio-visible: explicit ABC design, clean separation of generic (retry, logging, landing) and specific (per-source quirks) concerns.
- Zero external extraction-framework dependency. Extraction logic is owned and readable end-to-end.
- ~500–800 LOC of infrastructure code in `_base.py` and shared modules; each additional source adds a subclass, a Pydantic schema, and a YAML config.
- YAML config per source means credentials, cadence, and URL changes are config diffs — reviewable without touching extractor code.
- Credit to NYC DCP's `dcpy` for the ABC pattern and YAML-template idea. Not a dependency; a reference.
- Pydantic `extra='forbid'` + required-by-default stance catches schema additions and renames; value-level semantic drift is left to silver-layer dbt tests.
- Clean migration seam: if the extractor infrastructure grows complex enough to justify a framework later, the `Extractor` ABC boundary is where that migration happens.
