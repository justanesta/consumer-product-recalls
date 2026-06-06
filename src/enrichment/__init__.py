"""Pre-silver text-processing / entity-resolution helpers (Phase 6b).

Pure, unit-tested logic that the warehouse cannot do set-based: RapidFuzz
edit-distance firm clustering (firm_resolution, PR 6b.4) and the USDA
field_product_items establishment-number extractor (usda_estab_number, PR 6b.2).
Deterministic name-cleaning lives in dbt-SQL macros, not here.
"""
