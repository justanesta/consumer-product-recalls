{{ config(severity='warn') }}
-- Continuous, source-agnostic guard that ADR 0027 empty-string normalization holds at the silver
-- serving boundary. recall_product normalizes free-text source-uniformly (the `normalized` CTE);
-- recall_event normalizes per-source in the CPSC + NHTSA branches only (FDA/USDA/USCG already nullif
-- at staging). This test is what makes that per-source asymmetry safe: it returns a row (warn) if ANY
-- source ever leaks '' or whitespace-only into a guarded free-text column — a staging-nullif removal,
-- a new un-normalized source, or a reverted wrap — none of which the per-source recall_event wraps
-- alone would defend. Zero rows == clean.
--
-- number_of_units is intentionally NOT checked: it is the quantity_crosswalk join key (nullif-only,
-- no trim, to match the untrimmed crosswalk), so it carries no '' but whitespace there is allowed.
-- On-demand exhaustive discovery peer: scripts/sql/cross_source/empty_string_freetext_audit.sql.
{{ check_blank_freetext('recall_product', ['product_name', 'product_description', 'model', 'type', 'category_id']) }}
union all
{{ check_blank_freetext('recall_event', ['title', 'recall_reason', 'corrective_action', 'consequence_of_defect', 'notes', 'mfgcampno', 'fmvss']) }}
