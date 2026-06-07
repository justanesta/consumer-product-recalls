-- SCD monitor (Type-2-BENEFIT, measure-forward) — classification / severity AMENDMENTS.
--
-- recall_event_history (6c.1) already materializes this signal: it LAG()s classification over
-- bronze across FDA/USDA/USCG, excluding re-baseline waves (ADR 0027) and folding cosmetic
-- noise. So the monitor is just its classification slice. Returning rows = amendments exist →
-- graduate the ASSUMED designation to MEASURED in documentation/audit/scd_field_designations.md
-- and (re)open the SCD-2 build decision for the field. ~0 post-6a.5-reseed; accrues forward.
--
-- severity=warn via dbt_project.yml (tests/source_assumptions/). The rich/forensic version is
-- scripts/sql/cross_source/scd_monitors/assert_classification_stable.sql.

select source, source_recall_id, langcode, old_value, new_value, changed_at
from {{ ref('recall_event_history') }}
where field_name = 'classification'
