-- Singular test: FDA PRODUCTID does not renumber for the same
-- (recall_event_id, product_description_txt, recall_num) candidate key
-- across runs. Severity=warn via dbt_project.yml (per ADR 0031, the
-- threshold is "any non-zero rate" — but warn-not-error keeps existing
-- baselines from blocking PRs while we observe). Diagnostic + sample
-- query: scripts/sql/fda/bronze/assert_productid_stable.sql.

select recall_event_id, product_description_txt, recall_num
from {{ source('fda', 'fda_recalls_bronze') }}
group by recall_event_id, product_description_txt, recall_num
having count(distinct source_recall_id) > 1
   and count(distinct raw_landing_path) > 1
