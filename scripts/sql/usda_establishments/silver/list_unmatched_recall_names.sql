-- List the recall-side establishment names that don't match any FSIS-listed
-- establishment after the Phase 5b.2 Step 5 join (HTML-decode + name match).
--
-- Verification probe 2 in verify_step5_silver.sql shows the aggregate match
-- rate (99.27% as of 2026-05-02 dev). This script surfaces the *specific*
-- residual unmatched names so they can be inspected and triaged for Phase 6
-- firm entity resolution. Possible causes for any name appearing here:
--   - Genuinely novel firm not in the FSIS establishment list.
--   - Typo or non-canonical spelling on the recall side.
--   - Firm delisted from FSIS but still referenced in a historical recall.
--   - Whitespace or encoding pattern beyond the &#039; / &amp; pair.
--
-- No psql variables. Run as:
--   psql -f scripts/sql/usda_establishments/silver/list_unmatched_recall_names.sql

\pset null '<NULL>'

with recall_names as (
    select distinct
        upper(trim(establishment)) as nrm,
        establishment              as raw
    from public.stg_usda_fsis_recalls
    where establishment is not null
      and trim(establishment) <> ''
),
est_names as (
    select distinct upper(trim(establishment_name)) as nrm
    from public.stg_usda_fsis_establishments
)
select r.raw as unmatched_recall_name
from recall_names r
left join est_names e using (nrm)
where e.nrm is null
order by r.raw;
