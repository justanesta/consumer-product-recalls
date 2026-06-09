-- assert_fda_nhtsa_have_firms — firm-coverage monitor (master-plan C20; gold_design_notes
-- + implementation_plan Architectural follow-ups). severity=warn is inherited from the
-- `source_assumptions/` directory config in dbt_project.yml — do NOT set it here.
--
-- INVARIANT: every FDA and NHTSA recall_event must carry >= 1 firm in the recall_event_firm
-- bridge. Both sources always name a recalling firm (FDA establishment, NHTSA filer +
-- manufacturer), so a firmless FDA/NHTSA recall is a firm-extraction REGRESSION — hard
-- invariant, 0 baseline. Alert when this returns rows or the baseline drifts off 0.
--
-- USDA / CPSC / USCG are EXCLUDED — they carry *documented* firmless baselines:
--   USDA  ~426 (no_establishment_field, ~35% — usda/establishment_join_coverage.md)
--   CPSC  ~37  (retailer-only / non-recall announcements)
--   USCG  ~9   (Finding-S null anchor)
-- Probe / census: scripts/sql/cross_source/silver/inspect_firmless_recalls.sql.

select
    re.recall_event_id,
    re.source,
    re.source_recall_id
from {{ ref('recall_event') }} as re
left join {{ ref('recall_event_firm') }} as ref_f
    on ref_f.recall_event_id = re.recall_event_id
where re.source in ('FDA', 'NHTSA')
group by re.recall_event_id, re.source, re.source_recall_id
having count(ref_f.firm_id) = 0
