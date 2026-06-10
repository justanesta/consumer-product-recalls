-- Identify the out-of-set USCG severity value(s) that warn accepted_values [H,L,M,S] (2026-06-09).
-- stg_uscg_recalls now NULLs these in silver (preserving the bronze raw); this surfaces the raw value
-- + a sample recall so its provenance can be investigated before deciding to map it or keep it NULL.
select
    severity                    as severity_raw,
    upper(nullif(severity, '')) as severity_normalized,
    count(*)                    as n_rows,
    min(source_recall_id)       as sample_recall_id
from uscg_recalls_bronze
where upper(nullif(severity, '')) is not null
  and upper(nullif(severity, '')) not in ('H', 'L', 'M', 'S')
group by severity, upper(nullif(severity, ''))
order by n_rows desc;
