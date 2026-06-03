-- Profile FDA firm_fei_num for the firm_fda_attributes sidecar key decision.
-- Workstream W1 of the (b) capture-expansion PR
-- (project_scope/silver-field-capture-expansion-plan.md).
--
-- The two existing firm sidecars (firm_establishment_attributes,
-- firm_manufacturer_attributes) are keyed on their source's registry id
-- (establishment_number / mic) and fed by a one-row-per-entity directory source.
-- FDA has NO directory — its firm fields ride inline on the recall feed, one row
-- per product — so firm_fda_attributes must collapse to one row per key. This
-- profile answers the key-choice questions before we lock the model:
--
--   Q1 — Coverage: of distinct FDA firms (by normalized legal name), how many lack
--        any FEI? A FEI-keyed sidecar silently drops a firm's address if its FEI is
--        null. If pct_without_fei is material, key on normalized_name instead (like
--        firm.sql) and carry FEI as an attribute.
--   Q2 — FEI -> address cardinality: does a single FEI ever carry >1 distinct
--        address? That is what makes the Type-1 "latest wins" DISTINCT ON collapse
--        do real work (a firm that moved) rather than being a no-op.
--   Q3 — FEI <-> name cardinality: one FEI -> many names (renamed firm) or one name
--        -> many FEIs (key would split one firm-name into several sidecar rows).
--   Q4 — Address-field population at the latest-product grain — a full-corpus refresh
--        of the 2026-05-29 100-record-window numbers in capture_expansion_backlog.md.
--
-- Runs against full bronze (FDA re-seeded 2026-06-02 -> 134,450 distinct products)
-- per the full-corpus-validation rule. The _lp temp table mirrors stg_fda_recalls'
-- latest-row-per-product projection so the numbers match what the sidecar will see.
--
-- Usage (user runs):
--   set -a && . .env && set +a
--   PGPASSWORD="$NEON_PASSWORD" psql -h "$NEON_HOST" -U "$NEON_USER" -d "$NEON_DBNAME" \
--     -f scripts/sql/fda/bronze/profile_firm_fei_for_sidecar.sql

\set ON_ERROR_STOP on

-- Latest row per product (mirrors stg_fda_recalls ROW_NUMBER partition by
-- source_recall_id order by extraction_timestamp desc).
create temp table _lp as
select
    upper(trim(firm_legal_nam))      as normalized_name,
    firm_fei_num,
    nullif(trim(firm_city_nam), '')        as firm_city_nam,
    nullif(trim(firm_state_cd), '')        as firm_state_cd,
    nullif(trim(firm_state_prvnc_nam), '') as firm_state_prvnc_nam,
    nullif(trim(firm_country_nam), '')     as firm_country_nam,
    nullif(trim(firm_postal_cd), '')       as firm_postal_cd,
    nullif(trim(firm_line1_adr), '')       as firm_line1_adr,
    nullif(trim(firm_line2_adr), '')       as firm_line2_adr,
    nullif(trim(firm_surviving_nam), '')   as firm_surviving_nam,
    firm_surviving_fei
from (
    select *,
           row_number() over (
               partition by source_recall_id
               order by extraction_timestamp desc
           ) as rn
    from fda_recalls_bronze
) t
where rn = 1
  and firm_legal_nam is not null
  and trim(firm_legal_nam) <> '';

-- Q1 — FEI coverage at firm (normalized-name) grain.
with firm_fei as (
    select
        normalized_name,
        max(firm_fei_num)          as any_fei,        -- non-null if ANY row has an FEI
        count(distinct firm_fei_num) as distinct_fei
    from _lp
    group by normalized_name
)
select
    'Q1 firm-grain FEI coverage' as q,
    count(*)                                              as distinct_firms,
    count(*) filter (where any_fei is not null)           as firms_with_fei,
    count(*) filter (where any_fei is null)               as firms_without_fei,
    round(100.0 * count(*) filter (where any_fei is null)
          / nullif(count(*), 0), 1)                       as pct_without_fei,
    count(*) filter (where distinct_fei > 1)              as firms_with_multiple_fei
from firm_fei;

-- Q2/Q3 — per-FEI address and name cardinality.
with fei_grain as (
    select
        firm_fei_num,
        count(distinct (
            coalesce(firm_city_nam, '')  || '|' || coalesce(firm_state_cd, '') || '|' ||
            coalesce(firm_postal_cd, '') || '|' || coalesce(firm_line1_adr, '')
        ))                                       as distinct_addresses,
        count(distinct normalized_name)          as distinct_names
    from _lp
    where firm_fei_num is not null
    group by firm_fei_num
)
select
    'Q2/Q3 per-FEI cardinality' as q,
    count(*)                                          as distinct_feis,
    count(*) filter (where distinct_addresses > 1)    as feis_multi_address,
    max(distinct_addresses)                           as max_addresses_one_fei,
    count(*) filter (where distinct_names > 1)        as feis_multi_name,
    max(distinct_names)                               as max_names_one_fei
from fei_grain;

-- Q4 — address-field population at the latest-product grain (full-corpus refresh).
select
    'Q4 field population (latest-product rows)' as q,
    count(*)                                                          as latest_product_rows,
    round(100.0 * count(firm_fei_num)        / count(*), 1)           as pct_fei,
    round(100.0 * count(firm_city_nam)       / count(*), 1)           as pct_city,
    round(100.0 * count(firm_state_cd)       / count(*), 1)           as pct_state_cd,
    round(100.0 * count(firm_country_nam)    / count(*), 1)           as pct_country,
    round(100.0 * count(firm_postal_cd)      / count(*), 1)           as pct_postal,
    round(100.0 * count(firm_line1_adr)      / count(*), 1)           as pct_line1,
    round(100.0 * count(firm_line2_adr)      / count(*), 1)           as pct_line2,
    round(100.0 * count(firm_surviving_nam)  / count(*), 1)           as pct_surviving_nam,
    round(100.0 * count(firm_surviving_fei)  / count(*), 1)           as pct_surviving_fei
from _lp;
