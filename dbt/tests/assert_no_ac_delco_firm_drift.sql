-- Regression backstop (ADR 0033 Normalization class, Phase 6b PR 6b.3): 'AC DELCO' /
-- 'ACDELCO' is a NHTSA `maketxt` (equipment MAKE) normalization with 0 firm-name occurrences
-- (confirmed-in-doc). It is fixed at the PRODUCT grain — stg_nhtsa_recalls' identity partition
-- AND the recall_product_id md5 both canonicalize maketxt via the normalize_maketxt macro —
-- and must never re-enter the firm dimension. Returns rows if any firm.normalized_name
-- collapses to 'ACDELCO' under the SAME macro (i.e. the make leaked into firm). Expect 0 rows.
-- Narrow by design: vehicle makes legitimately overlap firm names (HARLEY-DAVIDSON is both a
-- make and a filer), so a blanket "no maketxt in firm" guard would false-fail — only the AC
-- DELCO drift class is asserted absent.

select f.firm_id, f.normalized_name
from {{ ref('firm') }} f
where {{ normalize_maketxt('f.normalized_name') }} = 'ACDELCO'
