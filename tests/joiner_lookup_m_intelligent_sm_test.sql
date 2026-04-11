-- Jinjaneering validation test for Joiner + Lookup Enrichment
-- Pattern: joiner_lookup
-- Tests: Fail if model has zero rows (empty output is invalid)

select
    'joiner_lookup_m_intelligent_sm' as model_name,
    'row_count_check' as test_type,
    count(*) as total_rows
from {{ ref('joiner_lookup_m_intelligent_sm') }}
having count(*) = 0
