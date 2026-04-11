-- Jinjaneering validation test for Complex Multi-Target Router
-- Pattern: complex_multi_target
-- Tests: Fail if model has zero rows (empty output is invalid)

select
    'complex_multi_target_m_alltransformationmapping' as model_name,
    'row_count_check' as test_type,
    count(*) as total_rows
from {{ ref('complex_multi_target_m_alltransformationmapping') }}
having count(*) = 0
