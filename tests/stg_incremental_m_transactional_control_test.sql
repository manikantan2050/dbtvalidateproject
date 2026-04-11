-- Jinjaneering validation test for Incremental Staging Load
-- Pattern: stg_incremental
-- Tests: Fail if model has zero rows (empty output is invalid)

select
    'stg_incremental_m_transactional_control' as model_name,
    'row_count_check' as test_type,
    count(*) as total_rows
from {{ ref('stg_incremental_m_transactional_control') }}
having count(*) = 0
