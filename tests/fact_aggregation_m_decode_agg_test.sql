-- Jinjaneering validation test for Fact Aggregation
-- Pattern: fact_aggregation
-- Tests: Fail if model has zero rows (empty output is invalid)

select
    'fact_aggregation_m_decode_agg' as model_name,
    'row_count_check' as test_type,
    count(*) as total_rows
from {{ ref('fact_aggregation_m_decode_agg') }}
having count(*) = 0
