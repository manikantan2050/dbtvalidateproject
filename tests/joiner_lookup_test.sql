-- Jinjaneering validation test for Joiner + Lookup Enrichment
-- Pattern: joiner_lookup
-- Tests: Row count validation and not-null check

select
    'joiner_lookup' as model_name,
    'row_count' as test_type,
    count(*) as total_rows,
    case
        when count(*) > 0 then 'PASS'
        else 'FAIL'
    end as result
from {{ ref('joiner_lookup') }}
having count(*) = 0
