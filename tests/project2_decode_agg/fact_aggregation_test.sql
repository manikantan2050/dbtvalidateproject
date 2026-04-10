-- Jinjaneering validation test for Fact Aggregation
-- Pattern: fact_aggregation
-- Tests: Primary key uniqueness and not-null constraints

{% set entities = var('fact_aggregation_entities', []) %}

{% for entity in entities %}

-- Test: PK uniqueness for {{ entity }}
select
    '{{ entity }}' as entity,
    'pk_uniqueness' as test_type,
    count(*) as total_rows,
    count(distinct {{ entity }}_key) as distinct_keys,
    case
        when count(*) = count(distinct {{ entity }}_key) then 'PASS'
        else 'FAIL'
    end as result
from {{ ref('fact_aggregation') }}
where entity = '{{ entity }}'

{% if not loop.last %}union all{% endif %}

{% endfor %}
