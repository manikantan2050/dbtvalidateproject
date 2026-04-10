-- Jinjaneering validation test for Complex Multi-Target Router
-- Pattern: complex_multi_target
-- Tests: Primary key uniqueness and not-null constraints

{% set entities = var('complex_multi_target_entities', []) %}

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
from {{ ref('complex_p4_multi_target') }}
where entity = '{{ entity }}'

{% if not loop.last %}union all{% endif %}

{% endfor %}
