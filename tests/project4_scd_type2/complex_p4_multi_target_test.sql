-- Jinjaneering validation test for complex_p4_multi_target
  -- Tests: Primary key uniqueness
  select
      'EMP_ID' as test_column,
      count(*) as total_rows,
      count(distinct EMP_ID) as distinct_keys,
      case when count(*) = count(distinct EMP_ID) then 'PASS' else 'FAIL' end as result
  from {{ ref('complex_p4_multi_target') }}
  having count(*) != count(distinct EMP_ID)
  