-- Jinjaneering validation test for complex_p10_multi_target
  -- Tests: Primary key uniqueness
  select
      'employee_id' as test_column,
      count(*) as total_rows,
      count(distinct employee_id) as distinct_keys,
      case when count(*) = count(distinct employee_id) then 'PASS' else 'FAIL' end as result
  from {{ ref('complex_p10_multi_target') }}
  having count(*) != count(distinct employee_id)
  