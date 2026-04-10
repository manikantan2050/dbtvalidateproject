-- Jinjaneering validation test for fact_aggregation
  -- Tests: Primary key uniqueness
  select
      'employee_id' as test_column,
      count(*) as total_rows,
      count(distinct employee_id) as distinct_keys,
      case when count(*) = count(distinct employee_id) then 'PASS' else 'FAIL' end as result
  from {{ ref('fact_aggregation') }}
  having count(*) != count(distinct employee_id)
  