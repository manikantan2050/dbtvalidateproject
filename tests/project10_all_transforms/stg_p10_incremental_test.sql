-- Jinjaneering validation test for stg_p10_incremental_source
  -- Tests: Primary key uniqueness
  select
      'employee_id' as test_column,
      count(*) as total_rows,
      count(distinct employee_id) as distinct_keys,
      case when count(*) = count(distinct employee_id) then 'PASS' else 'FAIL' end as result
  from {{ ref('stg_p10_incremental_source') }}
  having count(*) != count(distinct employee_id)
  