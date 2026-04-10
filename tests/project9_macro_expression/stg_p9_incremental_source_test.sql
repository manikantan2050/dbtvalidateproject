-- Jinjaneering validation test for stg_p9_incremental_source
  -- Tests: Primary key uniqueness
  select
      'EMPLOYEE_ID' as test_column,
      count(*) as total_rows,
      count(distinct EMPLOYEE_ID) as distinct_keys,
      case when count(*) = count(distinct EMPLOYEE_ID) then 'PASS' else 'FAIL' end as result
  from {{ ref('stg_p9_incremental_source') }}
  having count(*) != count(distinct EMPLOYEE_ID)
  