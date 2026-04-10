-- Jinjaneering validation test for stg_p7_incremental_source
  -- Tests: Primary key uniqueness
  select
      'ID' as test_column,
      count(*) as total_rows,
      count(distinct ID) as distinct_keys,
      case when count(*) = count(distinct ID) then 'PASS' else 'FAIL' end as result
  from {{ ref('stg_p7_incremental_source') }}
  having count(*) != count(distinct ID)
  