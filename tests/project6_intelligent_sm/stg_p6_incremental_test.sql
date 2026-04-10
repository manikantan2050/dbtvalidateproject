-- Jinjaneering validation test for stg_p6_incremental_source
  -- Tests: Primary key uniqueness
  select
      'Path' as test_column,
      count(*) as total_rows,
      count(distinct Path) as distinct_keys,
      case when count(*) = count(distinct Path) then 'PASS' else 'FAIL' end as result
  from {{ ref('stg_p6_incremental_source') }}
  having count(*) != count(distinct Path)
  