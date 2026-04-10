-- Jinjaneering validation test for stg_p5_incremental_source
  -- Tests: Primary key uniqueness
  select
      'FIELD1' as test_column,
      count(*) as total_rows,
      count(distinct FIELD1) as distinct_keys,
      case when count(*) = count(distinct FIELD1) then 'PASS' else 'FAIL' end as result
  from {{ ref('stg_p5_incremental_source') }}
  having count(*) != count(distinct FIELD1)
  