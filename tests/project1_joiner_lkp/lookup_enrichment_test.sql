-- Jinjaneering validation test for lookup_enrichment
  -- Tests: Primary key uniqueness
  select
      'order_id' as test_column,
      count(*) as total_rows,
      count(distinct order_id) as distinct_keys,
      case when count(*) = count(distinct order_id) then 'PASS' else 'FAIL' end as result
  from {{ ref('lookup_enrichment') }}
  having count(*) != count(distinct order_id)
  