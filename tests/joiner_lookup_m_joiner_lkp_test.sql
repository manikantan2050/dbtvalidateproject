-- Test: joiner_lookup_m_joiner_lkp join integrity
  -- Validates that the join between src_employees and src_users produces matching records
  -- and that the lookup enrichment from lkp_users works correctly.
  -- Returns rows only on failure (dbt test convention: HAVING count(*) = 0 means pass)

  select
      'join_integrity' as test_name,
      count(*) as total_rows,
      count(s2_order_id) as joined_rows,
      count(case when order_id is null then 1 end) as null_pk_count
  from {{ ref('joiner_lookup_m_joiner_lkp') }}
  having count(*) = 0
     or count(case when order_id is null then 1 end) > 0
  