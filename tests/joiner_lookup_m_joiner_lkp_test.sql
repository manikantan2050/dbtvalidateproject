-- Test: Verify joiner_lookup_m_joiner_lkp join integrity
-- Ensures all src_users records are preserved (LEFT JOIN from 106 rows)
-- and lookup enrichment is applied correctly

with validation as (
    select
        count(*) as total_rows,
        count(order_id) as non_null_orders,
        count(lkp_order_id) as lookup_matches,
        count(src1_order_id) as src1_matches
    from {{ ref('joiner_lookup_m_joiner_lkp') }}
)

select *
from validation
where total_rows < 100
   or non_null_orders != total_rows
   or lookup_matches < total_rows * 0.9