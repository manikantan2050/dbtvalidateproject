-- Test: Verify m_joiner_lkp transformation integrity
-- IICS mapping: Source(88) INNER JOIN Source1 on 5 keys -> 88 rows
-- Then Lookup enrichment from lkp_users on ORDER_ID
-- Expected: 88 rows with AuditRunDate from lookup

with validation as (
    select
        count(*) as total_rows,
        count(order_id) as has_order_id,
        count(customer_id) as has_customer_id,
        count(auditrundate) as has_auditrundate
    from {{ ref('joiner_lookup_m_joiner_lkp') }}
)

select *
from validation
where total_rows != 88
   or has_order_id != total_rows
   or has_customer_id != total_rows
   or has_auditrundate < 80