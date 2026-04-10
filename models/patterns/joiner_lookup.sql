-- Joiner + Lookup enrichment model
-- Multi-source join with lookup enrichment

with source_1 as (
    select ORDER_ID, CUSTOMER_ID, STATUS, SALESMAN_ID, ORDER_DATE
    from {{ source('source_db', 'src_employees') }}
),

source_2 as (
    select ORDER_ID, CUSTOMER_ID, STATUS, SALESMAN_ID, ORDER_DATE, AuditRunDate
    from {{ source('source_db', 'src_users') }}
),

joined as (
    select
        s1.ORDER_ID as s1_ORDER_ID,
        s1.CUSTOMER_ID as s1_CUSTOMER_ID,
        s1.STATUS as s1_STATUS,
        s1.SALESMAN_ID as s1_SALESMAN_ID,
        s1.ORDER_DATE as s1_ORDER_DATE,
        s2.ORDER_ID as s2_ORDER_ID,
        s2.CUSTOMER_ID as s2_CUSTOMER_ID,
        s2.STATUS as s2_STATUS,
        s2.SALESMAN_ID as s2_SALESMAN_ID,
        s2.ORDER_DATE as s2_ORDER_DATE,
        s2.AuditRunDate as s2_AuditRunDate
    from source_1 s1
    inner join source_2 s2
        on s1.ORDER_ID = s2.ORDER_ID and s1.CUSTOMER_ID = s2.CUSTOMER_ID
),

enriched as (
    select
        j.*,
        lkp.ORDER_ID as lkp_ORDER_ID,
        lkp.CUSTOMER_ID as lkp_CUSTOMER_ID,
        lkp.STATUS as lkp_STATUS,
        lkp.SALESMAN_ID as lkp_SALESMAN_ID,
        lkp.ORDER_DATE as lkp_ORDER_DATE,
        lkp.AuditRunDate as lkp_AuditRunDate
    from joined j
    left join {{ source('source_db', 'lkp_users') }} lkp
        on j.s1_ORDER_ID = lkp.ORDER_ID and j.s1_CUSTOMER_ID = lkp.CUSTOMER_ID
)

select *, current_timestamp as etl_load_dts
from enriched