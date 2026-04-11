-- Joiner + Lookup Enrichment
-- Pattern: joiner_lookup
-- Entity: m_joiner_lkp

{{ config(
    materialized='table'
) }}

with source_1 as (
    select order_id, customer_id, status, salesman_id, order_date
    from {{ source('source_db', 'src_employees') }}
),

source_2 as (
    select order_id, customer_id, status, salesman_id, order_date, auditrunddate
    from {{ source('source_db', 'src_users') }}
),

joined as (
    select
        s1.order_id,
        s1.customer_id,
        s1.status,
        s1.salesman_id,
        s1.order_date,
        s2.order_id as s2_order_id,
        s2.customer_id as s2_customer_id,
        s2.status as s2_status,
        s2.salesman_id as s2_salesman_id,
        s2.order_date as s2_order_date,
        s2.auditrunddate as s2_auditrunddate
    from source_1 s1
    inner join source_2 s2
        on s1.order_id = s2.order_id and s1.customer_id = s2.customer_id and s1.status = s2.status and s1.salesman_id = s2.salesman_id and s1.order_date = s2.order_date
),

enriched as (
    select
        j.*,
        lkp.order_id as lkp_order_id
    from joined j
    left join {{ source('source_db', 'lkp_users') }} lkp
        on j.order_id = lkp.order_id
)

select *, current_timestamp as etl_load_dts
from enriched
