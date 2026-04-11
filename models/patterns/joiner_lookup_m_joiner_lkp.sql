-- Joiner + Lookup Enrichment
-- Pattern: joiner_lookup
-- Entity: m_joiner_lkp (PROJECT10)

{{ config(
    materialized='view'
) }}

with source_1 as (
    select
        order_id,
        customer_id,
        status,
        salesman_id,
        order_date
    from {{ source('source_db', 'src_employees') }}
),

source_2 as (
    select
        order_id,
        customer_id,
        status,
        salesman_id,
        order_date,
        auditrunddate
    from {{ source('source_db', 'src_users') }}
),

lookup_ref as (
    select
        order_id as lkp_order_id,
        customer_id as lkp_customer_id,
        status as lkp_status
    from {{ source('source_db', 'lkp_users') }}
),

joined as (
    select
        s2.order_id,
        s2.customer_id,
        s2.status,
        s2.salesman_id,
        s2.order_date,
        s2.auditrunddate,
        s1.order_id as src1_order_id,
        s1.customer_id as src1_customer_id,
        s1.status as src1_status,
        s1.salesman_id as src1_salesman_id,
        s1.order_date as src1_order_date
    from source_2 s2
    left join source_1 s1
        on s2.order_id = s1.order_id
),

enriched as (
    select
        j.*,
        lkp.lkp_order_id,
        lkp.lkp_customer_id,
        lkp.lkp_status
    from joined j
    left join lookup_ref lkp
        on j.order_id = lkp.lkp_order_id
)

select
    *,
    current_timestamp as etl_load_dts
from enriched