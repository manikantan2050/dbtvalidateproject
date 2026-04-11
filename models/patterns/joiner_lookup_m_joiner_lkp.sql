-- Joiner + Lookup Transformation (IICS m_joiner_lkp)
-- Source (Master): src_employees -> Joiner
-- Source1 (Detail): src_users -> Joiner
-- Joiner: INNER JOIN on ORDER_ID, CUSTOMER_ID, STATUS, SALESMAN_ID, ORDER_DATE
-- Lookup: lkp_users on ORDER_ID -> brings AuditRunDate
-- Target: Tgt_Employees (ORDER_ID, CUSTOMER_ID, STATUS, SALESMAN_ID, ORDER_DATE, AuditRunDate)

{{ config(
    materialized='view'
) }}

with src_master as (
    -- Source (Master side of Joiner) = SRC_employees.csv
    select
        order_id,
        customer_id,
        status,
        salesman_id,
        order_date
    from {{ source('source_db', 'src_employees') }}
),

src_detail as (
    -- Source1 (Detail side of Joiner) = SRC_users.csv
    select
        order_id,
        customer_id,
        status,
        salesman_id,
        order_date,
        auditrunddate
    from {{ source('source_db', 'src_users') }}
),

joiner_output as (
    -- Joiner: INNER JOIN Master + Detail on all 5 common keys
    select
        m.order_id,
        m.customer_id,
        m.status,
        m.salesman_id,
        m.order_date
    from src_master m
    inner join src_detail d
        on m.order_id = d.order_id
        and m.customer_id = d.customer_id
        and m.status = d.status
        and m.salesman_id = d.salesman_id
        and m.order_date = d.order_date
),

lookup_enriched as (
    -- Lookup: LEFT JOIN lkp_users on ORDER_ID to get AuditRunDate
    select
        j.order_id,
        j.customer_id,
        j.status,
        j.salesman_id,
        j.order_date,
        lkp.auditrunddate as auditrundate
    from joiner_output j
    left join {{ source('source_db', 'lkp_users') }} lkp
        on j.order_id = lkp.order_id
)

-- Target: Tgt_Employees
select
    order_id,
    customer_id,
    status,
    salesman_id,
    order_date,
    auditrundate,
    current_timestamp as etl_load_dts
from lookup_enriched