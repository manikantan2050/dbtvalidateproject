-- UC4: m_SCD_Type2 - SCD Type 2 with lookup, expression, router
-- Source: src_emp_v3 -> Lookup(tgt_scd2_nks) -> MD5 checksum -> Router(INSERT/UPDATE/SOFT_DELETE)
{{ config(materialized='view') }}

with src as (
    select *, md5(cast(emp_id as varchar)||first_name||last_name||email||gender||cast(salary as varchar)||cast(dept_id as varchar)) as checksum
    from {{ source('source_db', 'src_emp_v3') }}
),
lkp as (
    select * from {{ source('source_db', 'tgt_scd2_nks') }}
),
flagged as (
    select
        s.emp_id, s.first_name, s.last_name, s.email, s.gender, s.salary, s.dept_id, s.checksum,
        l.emp_s_key as lkp_emp_s_key, l.checksum as lkp_checksum,
        case
            when l.emp_id is null then 'INSERT'
            when l.emp_id = s.emp_id and l.checksum <> s.checksum then 'UPDATE'
            else 'REJECT'
        end as flag,
        current_timestamp as new_dttm,
        current_date as start_date,
        date '9999-12-31' as end_date
    from src s
    left join lkp l on s.emp_id = l.emp_id
)
select *, current_timestamp as etl_load_dts from flagged
where flag in ('INSERT', 'UPDATE')