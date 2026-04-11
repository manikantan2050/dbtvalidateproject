-- UC8: M_Mass_Ingestion - Mass ingestion with Expression + Filter
-- Source: src_employee_data.csv -> Expression(Full_Name, Annual_salary) -> Filter(Active)
{{ config(materialized='view') }}

with src as (
    select * from {{ source('source_db', 'src_employee_data') }}
),
transformed as (
    select
        *,
        first_name || ' ' || last_name as full_name,
        base_salary + coalesce(bonus_amount, 0) as annual_salary
    from src
)
select *, current_timestamp as etl_load_dts
from transformed
where status = 'Active'