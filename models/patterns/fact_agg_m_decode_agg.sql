-- UC2: m_decode_agg - Decode + Conditional Aggregation
-- Source: employee_input.csv -> Expression(Updated_Salary) + Aggregator(Avg rating)
{{ config(materialized='view') }}

with src as (
    select * from {{ source('source_db', 'src_employee_input') }}
),
decoded as (
    select
        *,
        case
            when salary is null then 'NULL'
            else cast(salary as varchar)
        end as updated_salary,
        cast(salary as decimal(10,2)) as o_salary,
        performance_rating as o_performancerating
    from src
),
aggregated as (
    select avg(o_performancerating) as o_rating from decoded
)
select d.*, a.o_rating, current_timestamp as etl_load_dts
from decoded d cross join aggregated a