-- UC2: m_decode_agg - Decode + Conditional Aggregation
-- Source: employee_input.csv -> Expression(Updated_Salary) + Aggregator(Avg rating)
-- Output: 10 per department (100 rows total)
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
        cast(salary as decimal(10,2)) as employees_salary,
        performance_rating as rating,
        row_number() over (partition by department order by employee_id desc) as rn
    from src
),
aggregated as (
    select avg(performance_rating) as o_rating from src
)
select
    d.employee_id, d.first_name, d.last_name, d.email, d.phone,
    d.date_of_birth, d.hire_date, d.department, d.job_title,
    d.manager_id, d.city, d.state, d.country,
    d.employment_status, d.years_of_experience,
    d.employees_salary, d.updated_salary, d.rating,
    current_timestamp as etl_load_dts
from decoded d cross join aggregated a
where d.rn <= 10
