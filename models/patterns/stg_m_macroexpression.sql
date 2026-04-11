-- UC9: m_MacroExpression - Macro Expression transformations
-- Source: SRC_EmployeesDistortedData.csv -> Expression(sort/clean) -> Target
{{ config(materialized='view') }}

with src as (
    select * from {{ source('source_db', 'src_employees_distorted') }}
),
cleaned as (
    select
        employee_id, first_name, last_name, email, phone,
        date_of_birth, hire_date, department, job_title,
        salary, manager_id, city, state, country,
        employment_status, years_of_experience, performance_rating,
        upper(first_name) || ' ' || upper(last_name) as full_name_upper,
        case when salary > 100000 then 'A2' when salary >= 75000 then 'A1' else 'B' end as salary_grade
    from src
    where employment_status = 'Active'
)
select *, current_timestamp as etl_load_dts from cleaned
order by last_name, first_name