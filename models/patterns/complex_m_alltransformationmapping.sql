-- UC10: m_AllTransformationMapping - All transformations combined
-- Source: employee_mapping.csv + Lookup(department.csv) -> Router -> 4 targets
-- Aggregator, mapplet, expression, filter, lookup, router, sequence, sorter
{{ config(materialized='view') }}

with src as (
    select * from {{ source('source_db', 'src_employee_mapping') }}
),
lkp as (
    select department_id, department_name from {{ source('source_db', 'lkp_departments') }}
),
enriched as (
    select
        s.*,
        l.department_id as lkp_department_id,
        l.department_name as lkp_department_name,
        s.first_name || ' ' || s.last_name as full_name,
        case
            when s.salary > 100000 then 'A2'
            when s.salary >= 75000 then 'A1'
            else 'B'
        end as salary_grade,
        s.salary * 3 as quarterly_salary,
        current_date - s.hire_date as no_of_days_in_org
    from src s
    left join lkp l on s.department = l.department_name
),
with_agg as (
    select e.*, a.agg_salary
    from enriched e
    cross join (select round(avg(quarterly_salary)::numeric, 2) as agg_salary from enriched) a
),
routed as (
    select *,
        case
            when job_title = 'SeniorManager' then 'SeniorManager'
            when job_title = 'Manager' then 'Manager'
            when job_title = 'Developer' then 'Developer'
            else 'All_Active'
        end as route_target
    from with_agg
    where employment_status = 'Active'
)
select *, current_timestamp as etl_load_dts from routed