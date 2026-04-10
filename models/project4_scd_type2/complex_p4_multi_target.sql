{{ config(
    materialized='table'
) }}

{# Jinjaneering: Complex Multi-Target Router Pattern #}
{# Source: {{ src_db }}.{{ src_table }} #}
{# Targets: {{ target_tables | join(', ') }} #}
{# Transforms: Source → Expression → Lookup → Sequence → Mapplet → Router → Filter → Aggregator #}

with source as (
    select
        {% for f in source_fields %}
        {{ f.name }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source('idmc_source_p4', 'source_p4') }}
),

converted as (
    select
        *,
        cast(salary as decimal(20,2)) as salary_dec,
        to_date(hire_date, 'YYYY-MM-DD') as hire_date_dt
    from source
),

enriched as (
    select
        c.*,
        row_number() over (order by c.employee_id) as seq_id,
        concat(c.first_name, ' ', c.last_name) as full_name,
        case
            when c.salary_dec > 100000 then 'A2'
            when c.salary_dec >= 75000 and c.salary_dec <= 99999 then 'A1'
            else 'B'
        end as salary_grade,
        dept.department_id,
        datediff(day, c.hire_date_dt, current_date()) as days_in_org,
        round(c.salary_dec * 3, 2) as quarterly_salary
    from converted c
    left join {{ source('{{ src_db }}', 'department') }} dept
        on c.department = dept.department_name
),

{% for target in target_tables %}
{{ target.route_name | lower }} as (
    select *
    from enriched
    {% if target.filter_condition %}
    where {{ target.filter_condition }}
    {% endif %}
){% if not loop.last %},{% endif %}
{% endfor %}

{% for target in target_tables %}
-- Target: {{ target.name }}
-- select * from {{ target.route_name | lower }}
{% endfor %}
select * from enriched