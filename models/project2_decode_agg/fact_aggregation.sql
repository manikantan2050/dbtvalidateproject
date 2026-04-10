{{ config(
    materialized='table'
) }}

{# Jinjaneering: Fact aggregation pattern #}
{# Entity: {{ entity }} | Source: {{ src_db }}.{{ src_table }} #}

with source as (
    select * from {{ source('idmc_source_p2', 'source_p2') }}
    {% if filter_condition %}
    where {{ filter_condition }}
    {% endif %}
),

aggregated as (
    select
        {% for key in group_keys %}
        {{ key }},
        {% endfor %}
        {% for m in metric_fields %}
        {{ m.agg_func }}({{ m.field }}) as {{ m.alias }}{% if not loop.last %},{% endif %}
        {% endfor %}
        count(*) as record_count,
        current_timestamp() as etl_load_dts
    from source
    group by
        {% for key in group_keys %}
        {{ key }}{% if not loop.last %},{% endif %}
        {% endfor %}
)

select * from aggregated