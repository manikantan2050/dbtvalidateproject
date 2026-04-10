{{ config(
    materialized='incremental',
    unique_key='{{ pk_field }}',
    incremental_strategy='merge'
) }}

{# Jinjaneering: Incremental staging pattern #}
{# Entity: {{ entity }} | Source: {{ src_db }}.{{ src_table }} #}

with source as (
    select * from {{ source('idmc_source_p10', 'source_p10') }}
    {% if is_incremental() %}
    where {{ incremental_column }} > (
        select max({{ incremental_column }}) from {{ this }}
    )
    {% endif %}
),

filtered as (
    select *
    from source
    {% if filter_condition %}
    where {{ filter_condition }}
    {% endif %}
),

cleansed as (
    select
        {{ pk_field }},
        {% for f in source_fields %}
        {% if f.expr %}{{ f.expr }} as {{ f.name }}{% else %}{{ f.name }}{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
        current_timestamp() as etl_load_dts
    from filtered
)

select * from cleansed