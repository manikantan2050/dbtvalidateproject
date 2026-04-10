{{ config(
    materialized='table'
) }}

{# Jinjaneering: Multi-Source Joiner + Lookup Enrichment #}
{# Entity: {{ entity }} #}
{# Pattern: Source1 → Joiner ← Source2 → Lookup → Target #}

with source_1 as (
    select
        {% for f in source_1_fields %}
        {{ f }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source('{{ src_db }}', '{{ src_table }}') }}
),

source_2 as (
    select
        {% for f in source_2_fields %}
        {{ f }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source('{{ src_db_2 }}', '{{ src_table_2 }}') }}
),

joined as (
    select
        {% for f in source_1_fields %}
        s1.{{ f }} as s1_{{ f }},
        {% endfor %}
        {% for f in source_2_fields %}
        s2.{{ f }} as s2_{{ f }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from source_1 s1
    inner join source_2 s2
        on {% for k in join_keys %}s1.{{ k }} = s2.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
),

enriched as (
    select
        j.*,
        {% for f in lookup_fields %}
        lkp.{{ f }} as lkp_{{ f }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from joined j
    left join {{ source('{{ lkp_db }}', '{{ lkp_table }}') }} lkp
        on {% for k in lookup_keys %}j.s1_{{ k }} = lkp.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
)

select
    *,
    current_timestamp as etl_load_dts
from enriched