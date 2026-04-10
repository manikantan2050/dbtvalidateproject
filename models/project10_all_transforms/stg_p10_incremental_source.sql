{{ config(
      materialized='incremental',
      unique_key='employee_id',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Incremental staging pattern for m_AllTransformationMapping #}

  with source as (
      select
          employee_id,
        first_name,
        last_name,
        email,
        phone
      from {{ source('idmc_source_p10', 'source_p10') }}
      {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
      {% endif %}
  ),

  final as (
      select
          employee_id,
        first_name,
        last_name,
        email,
        phone,
          current_timestamp as loaded_at
      from source
  )

  select * from final
  