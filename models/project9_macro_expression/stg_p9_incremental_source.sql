{{ config(
      materialized='incremental',
      unique_key='employee_id',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Incremental staging pattern for m_MacroExpression #}

  with source as (
      select
          EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE
      from {{ source('idmc_source_p9', 'source_p9') }}
      {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
      {% endif %}
  ),

  final as (
      select
          EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
          current_timestamp as loaded_at
      from source
  )

  select * from final
  