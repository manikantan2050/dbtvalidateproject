{{ config(
      materialized='incremental',
      unique_key='employee_id',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Incremental staging pattern for M_Mass_Ingestion #}

  with source as (
      select
          employee_id,
        first_name,
        last_name,
        department,
        job_title
      from {{ source('idmc_source_p8', 'source_p8') }}
      {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
      {% endif %}
  ),

  final as (
      select
          employee_id,
        first_name,
        last_name,
        department,
        job_title,
          current_timestamp as loaded_at
      from source
  )

  select * from final
  