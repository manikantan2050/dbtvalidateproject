{{ config(
      materialized='table'
  ) }}

  {# Jinjaneering: Complex Multi-Target Router for m_AllTransformationMapping #}

  with source as (
      select
          employee_id,
          first_name,
          last_name,
          email,
          phone
      from {{ source('idmc_source_p10', 'source_p10') }}
  ),

  transformed as (
      select
          employee_id,
          first_name,
          last_name,
          email,
          phone,
          md5(concat(first_name, last_name, email)) as row_hash,
          current_timestamp as loaded_at
      from source
  )

  select * from transformed
  