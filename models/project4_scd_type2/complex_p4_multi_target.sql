{{ config(
      materialized='table'
  ) }}

  {# Jinjaneering: Complex Multi-Target Router for m_SCD_Type2 #}

  with source as (
      select
          EMP_ID,
          FIRST_NAME,
          LAST_NAME,
          EMAIL,
          GENDER
      from {{ source('idmc_source_p4', 'source_p4') }}
  ),

  transformed as (
      select
          EMP_ID,
          FIRST_NAME,
          LAST_NAME,
          EMAIL,
          GENDER,
          md5(concat(FIRST_NAME, LAST_NAME, EMAIL)) as row_hash,
          current_timestamp as loaded_at
      from source
  )

  select * from transformed
  