{{ config(
      materialized='table'
  ) }}

  {# Jinjaneering: Fact Aggregation pattern for m_decode_agg #}

  with source as (
      select
          employee_id,
          first_name,
          last_name,
          email,
          phone
      from {{ source('idmc_source_p2', 'source_p2') }}
  ),

  aggregated as (
      select
          employee_id,
          first_name,
          last_name,
          email,
          phone,
          count(*) as record_count,
          current_timestamp as loaded_at
      from source
      group by employee_id, first_name, last_name, email, phone
  )

  select * from aggregated
  