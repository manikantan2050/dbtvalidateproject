{{ config(
      materialized='incremental',
      unique_key='field1',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Incremental staging pattern for m_hierarchyparser #}

  with source as (
      select
          FIELD1
      from {{ source('idmc_source_p5', 'source_p5') }}
      {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
      {% endif %}
  ),

  final as (
      select
          FIELD1,
          current_timestamp as loaded_at
      from source
  )

  select * from final
  