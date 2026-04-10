{{ config(
      materialized='incremental',
      unique_key='path',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Incremental staging pattern for m_Intelligent_sm #}

  with source as (
      select
          Path
      from {{ source('idmc_source_p6', 'source_p6') }}
      {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
      {% endif %}
  ),

  final as (
      select
          Path,
          current_timestamp as loaded_at
      from source
  )

  select * from final
  