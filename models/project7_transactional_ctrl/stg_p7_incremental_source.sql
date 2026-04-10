{{ config(
      materialized='incremental',
      unique_key='id',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Incremental staging pattern for M_Transactional_Control #}

  with source as (
      select
          ID,
        FIRSTNAME,
        LASTNAME,
        HOMEPHONE,
        CELLPHONE
      from {{ source('idmc_source_p7', 'source_p7') }}
      {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
      {% endif %}
  ),

  final as (
      select
          ID,
        FIRSTNAME,
        LASTNAME,
        HOMEPHONE,
        CELLPHONE,
          current_timestamp as loaded_at
      from source
  )

  select * from final
  