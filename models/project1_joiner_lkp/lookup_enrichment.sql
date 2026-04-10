{{ config(
      materialized='incremental',
      unique_key='order_id',
      incremental_strategy='merge'
  ) }}

  {# Jinjaneering: Lookup Enrichment pattern for m_joiner_lkp #}

  with source as (
      select
          ORDER_ID,
          CUSTOMER_ID,
          STATUS,
          SALESMAN_ID,
          ORDER_DATE
      from {{ source('idmc_source_p1', 'source_p1') }}
      {% if is_incremental() %}
      where ORDER_DATE > (select max(ORDER_DATE) from {{ this }})
      {% endif %}
  ),

  enriched as (
      select
          ORDER_ID,
          CUSTOMER_ID,
          STATUS,
          SALESMAN_ID,
          ORDER_DATE,
          current_timestamp as loaded_at
      from source
  )

  select * from enriched
  