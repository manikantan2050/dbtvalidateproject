-- Joiner + Lookup Enrichment
-- Pattern: joiner_lookup
-- Entity: m_Intelligent_sm

{{ config(
    materialized='view'
) }}

select
    path,
    current_timestamp as etl_load_dts
from {{ source('source_db', 'src_employees') }}