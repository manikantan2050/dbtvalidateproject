-- Joiner + Lookup Enrichment
-- Pattern: joiner_lookup
-- Entity: m_hierarchyparser

{{ config(
    materialized='table'
) }}

select
    field1,
    current_timestamp as etl_load_dts
from {{ source('source_db', 'src_employees') }}