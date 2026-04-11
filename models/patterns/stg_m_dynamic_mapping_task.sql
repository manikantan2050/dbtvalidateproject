-- UC3: m_Dynamic_Mapping_Task - Dynamic mapping with expression
-- Source: src_contact.csv -> Expression(FULL_NAME)
{{ config(materialized='view') }}

select
    id, firstname, lastname, homephone, cellphone, workphone,
    firstname || ' ' || lastname as full_name,
    current_timestamp as etl_load_dts
from {{ source('source_db', 'src_contact') }}