-- UC7: M_Transactional_Control - Transaction Control transformation
-- Source: src_contact.csv -> TransactionControl -> Target
{{ config(materialized='view') }}

select
    id, firstname, lastname, homephone, cellphone, workphone,
    current_timestamp as etl_load_dts
from {{ source('source_db', 'src_contact') }}