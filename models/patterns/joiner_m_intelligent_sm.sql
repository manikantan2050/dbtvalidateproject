-- UC6: m_Intelligent_sm - Intelligent Structure Model + Joiner
-- Source: XML structure -> StructureParser -> Joiner
{{ config(materialized='view') }}

with parsed as (
    select path, book_pk, category, cover, lang, title, year, price, author, book_fk
    from {{ source('source_db', 'src_structure_xml') }}
),
joined as (
    select
        child.path, child.book_fk,
        parent.book_pk, parent.category, parent.cover,
        parent.lang, parent.title, parent.year, parent.price, parent.author
    from parsed child
    left join parsed parent on child.book_fk = parent.book_pk
)
select *, current_timestamp as etl_load_dts from joined