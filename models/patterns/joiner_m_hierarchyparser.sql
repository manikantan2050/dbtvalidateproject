-- UC5: m_hierarchyparser - Hierarchy Parser + Joiner
-- Source: XML hierarchy -> HierarchyParser -> Joiner(self-join FK_BOOK->PK_BOOK)
{{ config(materialized='view') }}

with parsed as (
    select pk_book, category, cover, title, lang, year, price, author, fk_book
    from {{ source('source_db', 'src_hierarchy_xml') }}
),
joined as (
    select
        child.fk_book, child.author,
        parent.pk_book, parent.category, parent.cover,
        parent.title, parent.lang, parent.year, parent.price
    from parsed child
    left join parsed parent on child.fk_book = parent.pk_book
)
select *, current_timestamp as etl_load_dts from joined